import json
from datetime import UTC, datetime, timedelta

import httpx
from sqlalchemy import select, update

from hookline import service
from hookline.delivery import Outcome
from hookline.models import Attempt, Delivery, DeliveryStatus, Endpoint
from hookline.runtime import Runtime
from hookline.signing import verify

from .conftest import FakeReceiver, drain, make_due


async def setup_one(
    runtime: Runtime, url: str = "http://127.0.0.1:9000/hook", *, endpoints: int = 1
) -> list[Delivery]:
    async with runtime.sessionmaker() as session:
        for i in range(endpoints):
            session.add(Endpoint(url=url, secret=f"whsec_{i}", event_types=["*"]))
        await session.commit()
        await service.publish(
            session, runtime.queue, event_type="order.paid", payload={"n": 1}, idempotency_key=None
        )
        return list(await session.scalars(select(Delivery)))


async def reload(runtime: Runtime, delivery: Delivery) -> Delivery:
    async with runtime.sessionmaker() as session:
        d = await session.get(Delivery, delivery.id)
        assert d is not None
        return d


async def attempts(runtime: Runtime) -> list[Attempt]:
    async with runtime.sessionmaker() as session:
        return list(await session.scalars(select(Attempt).order_by(Attempt.number)))


async def test_signed_request_the_receiver_can_verify(
    runtime: Runtime, receiver: FakeReceiver
) -> None:
    [d] = await setup_one(runtime)
    await drain(runtime)
    [request] = receiver.requests
    verify(request.content, request.headers["hookline-signature"], "whsec_0")
    body = json.loads(request.content)
    assert body["type"] == "order.paid" and body["data"] == {"n": 1}
    assert request.headers["hookline-delivery-id"] == str(d.id)
    assert request.headers["hookline-attempt"] == "1"
    assert (await reload(runtime, d)).status is DeliveryStatus.delivered


async def test_retries_with_backoff_then_delivers(runtime: Runtime, receiver: FakeReceiver) -> None:
    receiver.responses = [httpx.Response(500), httpx.ReadTimeout("slow"), httpx.Response(204)]
    [d] = await setup_one(runtime)
    before = datetime.now(UTC)

    await drain(runtime)
    d1 = await reload(runtime, d)
    assert d1.status is DeliveryStatus.pending
    assert d1.last_error == "HTTP 500"
    assert timedelta(seconds=15) <= d1.next_attempt_at - before <= timedelta(seconds=31)
    assert await drain(runtime) == 0  # not due yet

    await make_due(runtime)
    await drain(runtime)
    d2 = await reload(runtime, d)
    assert d2.last_error is not None and d2.last_error.startswith("timed out")
    assert d2.next_attempt_at - datetime.now(UTC) >= timedelta(seconds=29)

    await make_due(runtime)
    await drain(runtime)
    d3 = await reload(runtime, d)
    assert d3.status is DeliveryStatus.delivered and d3.run_attempts == 3
    log = await attempts(runtime)
    assert [(a.number, a.status_code) for a in log] == [(1, 500), (2, None), (3, 204)]
    assert log[0].next_attempt_at is not None and log[2].next_attempt_at is None


async def test_dead_letters_after_max_attempts(runtime: Runtime, receiver: FakeReceiver) -> None:
    runtime.settings.max_attempts = 3
    runtime.settings.circuit_failure_threshold = 100
    receiver.responses = [httpx.Response(500)] * 5
    [d] = await setup_one(runtime)
    for _ in range(3):
        await make_due(runtime)
        await drain(runtime)
    final = await reload(runtime, d)
    assert final.status is DeliveryStatus.dead
    assert final.finished_at is not None
    await make_due(runtime)
    assert await drain(runtime) == 0
    assert len(receiver.requests) == 3


async def test_honours_retry_after(runtime: Runtime, receiver: FakeReceiver) -> None:
    receiver.responses = [httpx.Response(429, headers={"Retry-After": "600"})]
    [d] = await setup_one(runtime)
    await drain(runtime)
    wait = (await reload(runtime, d)).next_attempt_at - datetime.now(UTC)
    assert timedelta(seconds=590) < wait <= timedelta(seconds=600)


async def test_redirects_are_failures(runtime: Runtime, receiver: FakeReceiver) -> None:
    receiver.responses = [httpx.Response(301, headers={"Location": "http://169.254.169.254/"})]
    [d] = await setup_one(runtime)
    await drain(runtime)
    assert len(receiver.requests) == 1
    assert "redirects are not followed" in ((await reload(runtime, d)).last_error or "")


async def test_blocked_address_is_dead_immediately(
    runtime: Runtime, receiver: FakeReceiver
) -> None:
    [d] = await setup_one(runtime, "http://10.0.0.8/hook")
    runtime.settings.allow_private_urls = False
    await drain(runtime)
    final = await reload(runtime, d)
    assert final.status is DeliveryStatus.dead
    assert (final.last_error or "").startswith("blocked:")
    assert receiver.requests == []


async def test_response_body_is_truncated(runtime: Runtime, receiver: FakeReceiver) -> None:
    receiver.responses = [httpx.Response(500, content=b"x" * 10_000)]
    await setup_one(runtime)
    await drain(runtime)
    [a] = await attempts(runtime)
    assert a.response_body is not None
    assert len(a.response_body) == runtime.settings.max_response_bytes


async def test_circuit_opens_and_postpones_without_spending_attempts(
    runtime: Runtime, receiver: FakeReceiver
) -> None:
    # Four deliveries to one endpoint; the threshold is 3 consecutive failures.
    receiver.responses = [httpx.Response(503)] * 3
    async with runtime.sessionmaker() as session:
        session.add(Endpoint(url="http://127.0.0.1:9000/", secret="whsec_x"))
        await session.commit()
        for i in range(4):
            await service.publish(
                session, runtime.queue, event_type="t", payload={"i": i}, idempotency_key=None
            )
    await drain(runtime)
    async with runtime.sessionmaker() as session:
        ep = (await session.scalars(select(Endpoint))).one()
        assert ep.circuit_open_until is not None and ep.circuit_trips == 1
        fourth = (await session.scalars(select(Delivery).where(Delivery.run_attempts == 0))).one()
    # The fourth came up after the circuit opened: postponed to when it closes, not sent.
    assert len(receiver.requests) == 3
    assert fourth.next_attempt_at == ep.circuit_open_until

    # Due again but the circuit is still open: postponed, no request, no attempt spent.
    await make_due(runtime)
    sent = len(receiver.requests)
    await drain(runtime)
    assert len(receiver.requests) == sent
    async with runtime.sessionmaker() as session:
        spent = [d.run_attempts for d in await session.scalars(select(Delivery))]
    assert max(spent) == 1

    # The circuit closes, a probe fails, and it reopens for twice as long.
    async with runtime.sessionmaker() as session:
        await session.execute(
            update(Endpoint).values(circuit_open_until=datetime.now(UTC) - timedelta(seconds=1))
        )
        await session.commit()
    receiver.responses = [httpx.Response(503)]
    await make_due(runtime)
    claims = await runtime.queue.claim(1)
    await runtime.deliverer().process(claims[0])
    async with runtime.sessionmaker() as session:
        ep = (await session.scalars(select(Endpoint))).one()
    assert ep.circuit_trips == 2
    assert ep.circuit_open_until is not None
    assert ep.circuit_open_until - datetime.now(UTC) > timedelta(seconds=100)

    # Once it answers, everything resets.
    async with runtime.sessionmaker() as session:
        await session.execute(update(Endpoint).values(circuit_open_until=None))
        await session.commit()
    await make_due(runtime)
    await drain(runtime)
    async with runtime.sessionmaker() as session:
        ep = (await session.scalars(select(Endpoint))).one()
        statuses = {d.status for d in await session.scalars(select(Delivery))}
    assert ep.consecutive_failures == 0 and ep.circuit_trips == 0
    assert statuses == {DeliveryStatus.delivered}


async def test_expired_lease_is_reclaimed_and_late_result_does_not_decide(
    runtime: Runtime, receiver: FakeReceiver
) -> None:
    [d] = await setup_one(runtime)
    [first] = await runtime.queue.claim(10)
    assert await runtime.queue.claim(10) == []  # leased
    # The first worker "dies": its lease runs out.
    async with runtime.sessionmaker() as session:
        await session.execute(
            update(Delivery).values(locked_until=datetime.now(UTC) - timedelta(seconds=1))
        )
        await session.commit()
    [second] = await runtime.queue.claim(10)
    assert second.lease_token != first.lease_token
    # The first worker was only slow: its request comes back failed, after it lost the lease.
    assert first.lease_token is not None
    late = Outcome(started_at=datetime.now(UTC), duration_ms=12_000, status_code=500)
    await runtime.deliverer().record(d.id, first.lease_token, late)
    still = await reload(runtime, d)
    assert still.status is DeliveryStatus.pending
    assert still.total_attempts == 1  # what happened is on record...
    assert still.run_attempts == 0  # ...but the stale holder does not decide anything
    assert still.lease_token == second.lease_token
    # A worker whose lease is gone does not even send.
    await runtime.deliverer().process(first)
    assert receiver.requests == []
    await runtime.deliverer().process(second)
    assert (await reload(runtime, d)).status is DeliveryStatus.delivered
    assert len(receiver.requests) == 1


async def test_disabled_endpoint_dead_letters_pending(
    runtime: Runtime, receiver: FakeReceiver
) -> None:
    [d] = await setup_one(runtime)
    async with runtime.sessionmaker() as session:
        await session.execute(update(Endpoint).values(active=False))
        await session.commit()
    await drain(runtime)
    final = await reload(runtime, d)
    assert final.status is DeliveryStatus.dead and final.last_error == "endpoint is disabled"
    assert receiver.requests == []


async def test_dns_failure_is_retried_not_dead_lettered(
    runtime: Runtime, receiver: FakeReceiver
) -> None:
    [d] = await setup_one(runtime, "https://does-not-exist.invalid/hook")
    runtime.settings.allow_private_urls = False
    await drain(runtime)
    after = await reload(runtime, d)
    assert after.status is DeliveryStatus.pending
    assert (after.last_error or "").startswith("cannot resolve")
    assert receiver.requests == []
