"""Behaviour that only a real Postgres can show: row locks and unique-index races."""

import asyncio

import httpx
from sqlalchemy import func, select

from hookline import service
from hookline.models import Delivery, Endpoint, Event
from hookline.runtime import Runtime

from .conftest import FakeReceiver, needs_postgres

pytestmark = needs_postgres


async def seed(runtime: Runtime, events: int) -> None:
    async with runtime.sessionmaker() as session:
        session.add(Endpoint(url="http://127.0.0.1:9000/", secret="whsec_pg"))
        await session.commit()
        for i in range(events):
            await service.publish(
                session, runtime.queue, event_type="t", payload={"i": i}, idempotency_key=None
            )


async def test_concurrent_workers_never_claim_the_same_row(pg_runtime: Runtime) -> None:
    await seed(pg_runtime, 60)
    batches = await asyncio.gather(*(pg_runtime.queue.claim(10) for _ in range(8)))
    ids = [c.delivery_id for batch in batches for c in batch]
    assert len(ids) == 60
    assert len(set(ids)) == 60
    assert await pg_runtime.queue.claim(10) == []


async def test_concurrent_idempotent_publishes_make_one_event(pg_runtime: Runtime) -> None:
    async with pg_runtime.sessionmaker() as session:
        session.add(Endpoint(url="http://127.0.0.1:9000/", secret="whsec_pg"))
        await session.commit()

    async def publish() -> str:
        async with pg_runtime.sessionmaker() as session:
            event, _ = await service.publish(
                session,
                pg_runtime.queue,
                event_type="order.paid",
                payload={"order": 7},
                idempotency_key="same-key",
            )
            return str(event.id)

    ids = await asyncio.gather(*(publish() for _ in range(10)))
    assert len(set(ids)) == 1
    async with pg_runtime.sessionmaker() as session:
        assert await session.scalar(select(func.count()).select_from(Event)) == 1
        assert await session.scalar(select(func.count()).select_from(Delivery)) == 1


async def test_endpoint_failures_are_counted_under_concurrency(
    pg_runtime: Runtime, receiver: FakeReceiver
) -> None:
    pg_runtime.settings.circuit_failure_threshold = 1000
    receiver.responses = [httpx.Response(500)] * 20
    await seed(pg_runtime, 20)
    claims = await pg_runtime.queue.claim(20)
    d = pg_runtime.deliverer()
    await asyncio.gather(*(d.process(c) for c in claims))
    async with pg_runtime.sessionmaker() as session:
        ep = (await session.scalars(select(Endpoint))).one()
    assert ep.consecutive_failures == 20
