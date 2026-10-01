"""Tests for the failure modes a review found: lost queue messages, slow receivers, hostile
headers, circuit trips under concurrency, DNS rebinding, and the rest."""

import asyncio
import ipaddress
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from typing import Any

import boto3
import httpcore
import httpx
import pytest
from moto import mock_aws
from sqlalchemy import select
from sqlalchemy.pool import NullPool

from hookline import aws, service, urls, worker
from hookline.config import Settings
from hookline.db import make_engine
from hookline.models import Attempt, Delivery, DeliveryStatus, Endpoint
from hookline.queues import Claim, SQSQueue, reconcile
from hookline.retry import parse_retry_after
from hookline.runtime import Runtime, make_client
from hookline.signing import SignatureError, sign, verify

from .conftest import FakeReceiver, drain, needs_postgres

# --------------------------------------------------------------------------- 1. lost messages


@pytest.fixture
def sqs() -> Iterator[tuple[Any, str]]:
    with mock_aws():
        client = boto3.client("sqs", region_name="us-east-1")
        url = client.create_queue(QueueName="hookline")["QueueUrl"]
        yield client, url


class FlakySQS(SQSQueue):
    """An SQS queue whose next ``fail`` sends raise, like throttling."""

    fail = 0

    async def schedule(self, delivery_id: uuid.UUID, delay_seconds: float) -> None:
        if self.fail:
            self.fail -= 1
            raise RuntimeError("ThrottlingException")
        await super().schedule(delivery_id, delay_seconds)


async def test_a_failed_send_after_commit_is_picked_up_by_the_reconciler(
    runtime: Runtime, sqs: tuple[Any, str]
) -> None:
    client, url = sqs
    queue = FlakySQS(client, url, wait_seconds=0)
    queue.fail = 1
    runtime.queue = queue
    async with runtime.sessionmaker() as session:
        session.add(Endpoint(url="http://127.0.0.1:9000/", secret="whsec_r"))
        await session.commit()
    async with runtime.sessionmaker() as session:
        with pytest.raises(RuntimeError):
            await service.publish(
                session, queue, event_type="t", payload={"a": 1}, idempotency_key="k-1"
            )
    # The client retries with the same key: the event exists, nothing new is sent.
    async with runtime.sessionmaker() as session:
        _, created = await service.publish(
            session, queue, event_type="t", payload={"a": 1}, idempotency_key="k-1"
        )
    assert created is False
    assert await queue.claim(10) == []

    requeued = await reconcile(runtime.sessionmaker, queue, grace_seconds=0, limit=100)
    assert requeued == 1
    claims = await queue.claim(10)
    assert len(claims) == 1
    await runtime.deliverer().process(claims[0])
    async with runtime.sessionmaker() as session:
        [d] = await session.scalars(select(Delivery))
    assert d.status is DeliveryStatus.delivered
    # Nothing left to find.
    assert await reconcile(runtime.sessionmaker, queue, grace_seconds=0, limit=100) == 0


async def test_reconciler_leaves_fresh_rows_alone(runtime: Runtime, sqs: tuple[Any, str]) -> None:
    client, url = sqs
    queue = FlakySQS(client, url, wait_seconds=0)
    queue.fail = 1
    runtime.queue = queue
    async with runtime.sessionmaker() as session:
        session.add(Endpoint(url="http://127.0.0.1:9000/", secret="whsec_r"))
        await session.commit()
        with pytest.raises(RuntimeError):
            await service.publish(session, queue, event_type="t", payload={}, idempotency_key=None)
    # Within the grace period its message may still be on the way.
    assert await reconcile(runtime.sessionmaker, queue, grace_seconds=120, limit=100) == 0


async def test_lambda_reconcile_handler(
    runtime: Runtime, sqs: tuple[Any, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    client, url = sqs
    queue = FlakySQS(client, url, wait_seconds=0)
    queue.fail = 1
    runtime.queue = queue
    runtime.settings.reconcile_grace_seconds = 0
    monkeypatch.setattr(aws, "_runtime", runtime)
    async with runtime.sessionmaker() as session:
        session.add(Endpoint(url="http://127.0.0.1:9000/", secret="whsec_r"))
        await session.commit()
        with pytest.raises(RuntimeError):
            await service.publish(session, queue, event_type="t", payload={}, idempotency_key=None)
    assert await aws._reconcile() == {"requeued": 1}


class RecordingQueue:
    def __init__(self, claims: list[list[Claim]] | None = None) -> None:
        self.batches = claims or []
        self.acked: list[Claim] = []

    async def claim(self, limit: int) -> list[Claim]:
        return self.batches.pop(0)[:limit] if self.batches else []

    async def schedule(self, delivery_id: uuid.UUID, delay_seconds: float) -> None:
        return None

    async def ack(self, claim: Claim) -> None:
        self.acked.append(claim)


async def test_a_claim_is_not_acknowledged_when_recording_fails(
    runtime: Runtime, monkeypatch: pytest.MonkeyPatch
) -> None:
    queue = RecordingQueue()
    runtime.queue = queue
    d = runtime.deliverer()

    async def boom(claim: Claim) -> None:
        raise ConnectionError("database went away")

    monkeypatch.setattr(d, "run_once", boom)
    claim = Claim(delivery_id=uuid.uuid4(), receipt="r")
    with pytest.raises(ConnectionError):
        await d.process(claim)
    assert queue.acked == []


# --------------------------------------------------------------------------- 2. slow receivers


@pytest.fixture
async def dripping_server() -> AsyncIterator[str]:
    """Answers with one byte of status line every 0.3s, forever: never trips a read timeout."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read(65536)
        try:
            for byte in b"HTTP/1.1 200 OK\r\nX-Slow: " + b"a" * 10_000:
                writer.write(bytes([byte]))
                await writer.drain()
                await asyncio.sleep(0.3)
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    yield f"http://127.0.0.1:{port}/"
    server.close()


async def test_an_attempt_ends_at_its_deadline_even_if_bytes_keep_coming(
    runtime: Runtime, dripping_server: str
) -> None:
    runtime.settings.request_timeout_seconds = 1.0
    runtime.settings.attempt_deadline_seconds = 1.0
    client = make_client()
    d = runtime.deliverer()
    d.client = client
    started = time.monotonic()
    outcome = await d.send(dripping_server, b"{}", {"Content-Type": "application/json"})
    elapsed = time.monotonic() - started
    await client.aclose()
    assert outcome.status_code is None
    assert outcome.error == "no complete response within 1s"
    assert elapsed < 2.5


def test_settings_keep_the_deadline_inside_the_lease_and_the_lambda() -> None:
    with pytest.raises(ValueError, match="half of lease_seconds"):
        Settings(attempt_deadline_seconds=40, lease_seconds=60)
    with pytest.raises(ValueError, match="Lambda timeout"):
        Settings(attempt_deadline_seconds=20, lease_seconds=100, lambda_timeout_seconds=30)
    with pytest.raises(ValueError, match="request_timeout_seconds"):
        Settings(request_timeout_seconds=30, attempt_deadline_seconds=20)
    Settings()  # the defaults fit


class FakeDeliverer:
    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.done: list[str] = []

    async def process(self, claim: Claim) -> None:
        if claim.receipt == "slow":
            await self.release.wait()
        self.done.append(claim.receipt or "")


async def test_a_slow_delivery_does_not_hold_up_the_next_claims() -> None:
    def c(name: str) -> Claim:
        return Claim(delivery_id=uuid.uuid4(), receipt=name)

    queue = RecordingQueue([[c("slow"), c("a")], [c("b")], [c("c")]])
    deliverer = FakeDeliverer()
    stop = asyncio.Event()
    task = asyncio.create_task(
        worker.run(
            queue,
            deliverer,  # type: ignore[arg-type]
            batch_size=2,
            concurrency=3,
            idle_seconds=0.01,
            stop=stop,
        )
    )
    for _ in range(100):
        if {"a", "b", "c"} <= set(deliverer.done):
            break
        await asyncio.sleep(0.01)
    assert {"a", "b", "c"} <= set(deliverer.done)
    assert "slow" not in deliverer.done
    deliverer.release.set()
    stop.set()
    await asyncio.wait_for(task, 2)
    assert "slow" in deliverer.done


async def test_worker_reconciles_on_its_interval() -> None:
    calls = 0

    async def fake_reconcile() -> int:
        nonlocal calls
        calls += 1
        return 0

    stop = asyncio.Event()
    task = asyncio.create_task(
        worker.run(
            RecordingQueue(),
            FakeDeliverer(),  # type: ignore[arg-type]
            batch_size=5,
            concurrency=2,
            idle_seconds=0.01,
            stop=stop,
            reconcile=fake_reconcile,
            reconcile_interval=0.05,
        )
    )
    await asyncio.sleep(0.2)
    stop.set()
    await asyncio.wait_for(task, 2)
    assert 2 <= calls <= 6


# --------------------------------------------------------------------------- 3. Retry-After


def test_retry_after_only_trusts_ascii_digits_and_real_dates() -> None:
    now = 1_700_000_000.0
    assert parse_retry_after("\xb2", now_epoch=now, cap=3600) is None
    assert parse_retry_after("١٢", now_epoch=now, cap=3600) is None
    assert parse_retry_after(" 120 ", now_epoch=now, cap=3600) == 120
    assert parse_retry_after("9" * 400, now_epoch=now, cap=3600) == 3600
    date = datetime.fromtimestamp(now + 90, UTC).strftime("%a, %d %b %Y %H:%M:%S GMT")
    assert parse_retry_after(date, now_epoch=now, cap=3600) == pytest.approx(90)
    assert parse_retry_after("Tue, 99 Foo 2026", now_epoch=now, cap=3600) is None


async def test_a_hostile_retry_after_still_counts_the_attempt(
    runtime: Runtime, receiver: FakeReceiver
) -> None:
    receiver.responses = [httpx.Response(503, headers=[(b"Retry-After", b"\xb2")])]
    async with runtime.sessionmaker() as session:
        session.add(Endpoint(url="http://127.0.0.1:9000/", secret="whsec_h"))
        await session.commit()
        await service.publish(
            session, runtime.queue, event_type="t", payload={}, idempotency_key=None
        )
    await drain(runtime)
    async with runtime.sessionmaker() as session:
        [d] = await session.scalars(select(Delivery))
        attempts = list(await session.scalars(select(Attempt)))
    assert d.total_attempts == 1 and d.run_attempts == 1
    assert len(attempts) == 1 and attempts[0].status_code == 503
    assert d.status is DeliveryStatus.pending and d.locked_until is None


# --------------------------------------------------------------------------- 4. circuit trips


@needs_postgres
async def test_concurrent_failures_trip_the_circuit_once(
    pg_runtime: Runtime, receiver: FakeReceiver
) -> None:
    pg_runtime.settings.circuit_failure_threshold = 3
    receiver.responses = [httpx.Response(500)] * 10
    async with pg_runtime.sessionmaker() as session:
        session.add(Endpoint(url="http://127.0.0.1:9000/", secret="whsec_c"))
        await session.commit()
        for i in range(10):
            await service.publish(
                session, pg_runtime.queue, event_type="t", payload={"i": i}, idempotency_key=None
            )
    claims = await pg_runtime.queue.claim(10)
    d = pg_runtime.deliverer()
    await asyncio.gather(*(d.process(c) for c in claims))
    async with pg_runtime.sessionmaker() as session:
        ep = (await session.scalars(select(Endpoint))).one()
    assert ep.circuit_trips == 1
    assert ep.circuit_open_until is not None
    rest = (ep.circuit_open_until - datetime.now(UTC)).total_seconds()
    assert 50 < rest <= 60


# --------------------------------------------------------------------------- 5. DNS rebinding


class FakeStream(httpcore.AsyncNetworkStream):
    def __init__(self) -> None:
        self.written = b""
        self._reply = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:  # noqa: ASYNC109
        data, self._reply = self._reply, b""
        return data

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:  # noqa: ASYNC109
        self.written += buffer

    async def aclose(self) -> None:
        return None

    def get_extra_info(self, info: str) -> Any:
        return None


class RecordingBackend(httpcore.AsyncNetworkBackend):
    def __init__(self) -> None:
        self.hosts: list[str] = []
        self.stream = FakeStream()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,  # noqa: ASYNC109
        local_address: str | None = None,
        socket_options: Any = None,
    ) -> httpcore.AsyncNetworkStream:
        self.hosts.append(host)
        return self.stream

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


async def test_the_connection_goes_to_the_address_that_was_checked(
    runtime: Runtime, monkeypatch: pytest.MonkeyPatch
) -> None:
    answers = [[ipaddress.ip_address("93.184.216.34")], [ipaddress.ip_address("127.0.0.1")]]
    lookups: list[str] = []

    async def rebinding(host: str, port: int) -> list[Any]:
        lookups.append(host)
        return answers.pop(0)

    monkeypatch.setattr(urls, "resolve", rebinding)
    runtime.settings.allow_private_urls = False
    backend = RecordingBackend()
    client = make_client(backend)
    d = runtime.deliverer()
    d.client = client
    outcome = await d.send("http://rebind.test/hook", b"{}", {"Content-Type": "application/json"})
    await client.aclose()
    assert outcome.status_code == 200
    assert backend.hosts == ["93.184.216.34"]
    assert lookups == ["rebind.test"]
    # Only the address changed: the request still names the host the user registered.
    assert b"Host: rebind.test" in backend.stream.written


async def test_pinning_is_scoped_to_the_request() -> None:
    backend = RecordingBackend()
    pinned_backend = urls.PinnedBackend(backend)
    with urls.pinned("example.test", [ipaddress.ip_address("93.184.216.34")]):
        await pinned_backend.connect_tcp("EXAMPLE.test", 80)
    await pinned_backend.connect_tcp("example.test", 80)
    assert backend.hosts == ["93.184.216.34", "example.test"]


class FailingBackend(RecordingBackend):
    def __init__(self, error: Exception) -> None:
        super().__init__()
        self.error = error

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,  # noqa: ASYNC109
        local_address: str | None = None,
        socket_options: Any = None,
    ) -> httpcore.AsyncNetworkStream:
        raise self.error


@pytest.mark.parametrize(
    ("raised", "expected"),
    [
        (httpcore.ConnectTimeout("slow"), httpx.ConnectTimeout),
        (httpcore.ConnectError("refused"), httpx.ConnectError),
        (httpcore.ReadError("reset"), httpx.ReadError),
    ],
)
async def test_the_pinned_transport_raises_what_httpx_would(
    raised: Exception, expected: type[httpx.HTTPError]
) -> None:
    # Delivery tells timeouts from other failures by httpx's exception types.
    client = make_client(FailingBackend(raised))
    with pytest.raises(expected):
        await client.get("http://example.test/")
    await client.aclose()


def test_every_httpcore_error_maps_to_its_closest_httpx_error() -> None:
    # The first match wins, so no entry may be shadowed by a base class listed before it.
    table = urls._HTTPCORE_ERRORS
    for i, (source, _) in enumerate(table):
        assert not any(issubclass(source, earlier) for earlier, _ in table[:i]), source


# --------------------------------------------------------------------------- 6. pools


def test_pool_is_sized_from_settings() -> None:
    url = "postgresql+asyncpg://u:p@db.invalid/hookline"
    engine = make_engine(url, pool_size=2, max_overflow=0)
    assert engine.pool.size() == 2  # type: ignore[attr-defined]
    assert isinstance(make_engine(url, null_pool=True).pool, NullPool)


# --------------------------------------------------------------------------- 7. address forms


@pytest.mark.parametrize(
    "address",
    [
        "::169.254.169.254",  # IPv4-compatible
        "64:ff9b::a9fe:a9fe",  # NAT64 of the metadata service
        "64:ff9b::a00:1",  # NAT64 of 10.0.0.1
        "2002:7f00:1::",  # 6to4 of 127.0.0.1
        "::ffff:10.0.0.1",  # mapped
    ],
)
def test_ipv6_wrappers_of_private_ipv4_are_not_public(address: str) -> None:
    assert not urls._is_public(ipaddress.ip_address(address))


def test_nat64_of_a_public_address_is_public() -> None:
    assert urls._is_public(ipaddress.ip_address("64:ff9b::808:808"))


# --------------------------------------------------------------------------- 8. empty key


@pytest.mark.parametrize("key", ["", "   "])
async def test_an_empty_idempotency_key_is_refused(api: httpx.AsyncClient, key: str) -> None:
    r = await api.post(
        "/v1/events", json={"type": "t", "data": {}}, headers={"Idempotency-Key": key}
    )
    assert r.status_code == 400
    assert "Idempotency-Key" in r.json()["detail"]


async def test_publish_treats_an_empty_key_as_none(runtime: Runtime) -> None:
    async with runtime.sessionmaker() as session:
        a, _ = await service.publish(
            session, runtime.queue, event_type="t", payload={}, idempotency_key=""
        )
        b, _ = await service.publish(
            session, runtime.queue, event_type="t", payload={}, idempotency_key=""
        )
    assert a.id != b.id and a.idempotency_key is None


# --------------------------------------------------------------------------- 9. crafted headers


@pytest.mark.parametrize(
    "header",
    [
        f"t={int(time.time())},v1=é",
        "t=" + "9" * 400 + ",v1=abc",
        "t=\uff11\uff12\uff13,v1=abc",  # fullwidth digits
        "t=-5,v1=abc",
    ],
)
def test_crafted_headers_raise_only_signature_error(header: str) -> None:
    with pytest.raises(SignatureError):
        verify(b"{}", header, "whsec_x")


def test_a_real_signature_still_verifies() -> None:
    header = sign(b"{}", "whsec_x")
    assert verify(b"{}", header, "whsec_x") > 0


# --------------------------------------------------------------------------- 10. htmx


async def test_dashboard_serves_htmx_itself(api: httpx.AsyncClient) -> None:
    page = await api.get("/dashboard", auth=("hookline", "test-key"))
    assert page.status_code == 200
    assert "unpkg" not in page.text
    assert "/dashboard/static/htmx.min.js" in page.text
    js = await api.get("/dashboard/static/htmx.min.js")
    assert js.status_code == 200 and "htmx" in js.text[:200]
