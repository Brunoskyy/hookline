import os
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from sqlalchemy import update

from hookline.api import create_app
from hookline.config import Settings
from hookline.db import create_all, make_engine, make_sessionmaker
from hookline.delivery import Deliverer
from hookline.models import Base, Delivery, DeliveryStatus
from hookline.queues import PostgresQueue
from hookline.runtime import Runtime

Handler = Callable[[httpx.Request], httpx.Response]


@dataclass
class FakeReceiver:
    """Stands in for every subscriber: records requests, answers from a script."""

    responses: list[httpx.Response | Exception] = field(default_factory=list)
    requests: list[httpx.Request] = field(default_factory=list)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.responses:
            item = self.responses.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        return httpx.Response(200, json={"ok": True})


def settings_for(url: str, **overrides: object) -> Settings:
    base: dict[str, object] = {
        "database_url": url,
        "api_key": "test-key",
        "allow_private_urls": True,
        "backoff_base_seconds": 30,
        "circuit_failure_threshold": 3,
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


async def build(settings: Settings, receiver: FakeReceiver) -> Runtime:
    engine = make_engine(settings.database_url)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await create_all(engine)
    sessionmaker = make_sessionmaker(engine)
    client = httpx.AsyncClient(transport=httpx.MockTransport(receiver))
    return Runtime(
        settings, engine, sessionmaker, PostgresQueue(sessionmaker, settings.lease_seconds), client
    )


@pytest.fixture
def receiver() -> FakeReceiver:
    return FakeReceiver()


@pytest.fixture
def settings() -> Settings:
    return settings_for("sqlite+aiosqlite:///:memory:")


@pytest.fixture
async def runtime(settings: Settings, receiver: FakeReceiver) -> AsyncIterator[Runtime]:
    rt = await build(settings, receiver)
    yield rt
    await rt.close()


PG_URL = os.environ.get("HOOKLINE_TEST_DATABASE_URL")
needs_postgres = pytest.mark.skipif(not PG_URL, reason="set HOOKLINE_TEST_DATABASE_URL")


@pytest.fixture
async def pg_runtime(receiver: FakeReceiver) -> AsyncIterator[Runtime]:
    assert PG_URL
    rt = await build(settings_for(PG_URL), receiver)
    yield rt
    await rt.close()


@pytest.fixture
async def api(runtime: Runtime) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(runtime=runtime)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://hookline.test",
        headers={"Authorization": "Bearer test-key"},
    ) as client:
        yield client


def deliverer(runtime: Runtime) -> Deliverer:
    return runtime.deliverer()


async def make_due(runtime: Runtime) -> None:
    """Moves every pending delivery's next attempt into the past, like time passing."""
    async with runtime.sessionmaker() as session:
        await session.execute(
            update(Delivery)
            .where(Delivery.status == DeliveryStatus.pending)
            .values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1))
        )
        await session.commit()


async def drain(runtime: Runtime) -> int:
    """Runs every due delivery once. Returns how many were attempted."""
    claims = await runtime.queue.claim(100)
    d = runtime.deliverer()
    for c in claims:
        await d.process(c)
    return len(claims)
