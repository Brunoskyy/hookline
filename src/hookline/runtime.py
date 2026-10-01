"""What a running process holds: settings, a database, a queue, an HTTP client."""

from dataclasses import dataclass

import httpcore
import httpx
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from hookline.config import Settings
from hookline.db import make_engine, make_sessionmaker
from hookline.delivery import Deliverer
from hookline.queues import PostgresQueue, Queue, SQSQueue
from hookline.urls import PinnedTransport


@dataclass
class Runtime:
    settings: Settings
    engine: AsyncEngine
    sessionmaker: async_sessionmaker[AsyncSession]
    queue: Queue
    client: httpx.AsyncClient

    def deliverer(self) -> Deliverer:
        return Deliverer(self.sessionmaker, self.queue, self.settings, self.client)

    async def close(self) -> None:
        await self.client.aclose()
        await self.engine.dispose()


def build_queue(settings: Settings, sessionmaker: async_sessionmaker[AsyncSession]) -> Queue:
    if settings.queue == "sqs":
        import boto3

        if not settings.sqs_queue_url:
            raise RuntimeError("HOOKLINE_SQS_QUEUE_URL is required when HOOKLINE_QUEUE=sqs")
        client = boto3.client("sqs", region_name=settings.aws_region)
        return SQSQueue(client, settings.sqs_queue_url)
    if settings.queue != "postgres":
        raise RuntimeError(f"unknown queue {settings.queue!r}")
    return PostgresQueue(sessionmaker, settings.lease_seconds)


def make_client(network_backend: httpcore.AsyncNetworkBackend | None = None) -> httpx.AsyncClient:
    """The client deliveries go out on. Its connections are opened to the address the URL
    check approved, see :class:`hookline.urls.PinnedTransport`."""
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
    return httpx.AsyncClient(transport=PinnedTransport(limits, network_backend))


def build_runtime(settings: Settings) -> Runtime:
    engine = make_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        null_pool=settings.db_null_pool,
    )
    sessionmaker = make_sessionmaker(engine)
    client = make_client()
    return Runtime(settings, engine, sessionmaker, build_queue(settings, sessionmaker), client)
