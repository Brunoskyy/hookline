"""What a running process holds: settings, a database, a queue, an HTTP client."""

from dataclasses import dataclass

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from hookline.config import Settings
from hookline.db import make_engine, make_sessionmaker
from hookline.delivery import Deliverer
from hookline.queues import PostgresQueue, Queue, SQSQueue


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


def build_runtime(settings: Settings) -> Runtime:
    engine = make_engine(settings.database_url)
    sessionmaker = make_sessionmaker(engine)
    client = httpx.AsyncClient(
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
    )
    return Runtime(settings, engine, sessionmaker, build_queue(settings, sessionmaker), client)
