"""Where due deliveries come from.

Two implementations behind one small interface. The Postgres queue is the deliveries table
itself: a worker claims due rows with ``FOR UPDATE SKIP LOCKED`` and takes a lease on them,
so any number of workers can poll without handing out the same row twice. The SQS queue is
for running workers as Lambda functions: messages only carry a delivery id and a delay, and
the database stays the source of truth, so a duplicate or early message is harmless.
"""

import asyncio
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Protocol

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hookline.db import db_now
from hookline.models import Delivery, DeliveryStatus

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient

#: SQS refuses delays longer than 15 minutes. Longer waits are sent in hops.
SQS_MAX_DELAY_SECONDS = 900


@dataclass(frozen=True)
class Claim:
    delivery_id: uuid.UUID
    #: Set when the queue already holds a lease on the row (Postgres); None for SQS, where the
    #: worker takes the lease itself.
    lease_token: uuid.UUID | None = None
    receipt: str | None = None


class Queue(Protocol):
    async def claim(self, limit: int) -> list[Claim]: ...
    async def schedule(self, delivery_id: uuid.UUID, delay_seconds: float) -> None: ...
    async def ack(self, claim: Claim) -> None: ...


def due_filter(now_value: object) -> list[object]:
    return [
        Delivery.status == DeliveryStatus.pending,
        Delivery.next_attempt_at <= now_value,
        or_(Delivery.locked_until.is_(None), Delivery.locked_until < now_value),
    ]


async def take_lease(
    session: AsyncSession, delivery_id: uuid.UUID, lease_seconds: float
) -> uuid.UUID | None:
    """Leases one delivery if it is due and nobody holds it. Returns the token or None."""
    now = await db_now(session)
    token = uuid.uuid4()
    result = await session.execute(
        update(Delivery)
        .where(Delivery.id == delivery_id, *due_filter(now))  # type: ignore[arg-type]
        .values(locked_until=now + timedelta(seconds=lease_seconds), lease_token=token)
        .returning(Delivery.id)
    )
    leased = result.scalar_one_or_none()
    await session.commit()
    return token if leased is not None else None


class PostgresQueue:
    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession], lease_seconds: float):
        self.sessionmaker = sessionmaker
        self.lease_seconds = lease_seconds

    async def claim(self, limit: int) -> list[Claim]:
        async with self.sessionmaker() as session:
            now = await db_now(session)
            token = uuid.uuid4()
            due = (
                select(Delivery.id)
                .where(*due_filter(now))  # type: ignore[arg-type]
                .order_by(Delivery.next_attempt_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            result = await session.execute(
                update(Delivery)
                .where(Delivery.id.in_(due.scalar_subquery()))
                .values(locked_until=now + timedelta(seconds=self.lease_seconds), lease_token=token)
                .returning(Delivery.id)
                .execution_options(synchronize_session=False)
            )
            ids = list(result.scalars())
            await session.commit()
        return [Claim(delivery_id=i, lease_token=token) for i in ids]

    async def schedule(self, delivery_id: uuid.UUID, delay_seconds: float) -> None:
        # The row's next_attempt_at is the schedule; nothing to send anywhere.
        return None

    async def ack(self, claim: Claim) -> None:
        return None


async def reconcile(
    sessionmaker: async_sessionmaker[AsyncSession],
    queue: Queue,
    *,
    grace_seconds: float,
    limit: int,
) -> int:
    """Queues again the deliveries a message-based queue has lost track of.

    The database is the source of truth and the queue only says when to look. A delivery can
    be committed and then miss its message: the send failed after the commit, a worker died
    after deleting the message, or the message ended in the poison queue. Such a row stays
    ``pending``, due and unleased for good. This finds rows overdue by more than
    ``grace_seconds`` (fresh ones still have their message on the way) and sends each a new
    one. Sending one too many is harmless: the worker only acts on a row it can lease.

    The Postgres queue polls the table itself, so there is nothing to reconcile there.
    """
    if isinstance(queue, PostgresQueue):
        return 0
    async with sessionmaker() as session:
        now = await db_now(session)
        cutoff = now - timedelta(seconds=grace_seconds)
        ids = list(
            await session.scalars(
                select(Delivery.id)
                .where(*due_filter(now), Delivery.next_attempt_at <= cutoff)  # type: ignore[arg-type]
                .order_by(Delivery.next_attempt_at)
                .limit(limit)
            )
        )
    for delivery_id in ids:
        await queue.schedule(delivery_id, 0)
    return len(ids)


class SQSQueue:
    def __init__(self, client: "SQSClient", queue_url: str, *, wait_seconds: int = 10):
        self.client = client
        self.queue_url = queue_url
        self.wait_seconds = wait_seconds

    async def claim(self, limit: int) -> list[Claim]:
        response = await asyncio.to_thread(
            self.client.receive_message,
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max(1, min(10, limit)),
            WaitTimeSeconds=self.wait_seconds,
        )
        claims = []
        for message in response.get("Messages", []):
            try:
                delivery_id = uuid.UUID(message["Body"])
            except (KeyError, ValueError):
                # Not ours; drop it rather than let it bounce forever.
                await asyncio.to_thread(
                    self.client.delete_message,
                    QueueUrl=self.queue_url,
                    ReceiptHandle=message["ReceiptHandle"],
                )
                continue
            claims.append(Claim(delivery_id=delivery_id, receipt=message["ReceiptHandle"]))
        return claims

    async def schedule(self, delivery_id: uuid.UUID, delay_seconds: float) -> None:
        delay = int(max(0, min(SQS_MAX_DELAY_SECONDS, round(delay_seconds))))
        await asyncio.to_thread(
            self.client.send_message,
            QueueUrl=self.queue_url,
            MessageBody=str(delivery_id),
            DelaySeconds=delay,
        )

    async def ack(self, claim: Claim) -> None:
        if claim.receipt is None:
            return
        await asyncio.to_thread(
            self.client.delete_message, QueueUrl=self.queue_url, ReceiptHandle=claim.receipt
        )
