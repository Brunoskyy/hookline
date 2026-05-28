"""The operations the API exposes, independent of HTTP."""

import hashlib
import json
import secrets
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from hookline.db import db_now
from hookline.models import Delivery, DeliveryStatus, Endpoint, Event
from hookline.queues import Queue


class IdempotencyConflictError(Exception):
    """The idempotency key was already used for a different request."""


class NotFoundError(Exception):
    pass


def new_secret() -> str:
    return "whsec_" + secrets.token_urlsafe(32)


def request_hash(event_type: str, payload: dict[str, Any]) -> str:
    canonical = json.dumps([event_type, payload], sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def publish(
    session: AsyncSession,
    queue: Queue,
    *,
    event_type: str,
    payload: dict[str, Any],
    idempotency_key: str | None,
) -> tuple[Event, bool]:
    """Stores an event and one delivery per subscribed endpoint. Returns (event, created).

    With an idempotency key, a repeat of the same request returns the original event and
    creates nothing. Two concurrent requests with the same key race on the unique index;
    the loser rolls back and reads the winner's event.
    """
    digest = request_hash(event_type, payload)
    # An empty key would be stored but never looked up; treat it as no key at all.
    idempotency_key = idempotency_key or None
    if idempotency_key:
        existing = await _by_key(session, idempotency_key)
        if existing is not None:
            return _same_or_conflict(existing, digest), False

    event = Event(
        type=event_type, payload=payload, idempotency_key=idempotency_key, request_hash=digest
    )
    session.add(event)
    try:
        await session.flush()
    except IntegrityError:
        await session.rollback()
        if not idempotency_key:
            raise
        existing = await _by_key(session, idempotency_key)
        if existing is None:
            raise
        return _same_or_conflict(existing, digest), False

    endpoints = (await session.scalars(select(Endpoint).where(Endpoint.active))).all()
    now = await db_now(session)
    deliveries = [
        Delivery(event_id=event.id, endpoint_id=ep.id, next_attempt_at=now)
        for ep in endpoints
        if ep.wants(event_type)
    ]
    session.add_all(deliveries)
    await session.commit()
    # The rows are committed before the queue hears about them. If a send fails here (SQS
    # throttling, a network error) the delivery is not lost: the reconciler finds pending
    # rows that are due and nobody holds, and queues them again.
    for d in deliveries:
        await queue.schedule(d.id, 0)
    return event, True


async def _by_key(session: AsyncSession, key: str) -> Event | None:
    return await session.scalar(select(Event).where(Event.idempotency_key == key))


def _same_or_conflict(event: Event, digest: str) -> Event:
    if event.request_hash != digest:
        raise IdempotencyConflictError
    return event


async def replay(session: AsyncSession, queue: Queue, delivery_id: uuid.UUID) -> Delivery:
    """Starts a fresh run of attempts for a delivery that finished (delivered or dead)."""
    delivery = await session.get(Delivery, delivery_id, with_for_update=True)
    if delivery is None:
        raise NotFoundError
    if delivery.status is DeliveryStatus.pending:
        return delivery
    delivery.status = DeliveryStatus.pending
    delivery.run_attempts = 0
    delivery.next_attempt_at = await db_now(session)
    delivery.finished_at = None
    delivery.locked_until = None
    delivery.lease_token = None
    await session.commit()
    await queue.schedule(delivery.id, 0)
    return delivery
