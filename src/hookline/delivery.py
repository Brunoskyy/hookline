"""One attempt at one delivery, and everything decided from its outcome."""

import asyncio
import json
import logging
import random
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import selectinload

from hookline.config import Settings
from hookline.db import db_now
from hookline.models import Attempt, Delivery, DeliveryStatus, Endpoint, Event
from hookline.queues import Claim, Queue, take_lease
from hookline.retry import backoff, parse_retry_after
from hookline.signing import HEADER, sign
from hookline.urls import ResolutionError, UnsafeURLError, check_url, pinned

log = logging.getLogger("hookline.delivery")

USER_AGENT = "Hookline/0.1 (+https://github.com/Brunoskyy/hookline)"


def _aware(value: datetime) -> datetime:
    """SQLite hands timestamps back without a zone; they are stored in UTC."""
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def envelope(event: Event) -> bytes:
    """The body every endpoint receives for an event. Stable across attempts."""
    return json.dumps(
        {
            "id": str(event.id),
            "type": event.type,
            "created_at": event.created_at.isoformat(),
            "data": event.payload,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


@dataclass
class Outcome:
    started_at: datetime
    duration_ms: int
    status_code: int | None = None
    error: str | None = None
    response_body: str | None = None
    retry_after: str | None = None
    #: A failure that retrying cannot fix, such as a blocked address.
    permanent: bool = False

    @property
    def ok(self) -> bool:
        return self.status_code is not None and 200 <= self.status_code < 300


class Deliverer:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        queue: Queue,
        settings: Settings,
        client: httpx.AsyncClient,
    ) -> None:
        self.sessionmaker = sessionmaker
        self.queue = queue
        self.settings = settings
        self.client = client

    async def process(self, claim: Claim) -> None:
        """Runs one attempt for a claimed delivery, then acknowledges the claim.

        The acknowledgement comes only after the outcome is committed. If anything in between
        raises (the database went away while recording), the message stays in the queue and
        comes back after its visibility timeout, instead of being deleted with nothing left to
        say the delivery still needs doing.
        """
        await self.run_once(claim)
        await self.queue.ack(claim)

    async def run_once(self, claim: Claim) -> None:
        """One attempt without acknowledging the claim; for callers that ack themselves."""
        token = claim.lease_token
        if token is None:
            async with self.sessionmaker() as session:
                token = await take_lease(session, claim.delivery_id, self.settings.lease_seconds)
            if token is None:
                await self._reschedule_if_early(claim.delivery_id)
                return

        async with self.sessionmaker() as session:
            delivery = await session.scalar(
                select(Delivery)
                .where(Delivery.id == claim.delivery_id)
                .options(selectinload(Delivery.event), selectinload(Delivery.endpoint))
            )
            if delivery is None or delivery.lease_token != token:
                return
            if delivery.status is not DeliveryStatus.pending:
                return
            now = await db_now(session)
            endpoint = delivery.endpoint
            if not endpoint.active:
                await self._finish_without_attempt(session, delivery, "endpoint is disabled")
                return
            if endpoint.circuit_open_until and endpoint.circuit_open_until > now:
                # The endpoint is resting. Wait for it without spending an attempt, and spread
                # the waiting deliveries over a window after it reopens so they do not all
                # arrive in the same second; the first to land is the probe.
                spread = timedelta(seconds=random.uniform(0, self.settings.circuit_open_seconds))  # noqa: S311
                delivery.next_attempt_at = endpoint.circuit_open_until + spread
                delivery.locked_until = None
                delivery.lease_token = None
                await session.commit()
                await self.queue.schedule(
                    delivery.id, (delivery.next_attempt_at - now).total_seconds()
                )
                return
            url, secret = endpoint.url, endpoint.secret
            body = envelope(delivery.event)
            headers = {
                "Content-Type": "application/json",
                "User-Agent": USER_AGENT,
                "Hookline-Event-Id": str(delivery.event.id),
                "Hookline-Event-Type": delivery.event.type,
                "Hookline-Delivery-Id": str(delivery.id),
                "Hookline-Attempt": str(delivery.total_attempts + 1),
            }

        # No session is open while the request is in flight.
        headers[HEADER] = sign(body, secret)
        outcome = await self.send(url, body, headers)
        await self.record(claim.delivery_id, token, outcome)

    async def send(self, url: str, body: bytes, headers: dict[str, str]) -> Outcome:
        started = datetime.now(UTC)
        clock = time.perf_counter()

        def done(**kwargs: object) -> Outcome:
            ms = int((time.perf_counter() - clock) * 1000)
            return Outcome(started_at=started, duration_ms=ms, **kwargs)  # type: ignore[arg-type]

        try:
            host, addresses = await check_url(url, allow_private=self.settings.allow_private_urls)
        except ResolutionError as e:
            return done(error=str(e))
        except UnsafeURLError as e:
            return done(error=f"blocked: {e}", permanent=True)

        limit = self.settings.max_response_bytes
        deadline = self.settings.attempt_deadline_seconds
        try:
            # The httpx timeout is per read; a receiver sending a byte every few seconds would
            # never trip it. The deadline bounds the whole attempt, well inside the lease.
            async with asyncio.timeout(deadline):
                # Connect to the address that was checked, not to whatever the name resolves
                # to a moment later.
                with pinned(host, addresses):
                    async with self.client.stream(
                        "POST",
                        url,
                        content=body,
                        headers=headers,
                        timeout=self.settings.request_timeout_seconds,
                        follow_redirects=False,
                    ) as response:
                        chunks: list[bytes] = []
                        size = 0
                        async for chunk in response.aiter_bytes():
                            chunks.append(chunk)
                            size += len(chunk)
                            if size >= limit:
                                break
                        text = b"".join(chunks)[:limit].decode("utf-8", errors="replace")
                        error = None
                        if 300 <= response.status_code < 400:
                            error = (
                                "redirects are not followed; point the endpoint at the final URL"
                            )
                        return done(
                            status_code=response.status_code,
                            response_body=text or None,
                            retry_after=response.headers.get("retry-after"),
                            error=error,
                        )
        except TimeoutError:
            return done(error=f"no complete response within {deadline:g}s")
        except httpx.TimeoutException:
            return done(error=f"timed out after {self.settings.request_timeout_seconds:g}s")
        except httpx.HTTPError as e:
            return done(error=f"{type(e).__name__}: {e}" if str(e) else type(e).__name__)

    async def record(self, delivery_id: uuid.UUID, token: uuid.UUID, outcome: Outcome) -> None:
        s = self.settings
        async with self.sessionmaker() as session:
            delivery = await session.scalar(
                select(Delivery).where(Delivery.id == delivery_id).with_for_update()
            )
            if delivery is None:
                return
            now = await db_now(session)
            number = delivery.total_attempts + 1
            attempt = Attempt(
                delivery_id=delivery.id,
                number=number,
                started_at=outcome.started_at,
                duration_ms=outcome.duration_ms,
                status_code=outcome.status_code,
                error=outcome.error,
                response_body=outcome.response_body,
            )
            session.add(attempt)
            delivery.total_attempts = number

            if delivery.lease_token != token:
                # The lease ran out mid-request and someone else owns the delivery now. Keep
                # the record of what happened, leave the decision to the current holder.
                log.warning("lease lost for delivery %s", delivery.id)
                await session.commit()
                return

            delivery.run_attempts += 1
            delivery.locked_until = None
            delivery.lease_token = None
            next_delay: float | None = None

            if outcome.ok:
                delivery.status = DeliveryStatus.delivered
                delivery.finished_at = now
                delivery.last_error = None
                await session.execute(
                    update(Endpoint)
                    .where(Endpoint.id == delivery.endpoint_id)
                    .values(consecutive_failures=0, circuit_trips=0, circuit_open_until=None)
                )
            else:
                delivery.last_error = outcome.error or f"HTTP {outcome.status_code}"
                open_until = await self._count_failure(session, delivery.endpoint_id, now)
                if outcome.permanent or delivery.run_attempts >= s.max_attempts:
                    delivery.status = DeliveryStatus.dead
                    delivery.finished_at = now
                else:
                    wait = parse_retry_after(
                        outcome.retry_after, now_epoch=now.timestamp(), cap=s.backoff_cap_seconds
                    )
                    delay = (
                        timedelta(seconds=wait)
                        if wait is not None
                        else backoff(
                            delivery.run_attempts,
                            base=s.backoff_base_seconds,
                            cap=s.backoff_cap_seconds,
                        )
                    )
                    next_at = now + delay
                    if open_until is not None and open_until > next_at:
                        next_at = open_until
                    delivery.next_attempt_at = next_at
                    attempt.next_attempt_at = next_at
                    next_delay = (next_at - now).total_seconds()
            await session.commit()

        if next_delay is not None:
            await self.queue.schedule(delivery_id, next_delay)

    async def _count_failure(
        self, session: AsyncSession, endpoint_id: uuid.UUID, now: datetime
    ) -> datetime | None:
        """Counts a failure against the endpoint and opens its circuit at the threshold.

        The counter is incremented in SQL so concurrent failures to one endpoint all count,
        and the row lock that UPDATE takes serializes them. A circuit is opened only if it is
        not open already: failures that were in flight when it tripped land on an open
        circuit and add nothing, so one outage is one trip, not one per request that was out
        at the time. Opening leaves the counter one short of the threshold, so the first probe
        after it closes reopens it if the endpoint is still down; each reopening doubles the
        rest, up to the cap.
        """
        s = self.settings
        failures, trips, open_until = (
            await session.execute(
                update(Endpoint)
                .where(Endpoint.id == endpoint_id)
                .values(consecutive_failures=Endpoint.consecutive_failures + 1)
                .returning(
                    Endpoint.consecutive_failures,
                    Endpoint.circuit_trips,
                    Endpoint.circuit_open_until,
                )
            )
        ).one()
        if open_until is not None and _aware(open_until) > now:
            return _aware(open_until)
        if failures < s.circuit_failure_threshold:
            return None
        trips += 1
        rest = min(s.circuit_open_cap_seconds, s.circuit_open_seconds * 2 ** (trips - 1))
        until = now + timedelta(seconds=rest)
        await session.execute(
            update(Endpoint)
            .where(Endpoint.id == endpoint_id)
            .values(
                consecutive_failures=s.circuit_failure_threshold - 1,
                circuit_trips=trips,
                circuit_open_until=until,
            )
        )
        return until

    async def _finish_without_attempt(
        self, session: AsyncSession, delivery: Delivery, reason: str
    ) -> None:
        delivery.status = DeliveryStatus.dead
        delivery.last_error = reason
        delivery.finished_at = await db_now(session)
        delivery.locked_until = None
        delivery.lease_token = None
        await session.commit()

    async def _reschedule_if_early(self, delivery_id: uuid.UUID) -> None:
        """An SQS message arrived before its delivery is due (delays over 15 minutes are sent
        in hops). Send the next hop; anything else about the row means there is nothing to do."""
        async with self.sessionmaker() as session:
            delivery = await session.get(Delivery, delivery_id)
            if delivery is None or delivery.status is not DeliveryStatus.pending:
                return
            now = await db_now(session)
            if delivery.next_attempt_at > now and delivery.locked_until is None:
                await self.queue.schedule(
                    delivery_id, (delivery.next_attempt_at - now).total_seconds()
                )
