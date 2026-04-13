import enum
import uuid
from datetime import UTC, datetime
from typing import Any, ClassVar

from sqlalchemy import (
    JSON,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    TypeDecorator,
    Uuid,
)
from sqlalchemy.engine import Dialect
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class UTCDateTime(TypeDecorator[datetime]):
    """Timezone-aware UTC in and out, whatever the backend does with zones."""

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("naive datetime")
        return value.astimezone(UTC)

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def utcnow() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    type_annotation_map: ClassVar[dict[Any, Any]] = {
        datetime: UTCDateTime(),
        dict[str, Any]: JSON,
        list[str]: JSON,
    }


class DeliveryStatus(enum.StrEnum):
    pending = "pending"
    delivered = "delivered"
    dead = "dead"


class Endpoint(Base):
    __tablename__ = "endpoints"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    url: Mapped[str] = mapped_column(String(2048))
    secret: Mapped[str] = mapped_column(String(128))
    description: Mapped[str] = mapped_column(String(200), default="")
    #: Event types this endpoint receives. ``["*"]`` means all of them.
    event_types: Mapped[list[str]] = mapped_column(default=lambda: ["*"])
    active: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    # Circuit breaker state, shared by every delivery to this endpoint.
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0)
    circuit_trips: Mapped[int] = mapped_column(Integer, default=0)
    circuit_open_until: Mapped[datetime | None] = mapped_column(default=None)

    def wants(self, event_type: str) -> bool:
        return "*" in self.event_types or event_type in self.event_types


class Event(Base):
    __tablename__ = "events"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    type: Mapped[str] = mapped_column(String(200), index=True)
    payload: Mapped[dict[str, Any]]
    #: Client-supplied key; the same key twice returns the first event instead of a new one.
    idempotency_key: Mapped[str | None] = mapped_column(String(200), unique=True, default=None)
    #: SHA-256 of the request, so a reused key with a different body is an error, not a no-op.
    request_hash: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(default=utcnow)

    deliveries: Mapped[list["Delivery"]] = relationship(back_populates="event")


class Delivery(Base):
    """One event going to one endpoint. The row is also the queue entry."""

    __tablename__ = "deliveries"
    __table_args__ = (Index("deliveries_due", "status", "next_attempt_at"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    event_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("events.id"), index=True)
    endpoint_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("endpoints.id"), index=True)
    status: Mapped[DeliveryStatus] = mapped_column(
        Enum(DeliveryStatus, native_enum=False, length=16), default=DeliveryStatus.pending
    )
    #: Attempts in the current run; a replay starts a new run at zero.
    run_attempts: Mapped[int] = mapped_column(Integer, default=0)
    total_attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(default=utcnow)
    #: A worker holds a delivery until this time. If it dies, the lease runs out and another
    #: worker picks the delivery up: at-least-once, never stuck.
    locked_until: Mapped[datetime | None] = mapped_column(default=None)
    lease_token: Mapped[uuid.UUID | None] = mapped_column(Uuid, default=None)
    last_error: Mapped[str | None] = mapped_column(Text, default=None)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(default=None)

    event: Mapped[Event] = relationship(back_populates="deliveries")
    endpoint: Mapped[Endpoint] = relationship()
    attempts: Mapped[list["Attempt"]] = relationship(
        back_populates="delivery", order_by="Attempt.number"
    )


class Attempt(Base):
    __tablename__ = "attempts"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    delivery_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("deliveries.id"), index=True)
    number: Mapped[int] = mapped_column(Integer)
    started_at: Mapped[datetime]
    duration_ms: Mapped[int] = mapped_column(Integer)
    status_code: Mapped[int | None] = mapped_column(Integer, default=None)
    error: Mapped[str | None] = mapped_column(Text, default=None)
    response_body: Mapped[str | None] = mapped_column(Text, default=None)
    #: When the next attempt was scheduled for, if there is one. Makes the timeline readable.
    next_attempt_at: Mapped[datetime | None] = mapped_column(default=None)

    delivery: Mapped[Delivery] = relationship(back_populates="attempts")

    @property
    def ok(self) -> bool:
        return self.status_code is not None and 200 <= self.status_code < 300
