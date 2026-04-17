import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from hookline.models import DeliveryStatus

EVENT_TYPE_PATTERN = r"^[a-z0-9][a-z0-9_.\-]{0,199}$"


class EndpointIn(BaseModel):
    url: str = Field(max_length=2048)
    description: str = Field(default="", max_length=200)
    event_types: list[str] = Field(default_factory=lambda: ["*"], min_length=1, max_length=100)

    @field_validator("event_types")
    @classmethod
    def _types(cls, value: list[str]) -> list[str]:
        import re

        for t in value:
            if t != "*" and not re.match(EVENT_TYPE_PATTERN, t):
                raise ValueError(f"invalid event type {t!r}")
        return sorted(set(value))


class EndpointPatch(BaseModel):
    active: bool | None = None
    description: str | None = Field(default=None, max_length=200)


class EndpointOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    url: str
    description: str
    event_types: list[str]
    active: bool
    created_at: datetime
    circuit_open_until: datetime | None


class EndpointCreated(EndpointOut):
    #: Only returned once, when the endpoint is created.
    secret: str


class EventIn(BaseModel):
    type: str = Field(pattern=EVENT_TYPE_PATTERN)
    data: dict[str, Any]


class EventOut(BaseModel):
    id: uuid.UUID
    type: str
    created_at: datetime
    data: dict[str, Any]
    deliveries: list[uuid.UUID]


class AttemptOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    number: int
    started_at: datetime
    duration_ms: int
    status_code: int | None
    error: str | None
    response_body: str | None
    next_attempt_at: datetime | None


class DeliveryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_id: uuid.UUID
    endpoint_id: uuid.UUID
    status: DeliveryStatus
    run_attempts: int
    total_attempts: int
    next_attempt_at: datetime | None
    last_error: str | None
    created_at: datetime
    finished_at: datetime | None


class DeliveryDetail(DeliveryOut):
    event_type: str
    endpoint_url: str
    attempts: list[AttemptOut]
