from functools import lru_cache

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Everything configurable, read from HOOKLINE_* environment variables."""

    model_config = SettingsConfigDict(env_prefix="HOOKLINE_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://hookline:hookline@localhost:5432/hookline"
    #: Bearer token for the /v1 API and the password for the dashboard. Empty disables auth,
    #: which only makes sense on a laptop.
    api_key: str = ""

    #: "postgres" keeps the queue in the deliveries table; "sqs" uses an SQS queue.
    queue: str = "postgres"
    sqs_queue_url: str = ""
    aws_region: str = "us-east-1"

    max_attempts: int = 8
    backoff_base_seconds: float = 30.0
    backoff_cap_seconds: float = 6 * 3600.0
    #: Per read/connect timeout handed to httpx. Not a limit on the whole request: a receiver
    #: can drip bytes and never trip it, which is what the attempt deadline is for.
    request_timeout_seconds: float = 10.0
    #: Wall-clock limit on one attempt, from connecting to reading the last byte kept.
    attempt_deadline_seconds: float = 20.0
    lease_seconds: float = 60.0
    #: The Lambda worker's timeout. An attempt has to end well inside it, or a slow receiver
    #: would fail the whole batch.
    lambda_timeout_seconds: float = 60.0

    circuit_failure_threshold: int = 5
    circuit_open_seconds: float = 60.0
    circuit_open_cap_seconds: float = 3600.0

    max_payload_bytes: int = 256 * 1024
    max_response_bytes: int = 2048

    #: Subscriber URLs that resolve to loopback, private or link-local addresses are refused
    #: unless this is set. Turn it on for local demos, never in production.
    allow_private_urls: bool = False

    worker_batch_size: int = 20
    worker_concurrency: int = 10
    worker_idle_seconds: float = 1.0

    #: With an SQS queue, how often pending deliveries that are due but not leased are sent
    #: to the queue again, and how long a row has to be overdue before it counts as lost.
    reconcile_interval_seconds: float = 60.0
    reconcile_grace_seconds: float = 120.0
    reconcile_batch_size: int = 500

    #: Database connections per process. Lambda runs many small processes, each with its own
    #: pool, so the default is small; see infra/README for the budget against max_connections.
    db_pool_size: int = 5
    db_max_overflow: int = 5
    #: No pool at all: open a connection per session. For use behind RDS Proxy.
    db_null_pool: bool = False

    @model_validator(mode="after")
    def _deadlines_fit(self) -> "Settings":
        if self.request_timeout_seconds > self.attempt_deadline_seconds:
            raise ValueError("request_timeout_seconds must not exceed attempt_deadline_seconds")
        # Leave room for the database work around the request inside both windows.
        if self.attempt_deadline_seconds > self.lease_seconds / 2:
            raise ValueError("attempt_deadline_seconds must be at most half of lease_seconds")
        if self.attempt_deadline_seconds > self.lambda_timeout_seconds / 2:
            raise ValueError("attempt_deadline_seconds must be at most half the Lambda timeout")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
