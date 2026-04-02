from functools import lru_cache

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
    request_timeout_seconds: float = 10.0
    lease_seconds: float = 60.0

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


@lru_cache
def get_settings() -> Settings:
    return Settings()
