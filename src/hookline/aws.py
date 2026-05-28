"""Entry points for AWS Lambda.

``api_handler`` serves the FastAPI app behind API Gateway. ``sqs_handler`` is the worker: SQS
invokes it with a batch of delivery ids, it runs one attempt for each, and reports failures
per message so only those come back.
"""

import asyncio
import uuid
from typing import Any

from mangum import Mangum

from hookline.api import create_app
from hookline.config import get_settings
from hookline.queues import Claim, SQSQueue, reconcile
from hookline.runtime import build_runtime

api_handler = Mangum(create_app(), lifespan="auto")

_runtime = None
# One loop for the life of the Lambda container: the database pool and the HTTP client are
# bound to it and are reused across invocations.
_loop = asyncio.new_event_loop()


def _get_runtime() -> Any:
    global _runtime
    if _runtime is None:
        _runtime = build_runtime(get_settings())
    return _runtime


async def _handle(records: list[dict[str, Any]]) -> dict[str, list[dict[str, str]]]:
    runtime = _get_runtime()
    deliverer = runtime.deliverer()
    queue = runtime.queue
    failures: list[dict[str, str]] = []

    async def one(record: dict[str, Any]) -> None:
        try:
            claim = Claim(delivery_id=uuid.UUID(record["body"]))
            # Lambda deletes a message only when it is not reported as failed, and it is
            # reported as failed whenever the outcome did not make it into the database.
            await deliverer.run_once(claim)
        except Exception:
            failures.append({"itemIdentifier": record["messageId"]})

    if not isinstance(queue, SQSQueue):
        raise RuntimeError("the SQS handler needs HOOKLINE_QUEUE=sqs")
    await asyncio.gather(*(one(r) for r in records))
    return {"batchItemFailures": failures}


def sqs_handler(event: dict[str, Any], context: object) -> dict[str, list[dict[str, str]]]:
    return _loop.run_until_complete(_handle(event.get("Records", [])))


async def _reconcile() -> dict[str, int]:
    runtime = _get_runtime()
    s = runtime.settings
    requeued = await reconcile(
        runtime.sessionmaker,
        runtime.queue,
        grace_seconds=s.reconcile_grace_seconds,
        limit=s.reconcile_batch_size,
    )
    return {"requeued": requeued}


def reconcile_handler(event: dict[str, Any], context: object) -> dict[str, int]:
    """Runs on an EventBridge schedule: re-queues deliveries whose message was lost, including
    the ones whose message ended in the poison queue."""
    return _loop.run_until_complete(_reconcile())
