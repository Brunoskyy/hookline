import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable

from hookline.delivery import Deliverer
from hookline.queues import Claim, Queue

log = logging.getLogger("hookline.worker")


async def run(
    queue: Queue,
    deliverer: Deliverer,
    *,
    batch_size: int,
    concurrency: int,
    idle_seconds: float,
    stop: asyncio.Event,
    reconcile: Callable[[], Awaitable[int]] | None = None,
    reconcile_interval: float = 60.0,
) -> None:
    """Claims due deliveries and runs them, at most ``concurrency`` at a time, until ``stop``.

    Claiming does not wait for a whole batch to finish: whenever a slot frees up, more work
    is claimed for it. One slow endpoint therefore holds one slot for at most the attempt
    deadline, never the whole worker. ``reconcile``, when given, runs every
    ``reconcile_interval`` seconds to re-queue deliveries a message queue lost.
    """
    running: set[asyncio.Task[None]] = set()
    last_reconcile = float("-inf")

    async def guarded(claim: Claim) -> None:
        try:
            await deliverer.process(claim)
        except Exception:
            log.exception("delivery %s crashed", claim.delivery_id)

    try:
        while not stop.is_set():
            if reconcile is not None and time.monotonic() - last_reconcile >= reconcile_interval:
                last_reconcile = time.monotonic()
                try:
                    requeued = await reconcile()
                    if requeued:
                        log.info("re-queued %d deliveries", requeued)
                except Exception:
                    log.exception("reconcile failed")

            free = concurrency - len(running)
            claims = await queue.claim(min(batch_size, free)) if free > 0 else []
            for claim in claims:
                task = asyncio.create_task(guarded(claim))
                running.add(task)
                task.add_done_callback(running.discard)

            if claims and len(running) < concurrency:
                continue
            waiters: set[asyncio.Future[object]] = {asyncio.ensure_future(stop.wait())}
            if running:
                waiters |= set(running)  # type: ignore[arg-type]
            with contextlib.suppress(TimeoutError):
                await asyncio.wait(
                    waiters,
                    timeout=None if len(running) >= concurrency else idle_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            for w in waiters:
                if w not in running:
                    w.cancel()
    finally:
        if running:
            await asyncio.gather(*running, return_exceptions=True)
