import asyncio
import contextlib
import logging

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
) -> None:
    """Claims due deliveries and runs them, a bounded number at a time, until ``stop`` is set.

    A batch is finished before the next is claimed, so a slow endpoint holds up at most one
    batch; its lease keeps everyone else away from it meanwhile.
    """
    gate = asyncio.Semaphore(concurrency)

    while not stop.is_set():
        claims = await queue.claim(batch_size)
        if not claims:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=idle_seconds)
            continue

        async def guarded(claim: Claim) -> None:
            async with gate:
                try:
                    await deliverer.process(claim)
                except Exception:
                    log.exception("delivery %s crashed", claim.delivery_id)

        await asyncio.gather(*(guarded(c) for c in claims))
