"""When to try again."""

import random
from datetime import timedelta
from email.utils import parsedate_to_datetime


def backoff(
    attempt: int,
    *,
    base: float,
    cap: float,
    rng: random.Random | None = None,
) -> timedelta:
    """Delay before attempt ``attempt + 1``, given that ``attempt`` (1-based) just failed.

    Exponential with "equal jitter": half the exponential delay is fixed, the other half is
    random. Deliveries that failed together (an endpoint that was down) spread out instead of
    coming back as a herd, and the delay never drops below half the nominal value.
    """
    if attempt < 1:
        raise ValueError("attempt is 1-based")
    nominal = min(cap, base * 2 ** (attempt - 1))
    r = rng or random
    return timedelta(seconds=nominal / 2 + r.uniform(0, nominal / 2))


def parse_retry_after(value: str | None, *, now_epoch: float, cap: float) -> float | None:
    """Seconds requested by a ``Retry-After`` header, capped. None when absent or unreadable.

    The header is the receiver's to write, so anything it sends is untrusted: only ASCII
    digits count as delta-seconds (``str.isdigit`` also accepts ``'²'``, which ``float``
    rejects), and nothing here raises.
    """
    if not value:
        return None
    value = value.strip()
    try:
        if value.isascii() and value.isdigit():
            seconds = float(value)
        else:
            when = parsedate_to_datetime(value)
            seconds = when.timestamp() - now_epoch
    except (TypeError, ValueError, OverflowError, IndexError):
        return None
    if seconds != seconds:  # NaN
        return None
    return max(0.0, min(cap, seconds))
