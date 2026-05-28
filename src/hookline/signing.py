"""Webhook signatures.

The header looks like ``t=1727712000,v1=5257a8...`` where ``v1`` is the hex HMAC-SHA256 of
``f"{t}.{body}"`` under the endpoint's secret. Signing the timestamp together with the body
lets a receiver refuse replays of an old request; several ``v1`` values may appear while a
secret is being rotated, and any one of them matching is enough.
"""

import hashlib
import hmac
import time
from collections.abc import Iterable

HEADER = "Hookline-Signature"
DEFAULT_TOLERANCE_SECONDS = 300


class SignatureError(ValueError):
    """The signature header is missing, malformed, stale, or does not match."""


def _digest(secret: str, timestamp: int, body: bytes) -> str:
    message = str(timestamp).encode() + b"." + body
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def sign(body: bytes, secrets: str | Iterable[str], timestamp: int | None = None) -> str:
    """Builds the header value for ``body``. Pass several secrets during a rotation."""
    ts = int(time.time()) if timestamp is None else timestamp
    keys = [secrets] if isinstance(secrets, str) else list(secrets)
    if not keys:
        raise ValueError("at least one secret is required")
    return ",".join([f"t={ts}", *(f"v1={_digest(k, ts, body)}" for k in keys)])


def parse(header: str) -> tuple[int, list[str]]:
    timestamp: int | None = None
    signatures: list[str] = []
    for part in header.split(","):
        key, sep, value = part.strip().partition("=")
        if not sep:
            continue
        if key == "t":
            # Seconds since the epoch fit in 11 digits for millennia; anything longer is an
            # attempt to overflow the arithmetic below, not a timestamp.
            if not (value.isascii() and value.isdigit() and len(value) <= 12):
                raise SignatureError("timestamp is not an integer")
            timestamp = int(value)
        elif key == "v1":
            signatures.append(value)
    if timestamp is None:
        raise SignatureError("no timestamp in signature header")
    if not signatures:
        raise SignatureError("no v1 signature in signature header")
    return timestamp, signatures


def verify(
    body: bytes,
    header: str | None,
    secret: str,
    *,
    tolerance: int = DEFAULT_TOLERANCE_SECONDS,
    now: float | None = None,
) -> int:
    """Checks a delivery on the receiving side and returns its timestamp.

    Raises :class:`SignatureError` when the header is missing or malformed, when the timestamp
    is further than ``tolerance`` seconds from now in either direction, or when no signature
    matches. Comparison is constant-time.
    """
    if not header:
        raise SignatureError(f"missing {HEADER} header")
    timestamp, signatures = parse(header)
    current = time.time() if now is None else now
    if abs(current - timestamp) > tolerance:
        raise SignatureError("timestamp outside the tolerance window")
    expected = _digest(secret, timestamp, body).encode()
    # Compared as bytes: compare_digest refuses non-ASCII str, and a crafted header must end
    # in SignatureError like any other bad one, not in a TypeError.
    if not any(hmac.compare_digest(expected, c.encode("utf-8", "replace")) for c in signatures):
        raise SignatureError("no signature matches")
    return timestamp
