"""Hookline: webhook delivery with signatures, retries and a record of every attempt."""

from hookline.signing import SignatureError, sign, verify

__all__ = ["SignatureError", "sign", "verify"]
__version__ = "0.1.0"
