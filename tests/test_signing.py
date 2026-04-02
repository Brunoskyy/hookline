import hashlib
import hmac

import pytest

from hookline.signing import SignatureError, parse, sign, verify

BODY = b'{"id":"evt_1","type":"order.paid"}'


def test_round_trip() -> None:
    header = sign(BODY, "whsec_a", timestamp=1_700_000_000)
    assert verify(BODY, header, "whsec_a", now=1_700_000_010) == 1_700_000_000


def test_signature_is_hmac_of_timestamp_dot_body() -> None:
    # Pinned so a refactor cannot silently change what receivers have to compute.
    expected = hmac.new(b"secret", b"1.hello", hashlib.sha256).hexdigest()
    assert sign(b"hello", "secret", timestamp=1) == f"t=1,v1={expected}"


@pytest.mark.parametrize(
    ("body", "secret", "now", "message"),
    [
        (BODY + b" ", "whsec_a", 1_700_000_000, "no signature matches"),
        (BODY, "whsec_b", 1_700_000_000, "no signature matches"),
        (BODY, "whsec_a", 1_700_000_301, "tolerance"),
        (BODY, "whsec_a", 1_699_999_699, "tolerance"),
    ],
)
def test_rejects(body: bytes, secret: str, now: float, message: str) -> None:
    header = sign(BODY, "whsec_a", timestamp=1_700_000_000)
    with pytest.raises(SignatureError, match=message):
        verify(body, header, secret, now=now)


def test_any_of_several_signatures_during_rotation() -> None:
    header = sign(BODY, ["whsec_old", "whsec_new"], timestamp=100)
    assert header.count("v1=") == 2
    assert verify(BODY, header, "whsec_old", now=100) == 100
    assert verify(BODY, header, "whsec_new", now=100) == 100


@pytest.mark.parametrize("header", [None, "", "v1=abc", "t=abc,v1=x", "t=1", "nonsense"])
def test_malformed(header: str | None) -> None:
    with pytest.raises(SignatureError):
        verify(BODY, header, "whsec_a", now=1)


def test_parse_ignores_unknown_schemes() -> None:
    assert parse("t=5,v0=old,v1=abc") == (5, ["abc"])


def test_sign_needs_a_secret() -> None:
    with pytest.raises(ValueError, match="secret"):
        sign(BODY, [])
