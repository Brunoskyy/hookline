import random

import pytest

from hookline.retry import backoff, parse_retry_after


def test_schedule_grows_and_is_capped() -> None:
    rng = random.Random(7)
    nominal = [30, 60, 120, 240, 480]
    for attempt, n in enumerate(nominal, start=1):
        for _ in range(50):
            s = backoff(attempt, base=30, cap=3600, rng=rng).total_seconds()
            assert n / 2 <= s <= n
    for _ in range(50):
        assert 1800 <= backoff(20, base=30, cap=3600, rng=rng).total_seconds() <= 3600


def test_jitter_spreads_a_herd() -> None:
    rng = random.Random(1)
    delays = {round(backoff(3, base=30, cap=3600, rng=rng).total_seconds()) for _ in range(100)}
    assert len(delays) > 30


def test_attempt_is_one_based() -> None:
    with pytest.raises(ValueError, match="1-based"):
        backoff(0, base=30, cap=60)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        ("120", 120.0),
        ("99999", 3600.0),
        ("Wed, 21 Oct 2015 07:28:00 GMT", 60.0),
        ("Wed, 21 Oct 2015 07:26:00 GMT", 0.0),
        ("soon", None),
    ],
)
def test_retry_after(value: str | None, expected: float | None) -> None:
    now = 1445412420.0  # 2015-10-21T07:27:00Z
    assert parse_retry_after(value, now_epoch=now, cap=3600) == expected
