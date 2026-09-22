import random
from datetime import datetime, timezone

from producers.dropout_timing import apply_dropout


def test_dropout_moves_timestamp_forward():
    event_ts = datetime(
        2026,
        9,
        22,
        12,
        0,
        0,
        tzinfo=timezone.utc,
    )

    result = apply_dropout(
        event_ts,
        random.Random(42),
    )

    difference = (
        result - event_ts
    ).total_seconds()

    assert 20 <= difference <= 60


def test_dropout_is_deterministic():
    event_ts = datetime(
        2026,
        9,
        22,
        12,
        0,
        0,
        tzinfo=timezone.utc,
    )

    result1 = apply_dropout(
        event_ts,
        random.Random(42),
    )

    result2 = apply_dropout(
        event_ts,
        random.Random(42),
    )

    assert result1 == result2