from datetime import datetime, timezone

from producers.event_timing import (
    generate_event_timestamps,
    get_next_event_timestamp,
)


def test_next_event_timestamp_is_3_to_8_seconds_later():
    event_ts = datetime(
        2026,
        9,
        22,
        12,
        0,
        0,
        tzinfo=timezone.utc,
    )

    result = get_next_event_timestamp(
        event_ts,
        __import__("random").Random(42),
    )

    difference = (result - event_ts).total_seconds()

    assert 3 <= difference <= 8


def test_next_event_timestamp_is_deterministic():
    event_ts = datetime(
        2026,
        9,
        22,
        12,
        0,
        0,
        tzinfo=timezone.utc,
    )

    result1 = get_next_event_timestamp(
        event_ts,
        __import__("random").Random(42),
    )

    result2 = get_next_event_timestamp(
        event_ts,
        __import__("random").Random(42),
    )

    assert result1 == result2


def test_generate_event_timestamps_has_3_to_8_second_intervals():
    from datetime import datetime, timezone
    import random

    start_ts = datetime(
        2026,
        9,
        22,
        12,
        0,
        0,
        tzinfo=timezone.utc,
    )

    timestamps = generate_event_timestamps(
        start_ts,
        10,
        random.Random(42),
    )

    assert len(timestamps) == 10

    for previous, current in zip(timestamps, timestamps[1:]):
        difference = (current - previous).total_seconds()

        assert 3 <= difference <= 8
