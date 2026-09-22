from datetime import datetime, timezone

from producers.timestamp_utils import make_out_of_order_timestamp


def test_timestamp_is_moved_backwards():
    event_ts = datetime(2026, 9, 22, 12, 0, 0, tzinfo=timezone.utc)

    result = make_out_of_order_timestamp(
        event_ts,
        seconds_behind=3,
    )

    assert result == datetime(
        2026,
        9,
        22,
        11,
        59,
        57,
        tzinfo=timezone.utc,
    )


def test_original_timestamp_is_not_modified():
    event_ts = datetime(2026, 9, 22, 12, 0, 0, tzinfo=timezone.utc)

    make_out_of_order_timestamp(event_ts, seconds_behind=5)

    assert event_ts == datetime(
        2026,
        9,
        22,
        12,
        0,
        0,
        tzinfo=timezone.utc,
    )