import random
from datetime import datetime, timezone

from producers.event_sequence import generate_patient_events


def test_generate_patient_events_count():
    start_ts = datetime(
        2026,
        9,
        22,
        12,
        0,
        0,
        tzinfo=timezone.utc,
    )

    events = generate_patient_events(
        "P0001",
        80,
        98,
        start_ts,
        10,
        random.Random(42),
    )

    assert len(events) >= 10


def test_generate_patient_events_have_non_decreasing_timestamps():
    start_ts = datetime(
        2026,
        9,
        22,
        12,
        0,
        0,
        tzinfo=timezone.utc,
    )

    events = generate_patient_events(
        "P0001",
        80,
        98,
        start_ts,
        10,
        random.Random(42),
    )

    assert events[0]["timestamp"] == start_ts

    for previous, current in zip(events, events[1:]):
        difference = (
            current["timestamp"] - previous["timestamp"]
        ).total_seconds()

        assert 0 <= difference <= 8


def test_duplicate_events_have_same_event_id():
    start_ts = datetime(
        2026,
        9,
        22,
        12,
        0,
        0,
        tzinfo=timezone.utc,
    )

    events = generate_patient_events(
        "P0001",
        80,
        98,
        start_ts,
        100,
        random.Random(42),
    )

    event_ids = [event["event_id"] for event in events]

    assert len(event_ids) >= 100
    assert len(event_ids) > len(set(event_ids))


def test_unique_event_ids_represent_original_events():
    start_ts = datetime(
        2026,
        9,
        22,
        12,
        0,
        0,
        tzinfo=timezone.utc,
    )

    events = generate_patient_events(
        "P0001",
        80,
        98,
        start_ts,
        10,
        random.Random(42),
    )

    event_ids = [event["event_id"] for event in events]

    unique_event_ids = set(event_ids)

    assert len(unique_event_ids) == 10

def test_patient_events_can_contain_sensor_dropout():
    start_ts = datetime(
        2026,
        9,
        22,
        12,
        0,
        0,
        tzinfo=timezone.utc,
    )

    events = generate_patient_events(
        "P0001",
        80,
        98,
        start_ts,
        100,
        random.Random(42),
    )

    timestamps = [
        event["timestamp"]
        for event in events
    ]

    gaps = [
        (
            current - previous
        ).total_seconds()
        for previous, current in zip(
            timestamps,
            timestamps[1:],
        )
        if current > previous
    ]

    assert any(gap >= 20 for gap in gaps)
