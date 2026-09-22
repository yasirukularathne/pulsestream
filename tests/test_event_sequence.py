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

    assert len(events) == 10


def test_generate_patient_events_have_sequential_timestamps():
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

        assert 3 <= difference <= 8


def test_generate_patient_events_have_unique_event_ids():
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

    assert len(event_ids) == len(set(event_ids))