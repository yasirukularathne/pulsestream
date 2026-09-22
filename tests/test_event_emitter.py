import random

from producers.event_emitter import prepare_events


def test_prepare_events_always_returns_original():
    event = {
        "event_id": "P0001-event-001",
        "patient_id": "P0001",
        "heart_rate": 80,
    }

    events = prepare_events(event, random.Random(42))

    assert events[0] == event


def test_duplicate_keeps_same_event_id():
    event = {
        "event_id": "P0001-event-001",
        "patient_id": "P0001",
        "heart_rate": 80,
    }

    rng = random.Random(1)

    events = prepare_events(event, rng)

    if len(events) == 2:
        assert events[0]["event_id"] == events[1]["event_id"]
        assert events[0] == events[1]