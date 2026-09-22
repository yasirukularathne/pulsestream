from datetime import datetime, timezone

from producers.event_metadata import create_event_metadata


def test_event_metadata_structure():
    result = create_event_metadata("P0001")

    assert result["event_id"].startswith("P0001-")
    assert isinstance(result["timestamp"], datetime)
    assert result["timestamp"].tzinfo == timezone.utc


def test_event_metadata_accepts_custom_timestamp():
    timestamp = datetime(2026, 9, 22, 12, 0, 0, tzinfo=timezone.utc)

    result = create_event_metadata("P0001", timestamp)

    assert result["timestamp"] == timestamp
    assert result["event_id"].startswith("P0001-")


def test_event_ids_are_unique():
    result1 = create_event_metadata("P0001")
    result2 = create_event_metadata("P0001")

    assert result1["event_id"] != result2["event_id"]