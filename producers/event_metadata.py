from datetime import datetime, timezone
import uuid


def create_event_metadata(patient_id, event_ts=None):
    if event_ts is None:
        event_ts = datetime.now(timezone.utc)

    event_id = f"{patient_id}-{uuid.uuid4()}"

    return {
        "event_id": event_id,
        "timestamp": event_ts,
    }