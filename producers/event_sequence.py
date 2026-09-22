from producers.event_timing import generate_event_timestamps
from producers.vitals_generator import generate_vitals


def generate_patient_events(
    patient_id,
    baseline_hr,
    baseline_spo2,
    start_ts,
    count,
    rng=None,
):
    timestamps = generate_event_timestamps(
        start_ts,
        count,
        rng,
    )

    events = []

    for event_ts in timestamps:
        event = generate_vitals(
            patient_id,
            baseline_hr,
            baseline_spo2,
            rng,
            event_ts=event_ts,
        )

        events.append(event)

    return events