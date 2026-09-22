import random

from producers.dropout_timing import apply_dropout
from producers.event_emitter import prepare_events
from producers.event_timing import get_next_event_timestamp
from producers.vitals_generator import generate_vitals


DROPOUT_PROBABILITY = 0.03


def generate_patient_events(
    patient_id,
    baseline_hr,
    baseline_spo2,
    start_ts,
    count,
    rng=None,
):
    rng = rng or random

    events = []
    current_ts = start_ts

    for index in range(count):
        event = generate_vitals(
            patient_id,
            baseline_hr,
            baseline_spo2,
            rng,
            event_ts=current_ts,
        )

        emitted_events = prepare_events(event, rng)
        events.extend(emitted_events)

        if index < count - 1:
            if rng.random() < DROPOUT_PROBABILITY:
                current_ts = apply_dropout(current_ts, rng)
            else:
                current_ts = get_next_event_timestamp(
                    current_ts,
                    rng,
                )

    return events