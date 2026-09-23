import random

from producers.dropout_timing import apply_dropout
from producers.event_emitter import prepare_events
from producers.event_timing import get_next_event_timestamp
from producers.timestamp_utils import make_out_of_order_timestamp
from producers.vitals_generator import generate_vitals


DROPOUT_PROBABILITY = 0.03
OUT_OF_ORDER_PROBABILITY = 0.02


def generate_patient_events(
    patient_id,
    baseline_hr,
    baseline_spo2,
    start_ts,
    count,
    rng=None,
    out_of_order_probability=OUT_OF_ORDER_PROBABILITY,
):
    rng = rng or random

    events = []
    current_ts = start_ts
    previous_event_ts = None

    for index in range(count):
        event = generate_vitals(
            patient_id,
            baseline_hr,
            baseline_spo2,
            rng,
            event_ts=current_ts,
        )

        if (
            previous_event_ts is not None
            and rng.random() < out_of_order_probability
        ):
            event["timestamp"] = make_out_of_order_timestamp(
                previous_event_ts
            )

        emitted_events = prepare_events(event, rng)
        events.extend(emitted_events)

        previous_event_ts = event["timestamp"]

        if index < count - 1:
            if rng.random() < DROPOUT_PROBABILITY:
                current_ts = apply_dropout(
                    current_ts,
                    rng,
                )
            else:
                current_ts = get_next_event_timestamp(
                    current_ts,
                    rng,
                )

    return events