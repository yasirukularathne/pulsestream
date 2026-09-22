from datetime import timedelta

from producers.sensor_dropout import generate_dropout_duration


def apply_dropout(event_ts, rng=None):
    dropout_duration = generate_dropout_duration(rng)

    return event_ts + timedelta(
        seconds=dropout_duration
    )