from datetime import timedelta

from producers.cadence import generate_cadence


def get_next_event_timestamp(event_ts, rng=None):
    cadence = generate_cadence(rng)

    return event_ts + timedelta(seconds=cadence)