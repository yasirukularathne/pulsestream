from datetime import timedelta

from producers.cadence import generate_cadence


def get_next_event_timestamp(event_ts, rng=None):
    cadence = generate_cadence(rng)

    return event_ts + timedelta(seconds=cadence)


def generate_event_timestamps(start_ts, count, rng=None):
    timestamps = [start_ts]

    current_ts = start_ts

    for _ in range(count - 1):
        current_ts = get_next_event_timestamp(current_ts, rng)
        timestamps.append(current_ts)

    return timestamps