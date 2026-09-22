from producers.duplicates import should_duplicate


def prepare_events(event, rng=None):
    events = [event]

    if should_duplicate(rng):
        events.append(event.copy())

    return events