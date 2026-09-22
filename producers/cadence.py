import random


MIN_CADENCE_SECONDS = 3
MAX_CADENCE_SECONDS = 8


def generate_cadence(rng=None):
    rng = rng or random

    return rng.randint(
        MIN_CADENCE_SECONDS,
        MAX_CADENCE_SECONDS,
    )