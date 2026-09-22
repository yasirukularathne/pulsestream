import random


MIN_DROPOUT_SECONDS = 20
MAX_DROPOUT_SECONDS = 60


def generate_dropout_duration(rng=None):
    rng = rng or random

    return rng.randint(
        MIN_DROPOUT_SECONDS,
        MAX_DROPOUT_SECONDS,
    )