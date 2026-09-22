import random


DUPLICATE_PROBABILITY = 0.01


def should_duplicate(rng=None):
    rng = rng or random

    return rng.random() < DUPLICATE_PROBABILITY