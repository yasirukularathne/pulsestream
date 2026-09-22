import random

from producers.cadence import generate_cadence


def test_cadence_is_within_range():
    cadence = generate_cadence(random.Random(42))

    assert 3 <= cadence <= 8


def test_cadence_is_deterministic():
    cadence1 = generate_cadence(random.Random(42))
    cadence2 = generate_cadence(random.Random(42))

    assert cadence1 == cadence2