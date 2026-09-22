import random

from producers.sensor_dropout import generate_dropout_duration


def test_dropout_duration_is_within_range():
    duration = generate_dropout_duration(
        random.Random(42)
    )

    assert 20 <= duration <= 60


def test_dropout_duration_is_deterministic():
    duration1 = generate_dropout_duration(
        random.Random(42)
    )

    duration2 = generate_dropout_duration(
        random.Random(42)
    )

    assert duration1 == duration2