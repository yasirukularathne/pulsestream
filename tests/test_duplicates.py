from producers.duplicates import should_duplicate


def test_should_duplicate_returns_boolean():
    result = should_duplicate(
        rng=__import__("random").Random(42),
    )

    assert isinstance(result, bool)


def test_duplicate_probability():
    import random

    duplicate_count = 0
    total_events = 1000

    for seed in range(total_events):
        if should_duplicate(random.Random(seed)):
            duplicate_count += 1

    duplicate_rate = duplicate_count / total_events

    assert 0.005 <= duplicate_rate <= 0.02