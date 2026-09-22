from producers.vitals_generator import generate_vitals


def test_generate_vitals_structure():
    rng = __import__("random").Random(42)

    result = generate_vitals(
        patient_id="P0001",
        baseline_hr=80,
        baseline_spo2=98,
        rng=rng,
    )

    assert result["patient_id"] == "P0001"
    assert 20 <= result["heart_rate"] <= 250
    assert 0 <= result["spo2"] <= 100
    assert result["systolic_bp"] > result["diastolic_bp"]
    assert 25 <= result["temperature"] <= 45


def test_generate_vitals_is_deterministic():
    result1 = generate_vitals(
        "P0001",
        80,
        98,
        __import__("random").Random(42),
    )

    result2 = generate_vitals(
        "P0001",
        80,
        98,
        __import__("random").Random(42),
    )

    assert result1 == result2


def test_abnormal_event_probability():
    import random

    abnormal_count = 0
    total_events = 1000

    for _ in range(total_events):
        result = generate_vitals(
            "P0001",
            80,
            98,
            random.Random(_),
        )

        if (
            result["heart_rate"] >= 130
            or result["spo2"] < 92
            or result["temperature"] >= 38.5
            or result["systolic_bp"] >= 150
        ):
            abnormal_count += 1

    abnormal_rate = abnormal_count / total_events

    assert 0.02 <= abnormal_rate <= 0.06