from producers.vitals_generator import generate_vitals


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

    assert result1["patient_id"] == result2["patient_id"]
    assert result1["heart_rate"] == result2["heart_rate"]
    assert result1["spo2"] == result2["spo2"]
    assert result1["systolic_bp"] == result2["systolic_bp"]
    assert result1["diastolic_bp"] == result2["diastolic_bp"]
    assert result1["temperature"] == result2["temperature"]



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