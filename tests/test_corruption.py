from producers.corruption import apply_invalid_value


def test_apply_invalid_value_creates_invalid_vitals():
    vitals = {
        "patient_id": "P0001",
        "heart_rate": 80,
        "spo2": 98,
        "systolic_bp": 120,
        "diastolic_bp": 80,
        "temperature": 36.8,
    }

    updated = apply_invalid_value(
        vitals,
        rng=__import__("random").Random(42),
    )

    assert updated["patient_id"] == "P0001"

    invalid = (
        updated["heart_rate"] < 20
        or updated["heart_rate"] > 250
        or updated["spo2"] < 0
        or updated["spo2"] > 100
        or updated["systolic_bp"] < 20
        or updated["systolic_bp"] > 300
        or updated["temperature"] < 25
        or updated["temperature"] > 45
    )

    assert invalid