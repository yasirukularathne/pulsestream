from producers.abnormality import apply_abnormality


def test_abnormality_changes_vitals():
    vitals = {
        "patient_id": "P0001",
        "heart_rate": 80,
        "spo2": 98,
        "systolic_bp": 120,
        "diastolic_bp": 80,
        "temperature": 36.8,
    }

    updated = apply_abnormality(
        vitals,
        rng=__import__("random").Random(42),
    )

    assert updated["patient_id"] == "P0001"
    assert updated != vitals


def test_abnormality_returns_valid_structure():
    vitals = {
        "patient_id": "P0001",
        "heart_rate": 80,
        "spo2": 98,
        "systolic_bp": 120,
        "diastolic_bp": 80,
        "temperature": 36.8,
    }

    updated = apply_abnormality(
        vitals,
        rng=__import__("random").Random(42),
    )

    assert 20 <= updated["heart_rate"] <= 250
    assert 0 <= updated["spo2"] <= 100
    assert updated["systolic_bp"] > updated["diastolic_bp"]
    assert 25 <= updated["temperature"] <= 45
