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