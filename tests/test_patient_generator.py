from producers.patient_generator import generate_patients


def test_generate_patients_count():
    patients = generate_patients(count=20, seed=42)

    assert len(patients) == 20


def test_generate_patients_structure():
    patients = generate_patients(count=5, seed=42)

    for patient in patients:
        assert patient["patient_id"].startswith("P")
        assert 60 <= patient["baseline_hr"] <= 100
        assert 95 <= patient["baseline_spo2"] <= 99


def test_generate_patients_is_deterministic():
    patients1 = generate_patients(count=5, seed=42)
    patients2 = generate_patients(count=5, seed=42)

    assert patients1 == patients2