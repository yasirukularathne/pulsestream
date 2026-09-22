from shared.validation import validate_vitals_record


def test_valid_vitals():
    record = {
        "heart_rate": 80,
        "spo2": 98,
        "systolic_bp": 120,
        "diastolic_bp": 80,
        "temperature": 36.8,
    }

    assert validate_vitals_record(record) == (True, None)


def test_invalid_heart_rate():
    record = {
        "heart_rate": 500,
        "spo2": 98,
        "systolic_bp": 120,
        "diastolic_bp": 80,
        "temperature": 36.8,
    }

    assert validate_vitals_record(record) == (False, "range_error")