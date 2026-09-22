HR_MIN = 20
HR_MAX = 250

SPO2_MIN = 0
SPO2_MAX = 100

BP_MIN = 20
BP_MAX = 300

TEMP_MIN = 25
TEMP_MAX = 45


def validate_bp(systolic_bp, diastolic_bp):
    if not (BP_MIN <= systolic_bp <= BP_MAX):
        return False

    if not (BP_MIN <= diastolic_bp <= BP_MAX):
        return False

    if systolic_bp <= diastolic_bp:
        return False

    return True


def validate_vitals_record(record):
    if not (HR_MIN <= record["heart_rate"] <= HR_MAX):
        return False, "range_error"

    if not (SPO2_MIN <= record["spo2"] <= SPO2_MAX):
        return False, "range_error"

    if not validate_bp(
        record["systolic_bp"],
        record["diastolic_bp"]
    ):
        return False, "range_error"

    if not (TEMP_MIN <= record["temperature"] <= TEMP_MAX):
        return False, "range_error"

    return True, None