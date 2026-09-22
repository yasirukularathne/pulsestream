import random


def apply_invalid_value(vitals, rng=None):
    rng = rng or random

    updated = vitals.copy()

    invalid_type = rng.choice([
        "invalid_heart_rate",
        "invalid_spo2",
        "invalid_systolic_bp",
        "invalid_temperature",
    ])

    if invalid_type == "invalid_heart_rate":
        updated["heart_rate"] = 500

    elif invalid_type == "invalid_spo2":
        updated["spo2"] = -10

    elif invalid_type == "invalid_systolic_bp":
        updated["systolic_bp"] = 500

    elif invalid_type == "invalid_temperature":
        updated["temperature"] = 60.0

    return updated