import random


def apply_abnormality(vitals, rng=None):
    rng = rng or random

    abnormal_type = rng.choice([
        "high_hr",
        "low_spo2",
        "high_temperature",
        "high_bp",
    ])

    updated = vitals.copy()

    if abnormal_type == "high_hr":
        updated["heart_rate"] = rng.randint(130, 180)

    elif abnormal_type == "low_spo2":
        updated["spo2"] = round(rng.uniform(85, 92), 2)

    elif abnormal_type == "high_temperature":
        updated["temperature"] = round(rng.uniform(38.5, 40.0), 2)

    elif abnormal_type == "high_bp":
        updated["systolic_bp"] = rng.randint(150, 190)
        updated["diastolic_bp"] = rng.randint(90, 110)

    return updated