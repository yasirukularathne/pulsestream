import random

from producers.abnormality import apply_abnormality
from producers.event_metadata import create_event_metadata

ABNORMAL_PROBABILITY = 0.04


def generate_vitals(
    patient_id,
    baseline_hr,
    baseline_spo2,
    rng=None,
):
    rng = rng or random

    heart_rate = baseline_hr + rng.randint(-5, 5)
    spo2 = baseline_spo2 + rng.uniform(-1.0, 1.0)

    systolic_bp = rng.randint(110, 130)
    diastolic_bp = rng.randint(70, 85)

    temperature = round(rng.uniform(36.3, 37.2), 2)

    vitals = {
        "patient_id": patient_id,
        "heart_rate": heart_rate,
        "spo2": round(spo2, 2),
        "systolic_bp": systolic_bp,
        "diastolic_bp": diastolic_bp,
        "temperature": temperature,
    }

    if rng.random() < ABNORMAL_PROBABILITY:
        vitals = apply_abnormality(vitals, rng)

    metadata = create_event_metadata(patient_id)

    vitals.update(metadata)

    return vitals