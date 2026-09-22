import random


def generate_patients(count=20, seed=42):
    rng = random.Random(seed)

    patients = []

    for index in range(1, count + 1):
        patients.append({
            "patient_id": f"P{index:04d}",
            "baseline_hr": rng.randint(60, 100),
            "baseline_spo2": round(rng.uniform(95, 99), 2),
        })

    return patients