import csv
import random
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "data" / "incoming"

PATIENT_COUNT = 20
SEED = 42

TEST_TYPES = {
    "CBC": (4.0, 12.0),
    "Sodium": (135.0, 145.0),
    "Potassium": (3.5, 5.0),
    "Creatinine": (0.6, 1.3),
}


def generate_lab_results(
    patient_count=PATIENT_COUNT,
    seed=SEED,
    collected_at=None,
):
    rng = random.Random(seed)

    if collected_at is None:
        collected_at = datetime.now(timezone.utc)

    rows = []

    for patient_number in range(1, patient_count + 1):
        patient_id = f"P{patient_number:04d}"

        for test_type, (lower, upper) in TEST_TYPES.items():
            value = round(rng.uniform(lower, upper), 2)

            rows.append(
                {
                    "patient_id": patient_id,
                    "test_type": test_type,
                    "result_value": value,
                    "reference_range": f"{lower}-{upper}",
                    "collected_at": collected_at.isoformat(),
                }
            )

    # ~5% missing reference ranges
    missing_reference_count = max(1, round(len(rows) * 0.05))

    for row in rng.sample(rows, missing_reference_count):
        row["reference_range"] = ""

    # ~2% malformed result values
    malformed_count = max(1, round(len(rows) * 0.02))

    for row in rng.sample(rows, malformed_count):
        row["result_value"] = "INVALID"

    # Occasional duplicate patient + test
    duplicate_source = rng.choice(rows)
    rows.append(duplicate_source.copy())

    # Shuffle rows so duplicates / invalid rows are not grouped together
    rng.shuffle(rows)

    return rows


def write_lab_file(rows, collected_at=None):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if collected_at is None:
        collected_at = datetime.now(timezone.utc)

    filename = f"labs_{collected_at.strftime('%Y%m%d')}.csv"
    output_path = OUTPUT_DIR / filename

    fieldnames = [
        "patient_id",
        "test_type",
        "result_value",
        "reference_range",
        "collected_at",
    ]

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return output_path


def main():
    collected_at = datetime.now(timezone.utc)

    rows = generate_lab_results(
        patient_count=PATIENT_COUNT,
        seed=SEED,
        collected_at=collected_at,
    )

    output_path = write_lab_file(
        rows,
        collected_at=collected_at,
    )

    print(f"Generated {len(rows)} lab rows")
    print(f"Output: {output_path}")


if __name__ == "__main__":
    main()