from datetime import datetime, timezone
import csv
import json
from pathlib import Path

import psycopg2

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor


PROJECT_ROOT = Path("/mnt/c/Users/yasiru/Desktop/pulsestream")
LABS_DIR = PROJECT_ROOT / "data" / "incoming"

POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5433
POSTGRES_DB = "pulsestream"
POSTGRES_USER = "pulsestream"
POSTGRES_PASSWORD = "changeme"


def test_batch_pipeline():
    print("PulseStream daily reconciliation pipeline started")


def validate_daily_labs(**context):
    ds_nodash = context["ds_nodash"]
    sim_date = context["ds"]

    file_path = LABS_DIR / f"labs_{ds_nodash}.csv"

    print(f"Validating lab file: {file_path}")
    print(f"Simulation date: {sim_date}")

    required_columns = {
        "patient_id",
        "test_type",
        "result_value",
        "reference_range",
        "collected_at",
    }

    allowed_test_types = {
        "CBC",
        "Sodium",
        "Potassium",
        "Creatinine",
    }

    if not file_path.exists():
        raise FileNotFoundError(
            f"Lab file not found: {file_path}"
        )

    total_rows = 0
    valid_rows = 0
    rejected_rows = []

    seen_keys = set()

    # ---------------------------------------------------------
    # Read and validate CSV
    # ---------------------------------------------------------
    with file_path.open(
        "r",
        newline="",
        encoding="utf-8",
    ) as csv_file:

        reader = csv.DictReader(csv_file)

        actual_columns = set(reader.fieldnames or [])

        missing_columns = required_columns - actual_columns

        if missing_columns:
            raise ValueError(
                f"Missing required columns: "
                f"{sorted(missing_columns)}"
            )

        for row in reader:
            total_rows += 1

            patient_id = row["patient_id"].strip()
            test_type = row["test_type"].strip()
            result_value = row["result_value"].strip()
            reference_range = row["reference_range"].strip()
            collected_at = row["collected_at"].strip()

            reason = None

            # -------------------------------------------------
            # 1. Patient ID validation
            # -------------------------------------------------
            if not patient_id:
                reason = "missing_patient_id"

            # -------------------------------------------------
            # 2. Test type validation
            # -------------------------------------------------
            elif test_type not in allowed_test_types:
                reason = "invalid_test_type"

            # -------------------------------------------------
            # 3. Result validation
            # -------------------------------------------------
            else:
                try:
                    float(result_value)
                except ValueError:
                    reason = "invalid_result"

            # -------------------------------------------------
            # 4. Reference range validation
            # -------------------------------------------------
            if reason is None and not reference_range:
                reason = "missing_reference_range"

            # -------------------------------------------------
            # 5. Timestamp validation
            # -------------------------------------------------
            if reason is None and not collected_at:
                reason = "missing_collected_at"

            # -------------------------------------------------
            # 6. Duplicate patient + test validation
            # -------------------------------------------------
            duplicate_key = (
                patient_id,
                test_type,
            )

            if reason is None and duplicate_key in seen_keys:
                reason = "duplicate_patient_test"

            seen_keys.add(duplicate_key)

            # -------------------------------------------------
            # Store rejected row
            # -------------------------------------------------
            if reason is not None:
                rejected_rows.append(
                    {
                        "raw_content": json.dumps(row),
                        "reason": reason,
                    }
                )
            else:
                valid_rows += 1

    # ---------------------------------------------------------
    # Write rejected rows to PostgreSQL
    # ---------------------------------------------------------
    connection = None

    try:
        connection = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )

        cursor = connection.cursor()

        # -----------------------------------------------------
        # Idempotency:
        # Remove previous rejected rows for this simulation day
        # -----------------------------------------------------
        cursor.execute(
            """
            DELETE FROM rejected_rows
            WHERE source = %s
              AND sim_date = %s
            """,
            (
                "daily_labs",
                sim_date,
            ),
        )

        deleted_rows = cursor.rowcount

        if deleted_rows > 0:
            print(
                f"Deleted {deleted_rows} previous rejected rows "
                f"for simulation date {sim_date}."
            )

        # -----------------------------------------------------
        # Insert current rejected rows
        # -----------------------------------------------------
        for rejected in rejected_rows:
            cursor.execute(
                """
                INSERT INTO rejected_rows (
                    source,
                    raw_content,
                    reason,
                    rejected_at,
                    sim_date
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    "daily_labs",
                    rejected["raw_content"],
                    rejected["reason"],
                    datetime.now(timezone.utc),
                    sim_date,
                ),
            )

        connection.commit()

        print(
            f"Inserted {len(rejected_rows)} rejected rows "
            "into PostgreSQL."
        )

    except Exception as exc:
        if connection:
            connection.rollback()

        print(
            f"Failed to write rejected rows to PostgreSQL: {exc}"
        )

        raise

    finally:
        if connection:
            connection.close()

    # ---------------------------------------------------------
    # Validation summary
    # ---------------------------------------------------------
    print("----- Daily Lab Validation Summary -----")
    print(f"File: {file_path}")
    print(f"Simulation date: {sim_date}")
    print(f"Total rows: {total_rows}")
    print(f"Valid rows: {valid_rows}")
    print(f"Rejected rows: {len(rejected_rows)}")

    reason_counts = {}

    for rejected in rejected_rows:
        reason = rejected["reason"]

        reason_counts[reason] = (
            reason_counts.get(reason, 0) + 1
        )

    for reason, count in sorted(reason_counts.items()):
        print(f"{reason}: {count}")

    print("----------------------------------------")

    if total_rows == 0:
        raise ValueError(
            "Lab file contains no data rows."
        )

    print("Lab validation completed successfully.")


# =============================================================
# Airflow DAG
# =============================================================

with DAG(
    dag_id="pulsestream_daily_reconciliation",
    start_date=datetime(2026, 9, 27),
    schedule=None,
    catchup=False,
    tags=[
        "pulsestream",
        "batch",
    ],
) as dag:

    start_pipeline = PythonOperator(
        task_id="start_pipeline",
        python_callable=test_batch_pipeline,
    )

    wait_for_daily_labs = FileSensor(
        task_id="wait_for_daily_labs",
        filepath=(
            "/mnt/c/Users/yasiru/Desktop/"
            "pulsestream/data/incoming/"
            "labs_{{ ds_nodash }}.csv"
        ),
        poke_interval=5,
        timeout=30,
        mode="poke",
    )

    validate_daily_labs = PythonOperator(
        task_id="validate_daily_labs",
        python_callable=validate_daily_labs,
    )

    start_pipeline >> wait_for_daily_labs >> validate_daily_labs