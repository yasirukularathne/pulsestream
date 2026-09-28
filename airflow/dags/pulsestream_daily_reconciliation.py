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


REQUIRED_COLUMNS = {
    "patient_id",
    "test_type",
    "result_value",
    "reference_range",
    "collected_at",
}

ALLOWED_TEST_TYPES = {
    "CBC",
    "Sodium",
    "Potassium",
    "Creatinine",
}


def read_and_validate_daily_labs(file_path):
    """
    Read the daily lab CSV and separate valid rows from rejected rows.

    Returns:
        total_rows: total CSV data rows
        valid_rows: rows suitable for daily_lab_results
        rejected_rows: rows suitable for rejected_rows
    """

    if not file_path.exists():
        raise FileNotFoundError(
            f"Lab file not found: {file_path}"
        )

    total_rows = 0
    valid_rows = []
    rejected_rows = []

    seen_keys = set()

    with file_path.open(
        "r",
        newline="",
        encoding="utf-8",
    ) as csv_file:

        reader = csv.DictReader(csv_file)

        actual_columns = set(reader.fieldnames or [])

        missing_columns = REQUIRED_COLUMNS - actual_columns

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

            if not patient_id:
                reason = "missing_patient_id"

            elif test_type not in ALLOWED_TEST_TYPES:
                reason = "invalid_test_type"

            else:
                try:
                    float(result_value)
                except ValueError:
                    reason = "invalid_result"

            if reason is None and not reference_range:
                reason = "missing_reference_range"

            if reason is None and not collected_at:
                reason = "missing_collected_at"

            duplicate_key = (
                patient_id,
                test_type,
            )

            if reason is None and duplicate_key in seen_keys:
                reason = "duplicate_patient_test"

            seen_keys.add(duplicate_key)

            if reason is not None:
                rejected_rows.append(
                    {
                        "raw_content": json.dumps(row),
                        "reason": reason,
                    }
                )
            else:
                valid_rows.append(
                    {
                        "patient_id": patient_id,
                        "test_type": test_type,
                        "result_value": result_value,
                        "reference_range": reference_range,
                        "collected_at": collected_at,
                    }
                )

    return total_rows, valid_rows, rejected_rows


def get_simulation_context(context):
    ds_nodash = context["ds_nodash"]
    sim_date = context["ds"]

    file_path = LABS_DIR / f"labs_{ds_nodash}.csv"

    return sim_date, file_path


def test_batch_pipeline():
    print("PulseStream daily reconciliation pipeline started")


def validate_daily_labs(**context):
    sim_date, file_path = get_simulation_context(context)

    print(f"Validating lab file: {file_path}")
    print(f"Simulation date: {sim_date}")

    (
        total_rows,
        valid_rows,
        rejected_rows,
    ) = read_and_validate_daily_labs(file_path)

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

    print("----- Daily Lab Validation Summary -----")
    print(f"File: {file_path}")
    print(f"Simulation date: {sim_date}")
    print(f"Total rows: {total_rows}")
    print(f"Valid rows: {len(valid_rows)}")
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


def stage_daily_labs(**context):
    sim_date, file_path = get_simulation_context(context)

    print(f"Staging valid lab rows from: {file_path}")
    print(f"Simulation date: {sim_date}")

    (
        total_rows,
        valid_rows,
        rejected_rows,
    ) = read_and_validate_daily_labs(file_path)

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

        cursor.execute(
            """
            DELETE FROM daily_lab_results
            WHERE sim_date = %s
            """,
            (sim_date,),
        )

        deleted_rows = cursor.rowcount

        if deleted_rows > 0:
            print(
                f"Deleted {deleted_rows} previous staged lab rows "
                f"for simulation date {sim_date}."
            )

        staged_rows = 0

        for row in valid_rows:
            collected_at = datetime.fromisoformat(
                row["collected_at"]
            )

            if collected_at.tzinfo is not None:
                collected_at = (
                    collected_at
                    .astimezone(timezone.utc)
                    .replace(tzinfo=None)
                )

            cursor.execute(
                """
                INSERT INTO daily_lab_results (
                    patient_id,
                    test_type,
                    result_value,
                    reference_range,
                    collected_at,
                    sim_date
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    row["patient_id"],
                    row["test_type"],
                    row["result_value"],
                    row["reference_range"],
                    collected_at,
                    sim_date,
                ),
            )

            staged_rows += 1

        connection.commit()

        print(
            f"Inserted {staged_rows} valid lab rows "
            "into daily_lab_results."
        )

    except Exception as exc:
        if connection:
            connection.rollback()

        print(
            f"Failed to stage daily lab results: {exc}"
        )

        raise

    finally:
        if connection:
            connection.close()

    print("----- Daily Lab Staging Summary -----")
    print(f"File: {file_path}")
    print(f"Simulation date: {sim_date}")
    print(f"Total rows: {total_rows}")
    print(f"Valid rows staged: {len(valid_rows)}")
    print(f"Rejected rows: {len(rejected_rows)}")
    print("-------------------------------------")

    if total_rows == 0:
        raise ValueError(
            "Lab file contains no data rows."
        )

    print("Daily lab staging completed successfully.")


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

    stage_daily_labs = PythonOperator(
        task_id="stage_daily_labs",
        python_callable=stage_daily_labs,
    )

    (
        start_pipeline
        >> wait_for_daily_labs
        >> validate_daily_labs
        >> stage_daily_labs
    )
