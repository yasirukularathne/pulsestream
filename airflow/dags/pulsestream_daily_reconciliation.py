from datetime import datetime, timezone
import csv
import json
from pathlib import Path

import psycopg2

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor


# =========================================================
# PROJECT CONFIGURATION
# =========================================================

PROJECT_ROOT = Path(
    "/mnt/c/Users/yasiru/Desktop/pulsestream"
)

LABS_DIR = PROJECT_ROOT / "data" / "incoming"

POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5433
POSTGRES_DB = "pulsestream"
POSTGRES_USER = "pulsestream"
POSTGRES_PASSWORD = "changeme"


# =========================================================
# LAB VALIDATION CONFIGURATION
# =========================================================

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


# =========================================================
# DATABASE CONNECTION
# =========================================================

def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


# =========================================================
# PIPELINE RUN TRACKING
# =========================================================

def create_pipeline_run(**context):
    """
    Create a pipeline_runs record when the DAG starts.
    """

    dag_run = context["dag_run"]

    dag_id = dag_run.dag_id
    run_id = dag_run.run_id

    started_at = datetime.now(timezone.utc).replace(
        tzinfo=None
    )

    connection = None

    try:
        connection = get_postgres_connection()

        cursor = connection.cursor()

        cursor.execute(
            """
            INSERT INTO pipeline_runs (
                dag_id,
                run_id,
                status,
                started_at,
                ended_at,
                records_processed,
                records_rejected
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
            """,
            (
                dag_id,
                run_id,
                "running",
                started_at,
                None,
                0,
                0,
            ),
        )

        connection.commit()

        print(
            "----- Pipeline Run Started -----"
        )

        print(
            f"DAG ID: {dag_id}"
        )

        print(
            f"Run ID: {run_id}"
        )

        print(
            f"Started at: {started_at}"
        )

        print(
            "Status: running"
        )

        print(
            "---------------------------------"
        )

    except Exception as exc:

        if connection:
            connection.rollback()

        print(
            f"Failed to create pipeline run: {exc}"
        )

        raise

    finally:

        if connection:
            connection.close()


def update_pipeline_run(context, status):
    """
    Update pipeline_runs after the DAG finishes.

    status:
        success
        failed
    """

    dag_run = context["dag_run"]

    dag_id = dag_run.dag_id
    run_id = dag_run.run_id

    ended_at = datetime.now(timezone.utc).replace(
        tzinfo=None
    )

    connection = None

    try:

        connection = get_postgres_connection()

        cursor = connection.cursor()

        # ---------------------------------------------------------
        # Count successfully staged laboratory records
        # ---------------------------------------------------------

        sim_date = context["ds"]

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM daily_lab_results
            WHERE sim_date = %s
            """,
            (sim_date,),
        )

        records_processed = cursor.fetchone()[0]

        # ---------------------------------------------------------
        # Count rejected laboratory records
        # ---------------------------------------------------------

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM rejected_rows
            WHERE source = %s
              AND sim_date = %s
            """,
            (
                "daily_labs",
                sim_date,
            ),
        )

        records_rejected = cursor.fetchone()[0]

        # ---------------------------------------------------------
        # Update pipeline run
        # ---------------------------------------------------------

        cursor.execute(
            """
            UPDATE pipeline_runs
            SET
                status = %s,
                ended_at = %s,
                records_processed = %s,
                records_rejected = %s
            WHERE dag_id = %s
              AND run_id = %s
            """,
            (
                status,
                ended_at,
                records_processed,
                records_rejected,
                dag_id,
                run_id,
            ),
        )

        connection.commit()

        print(
            "----- Pipeline Run Completed -----"
        )

        print(
            f"DAG ID: {dag_id}"
        )

        print(
            f"Run ID: {run_id}"
        )

        print(
            f"Status: {status}"
        )

        print(
            f"Ended at: {ended_at}"
        )

        print(
            f"Records processed: "
            f"{records_processed}"
        )

        print(
            f"Records rejected: "
            f"{records_rejected}"
        )

        print(
            "-----------------------------------"
        )

    except Exception as exc:

        if connection:
            connection.rollback()

        print(
            f"Failed to update pipeline run: {exc}"
        )

        # Do not hide the original DAG result.
        # The pipeline tracking failure is logged.
        print(
            "Pipeline execution status remains "
            f"{status}."
        )

    finally:

        if connection:
            connection.close()


def pipeline_success_callback(context):
    """
    DAG-level success callback.
    """

    update_pipeline_run(
        context,
        "success",
    )


def pipeline_failure_callback(context):
    """
    DAG-level failure callback.
    """

    update_pipeline_run(
        context,
        "failed",
    )


# =========================================================
# SHARED LAB READER / VALIDATOR
# =========================================================

def read_and_validate_daily_labs(file_path):
    """
    Read and validate a daily laboratory CSV file.

    Returns:
        total_rows
        valid_rows
        rejected_rows
    """

    total_rows = 0
    valid_rows = []
    rejected_rows = []

    seen_patient_tests = set()

    with file_path.open(
        "r",
        newline="",
        encoding="utf-8",
    ) as csv_file:

        reader = csv.DictReader(csv_file)

        if reader.fieldnames is None:
            raise ValueError(
                f"Missing CSV header in {file_path}"
            )

        missing_columns = (
            REQUIRED_COLUMNS
            - set(reader.fieldnames)
        )

        if missing_columns:
            raise ValueError(
                "Missing required columns: "
                f"{sorted(missing_columns)}"
            )

        for row_number, row in enumerate(
            reader,
            start=2,
        ):

            total_rows += 1

            patient_id = (
                row.get("patient_id") or ""
            ).strip()

            test_type = (
                row.get("test_type") or ""
            ).strip()

            result_value = (
                row.get("result_value") or ""
            ).strip()

            reference_range = (
                row.get("reference_range") or ""
            ).strip()

            collected_at = (
                row.get("collected_at") or ""
            ).strip()

            # -------------------------------------------------
            # Patient ID validation
            # -------------------------------------------------

            if not patient_id:

                rejected_rows.append(
                    {
                        "row_number": row_number,
                        "raw_content": json.dumps(row),
                        "reason": "missing_patient_id",
                    }
                )

                continue

            # -------------------------------------------------
            # Test type validation
            # -------------------------------------------------

            if test_type not in ALLOWED_TEST_TYPES:

                rejected_rows.append(
                    {
                        "row_number": row_number,
                        "raw_content": json.dumps(row),
                        "reason": "invalid_test_type",
                    }
                )

                continue

            # -------------------------------------------------
            # Result validation
            # -------------------------------------------------

            try:

                float(result_value)

            except (ValueError, TypeError):

                rejected_rows.append(
                    {
                        "row_number": row_number,
                        "raw_content": json.dumps(row),
                        "reason": "invalid_result",
                    }
                )

                continue

            # -------------------------------------------------
            # Reference range validation
            # -------------------------------------------------

            if not reference_range:

                rejected_rows.append(
                    {
                        "row_number": row_number,
                        "raw_content": json.dumps(row),
                        "reason": "missing_reference_range",
                    }
                )

                continue

            try:

                lower, upper = (
                    reference_range.split("-", 1)
                )

                float(lower)
                float(upper)

            except (
                ValueError,
                AttributeError,
            ):

                rejected_rows.append(
                    {
                        "row_number": row_number,
                        "raw_content": json.dumps(row),
                        "reason": "invalid_reference_range",
                    }
                )

                continue

            # -------------------------------------------------
            # Timestamp validation
            # -------------------------------------------------

            try:

                datetime.fromisoformat(
                    collected_at
                )

            except ValueError:

                rejected_rows.append(
                    {
                        "row_number": row_number,
                        "raw_content": json.dumps(row),
                        "reason": "invalid_collected_at",
                    }
                )

                continue

            # -------------------------------------------------
            # Duplicate patient + test validation
            # -------------------------------------------------

            duplicate_key = (
                patient_id,
                test_type,
            )

            if duplicate_key in seen_patient_tests:

                rejected_rows.append(
                    {
                        "row_number": row_number,
                        "raw_content": json.dumps(row),
                        "reason": "duplicate_patient_test",
                    }
                )

                continue

            seen_patient_tests.add(
                duplicate_key
            )

            valid_rows.append(row)

    return (
        total_rows,
        valid_rows,
        rejected_rows,
    )


# =========================================================
# SIMULATION CONTEXT
# =========================================================

def get_simulation_context(context):
    """
    Resolve the simulation date and expected lab file
    from the Airflow logical date.
    """

    ds_nodash = context["ds_nodash"]
    sim_date = context["ds"]

    file_path = (
        LABS_DIR
        / f"labs_{ds_nodash}.csv"
    )

    return sim_date, file_path


# =========================================================
# VALIDATE DAILY LABS
# =========================================================

def validate_daily_labs(**context):
    """
    Validate the daily laboratory file and persist
    rejected rows into PostgreSQL.

    Rejected-row persistence is idempotent for
    each simulation date.
    """

    sim_date, file_path = (
        get_simulation_context(context)
    )

    print(
        f"Validating daily labs for: "
        f"{sim_date}"
    )

    if not file_path.exists():

        raise FileNotFoundError(
            f"Lab file not found: {file_path}"
        )

    (
        total_rows,
        valid_rows,
        rejected_rows,
    ) = read_and_validate_daily_labs(
        file_path
    )

    connection = None

    try:

        connection = get_postgres_connection()

        cursor = connection.cursor()

        # -----------------------------------------------------
        # Delete previous rejected rows for this date
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

        # -----------------------------------------------------
        # Insert rejected rows
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
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
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
            "----- Daily Lab Validation -----"
        )

        print(
            f"Simulation date: {sim_date}"
        )

        print(
            f"Total rows: {total_rows}"
        )

        print(
            f"Valid rows: {len(valid_rows)}"
        )

        print(
            f"Rejected rows: "
            f"{len(rejected_rows)}"
        )

        print(
            "---------------------------------"
        )

        if total_rows == 0:

            raise ValueError(
                f"No laboratory rows found "
                f"for {sim_date}."
            )

    except Exception as exc:

        if connection:
            connection.rollback()

        print(
            f"Daily lab validation failed: "
            f"{exc}"
        )

        raise

    finally:

        if connection:
            connection.close()


# =========================================================
# STAGE DAILY LABS
# =========================================================

def stage_daily_labs(**context):
    """
    Stage validated daily laboratory rows into
    daily_lab_results.

    Existing rows for the simulation date are
    deleted first, making the operation idempotent.
    """

    sim_date, file_path = (
        get_simulation_context(context)
    )

    print(
        f"Staging daily labs for: "
        f"{sim_date}"
    )

    if not file_path.exists():

        raise FileNotFoundError(
            f"Lab file not found: {file_path}"
        )

    (
        total_rows,
        valid_rows,
        rejected_rows,
    ) = read_and_validate_daily_labs(
        file_path
    )

    if not valid_rows:

        raise ValueError(
            f"No valid laboratory rows "
            f"available for {sim_date}."
        )

    connection = None

    try:

        connection = get_postgres_connection()

        cursor = connection.cursor()

        # -----------------------------------------------------
        # Delete existing staged rows
        # -----------------------------------------------------

        cursor.execute(
            """
            DELETE FROM daily_lab_results
            WHERE sim_date = %s
            """,
            (sim_date,),
        )

        # -----------------------------------------------------
        # Insert validated laboratory rows
        # -----------------------------------------------------

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
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
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

        connection.commit()

        print(
            "----- Daily Lab Staging -----"
        )

        print(
            f"Simulation date: {sim_date}"
        )

        print(
            f"Input rows: {total_rows}"
        )

        print(
            f"Valid rows staged: "
            f"{len(valid_rows)}"
        )

        print(
            f"Rejected rows: "
            f"{len(rejected_rows)}"
        )

        print(
            "Daily lab staging completed."
        )

        print(
            "------------------------------"
        )

    except Exception as exc:

        if connection:
            connection.rollback()

        print(
            f"Daily lab staging failed: "
            f"{exc}"
        )

        raise

    finally:

        if connection:
            connection.close()


# =========================================================
# CALCULATE AUTHORITATIVE DAILY RISK
# =========================================================

def calculate_daily_risk(**context):
    """
    Calculate the authoritative operational risk flag
    by reconciling the latest realtime trend evidence
    with the daily laboratory results.
    """

    sim_date, _ = get_simulation_context(context)

    print(
        f"Calculating authoritative daily risk for: "
        f"{sim_date}"
    )

    connection = None

    try:

        connection = get_postgres_connection()

        cursor = connection.cursor()

        # ---------------------------------------------------------
        # Get patients represented in today's valid lab data
        # ---------------------------------------------------------

        cursor.execute(
            """
            SELECT DISTINCT patient_id
            FROM daily_lab_results
            WHERE sim_date = %s
            ORDER BY patient_id
            """,
            (sim_date,),
        )

        patients = [
            row[0]
            for row in cursor.fetchall()
        ]

        if not patients:

            raise ValueError(
                f"No staged lab patients found "
                f"for {sim_date}."
            )

        reports = []

        # ---------------------------------------------------------
        # Calculate daily evidence for each patient
        # ---------------------------------------------------------

        for patient_id in patients:

            # -----------------------------------------------------
            # Latest realtime trend
            # -----------------------------------------------------

            cursor.execute(
                """
                SELECT
                    window_end,
                    avg_hr,
                    avg_spo2,
                    avg_temp,
                    abnormal_count,
                    risk_flag_realtime
                FROM vitals_trends
                WHERE patient_id = %s
                  AND window_end <= %s
                ORDER BY window_end DESC
                LIMIT 1
                """,
                (
                    patient_id,
                    f"{sim_date} 23:59:59",
                ),
            )

            trend = cursor.fetchone()

            if trend is None:

                trend_summary = (
                    "No realtime trend evidence available "
                    "on or before simulation date."
                )

                trend_flag = "normal"
                trend_available = False

            else:

                (
                    window_end,
                    avg_hr,
                    avg_spo2,
                    avg_temp,
                    abnormal_count,
                    trend_flag,
                ) = trend

                trend_summary = (
                    f"Latest realtime trend: {trend_flag}; "
                    f"abnormal events: {abnormal_count}; "
                    f"window ended: {window_end}; "
                    f"avg HR: {avg_hr}; "
                    f"avg SpO2: {avg_spo2}; "
                    f"avg temperature: {avg_temp}."
                )

                trend_available = True

            # -----------------------------------------------------
            # Evaluate laboratory results
            # -----------------------------------------------------

            cursor.execute(
                """
                SELECT
                    test_type,
                    result_value,
                    reference_range
                FROM daily_lab_results
                WHERE patient_id = %s
                  AND sim_date = %s
                ORDER BY test_type
                """,
                (
                    patient_id,
                    sim_date,
                ),
            )

            lab_rows = cursor.fetchall()

            abnormal_labs = 0

            for (
                test_type,
                result_value,
                reference_range,
            ) in lab_rows:

                try:

                    lower, upper = (
                        reference_range.split("-", 1)
                    )

                    lower = float(lower)
                    upper = float(upper)
                    value = float(result_value)

                    if (
                        value < lower
                        or value > upper
                    ):

                        abnormal_labs += 1

                except (
                    ValueError,
                    AttributeError,
                ):

                    continue

            lab_count = len(lab_rows)

            if abnormal_labs == 0:

                lab_flag = "normal"

            elif abnormal_labs == 1:

                lab_flag = "watch"

            else:

                lab_flag = "elevated"

            lab_summary = (
                f"{abnormal_labs} of {lab_count} valid lab "
                "results outside supplied reference ranges."
            )

            # -----------------------------------------------------
            # Combine trend and lab evidence
            # -----------------------------------------------------

            if (
                trend_flag == "elevated"
                or lab_flag == "elevated"
            ):

                operational_risk_flag = "elevated"

            elif (
                trend_flag == "watch"
                and lab_flag == "watch"
            ):

                operational_risk_flag = "elevated"

            elif (
                trend_flag == "watch"
                or lab_flag == "watch"
            ):

                operational_risk_flag = "watch"

            else:

                operational_risk_flag = "normal"

            # -----------------------------------------------------
            # Data quality note
            # -----------------------------------------------------

            quality_notes = []

            if not trend_available:

                quality_notes.append(
                    "realtime trend unavailable"
                )

            if abnormal_labs > 0:

                quality_notes.append(
                    f"{abnormal_labs} lab result(s) outside "
                    "supplied reference ranges"
                )

            if not quality_notes:

                data_quality_note = (
                    "Valid staged lab data available; "
                    "no lab results outside supplied "
                    "reference ranges."
                )

            else:

                data_quality_note = "; ".join(
                    quality_notes
                )

            reports.append(
                {
                    "patient_id": patient_id,
                    "sim_date": sim_date,
                    "trend_summary": trend_summary,
                    "lab_summary": lab_summary,
                    "operational_risk_flag":
                        operational_risk_flag,
                    "data_quality_note":
                        data_quality_note,
                }
            )

        # ---------------------------------------------------------
        # Print calculation results
        # ---------------------------------------------------------

        print(
            "----- Daily Risk Calculation -----"
        )

        print(
            f"Simulation date: {sim_date}"
        )

        print(
            f"Patients evaluated: "
            f"{len(reports)}"
        )

        for report in reports:

            print(
                f"{report['patient_id']} | "
                f"{report['operational_risk_flag']} | "
                f"{report['lab_summary']} | "
                f"{report['data_quality_note']}"
            )

        print(
            "----------------------------------"
        )

        return reports

    except Exception as exc:

        print(
            f"Failed to calculate daily risk: "
            f"{exc}"
        )

        raise

    finally:

        if connection:
            connection.close()


# =========================================================
# WRITE DAILY RISK REPORT
# =========================================================

def write_daily_risk_report(**context):
    """
    Persist the authoritative daily risk report
    into PostgreSQL.

    Idempotent for each simulation date.
    """

    sim_date, _ = get_simulation_context(context)

    reports = context["ti"].xcom_pull(
        task_ids="calculate_daily_risk"
    )

    if not reports:

        raise ValueError(
            f"No daily risk reports found "
            f"for {sim_date}."
        )

    connection = None

    try:

        connection = get_postgres_connection()

        cursor = connection.cursor()

        # ---------------------------------------------------------
        # Delete existing reports for simulation date
        # ---------------------------------------------------------

        cursor.execute(
            """
            DELETE FROM daily_risk_report
            WHERE sim_date = %s
            """,
            (sim_date,),
        )

        deleted_rows = cursor.rowcount

        # ---------------------------------------------------------
        # Insert new reports
        # ---------------------------------------------------------

        insert_sql = """
            INSERT INTO daily_risk_report (
                patient_id,
                sim_date,
                trend_summary,
                lab_summary,
                operational_risk_flag,
                data_quality_note
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
        """

        for report in reports:

            cursor.execute(
                insert_sql,
                (
                    report["patient_id"],
                    report["sim_date"],
                    report["trend_summary"],
                    report["lab_summary"],
                    report["operational_risk_flag"],
                    report["data_quality_note"],
                ),
            )

        connection.commit()

        print(
            "----- Daily Risk Report Write -----"
        )

        print(
            f"Simulation date: {sim_date}"
        )

        print(
            f"Existing rows deleted: "
            f"{deleted_rows}"
        )

        print(
            f"Rows inserted: {len(reports)}"
        )

        print(
            "Daily risk report persisted successfully."
        )

        print(
            "------------------------------------"
        )

    except Exception as exc:

        if connection:
            connection.rollback()

        print(
            f"Failed to write daily risk report: "
            f"{exc}"
        )

        raise

    finally:

        if connection:
            connection.close()


# =========================================================
# QUALITY GATE
# =========================================================

def quality_gate(**context):
    """
    Validate the persisted daily risk report.
    """

    sim_date, _ = get_simulation_context(context)

    expected_patient_count = 20

    connection = None

    try:

        connection = get_postgres_connection()

        cursor = connection.cursor()

        # ---------------------------------------------------------
        # Expected report count
        # ---------------------------------------------------------

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM daily_risk_report
            WHERE sim_date = %s
            """,
            (sim_date,),
        )

        report_count = cursor.fetchone()[0]

        if report_count != expected_patient_count:

            raise ValueError(
                f"Quality gate failed: expected "
                f"{expected_patient_count} risk reports "
                f"for {sim_date}, found {report_count}."
            )

        # ---------------------------------------------------------
        # Valid risk flags
        # ---------------------------------------------------------

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM daily_risk_report
            WHERE sim_date = %s
              AND operational_risk_flag NOT IN (
                  'normal',
                  'watch',
                  'elevated'
              )
            """,
            (sim_date,),
        )

        invalid_flag_count = cursor.fetchone()[0]

        if invalid_flag_count > 0:

            raise ValueError(
                f"Quality gate failed: "
                f"{invalid_flag_count} report(s) contain "
                "an invalid operational risk flag."
            )

        # ---------------------------------------------------------
        # Required fields
        # ---------------------------------------------------------

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM daily_risk_report
            WHERE sim_date = %s
              AND (
                  patient_id IS NULL
                  OR trend_summary IS NULL
                  OR lab_summary IS NULL
                  OR operational_risk_flag IS NULL
                  OR data_quality_note IS NULL
              )
            """,
            (sim_date,),
        )

        null_field_count = cursor.fetchone()[0]

        if null_field_count > 0:

            raise ValueError(
                f"Quality gate failed: "
                f"{null_field_count} report(s) contain "
                "NULL required fields."
            )

        # ---------------------------------------------------------
        # Staged laboratory data
        # ---------------------------------------------------------

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM daily_lab_results
            WHERE sim_date = %s
            """,
            (sim_date,),
        )

        lab_count = cursor.fetchone()[0]

        if lab_count == 0:

            raise ValueError(
                f"Quality gate failed: no staged laboratory "
                f"data found for {sim_date}."
            )

        # ---------------------------------------------------------
        # Quality gate passed
        # ---------------------------------------------------------

        print(
            "----- Quality Gate -----"
        )

        print(
            f"Simulation date: {sim_date}"
        )

        print(
            f"Daily risk reports: "
            f"{report_count}"
        )

        print(
            f"Invalid risk flags: "
            f"{invalid_flag_count}"
        )

        print(
            f"Reports with NULL required fields: "
            f"{null_field_count}"
        )

        print(
            f"Staged laboratory rows: "
            f"{lab_count}"
        )

        print(
            "QUALITY GATE PASSED"
        )

        print(
            "------------------------"
        )

    except Exception as exc:

        print(
            f"Quality gate failed: "
            f"{exc}"
        )

        raise

    finally:

        if connection:
            connection.close()


# =========================================================
# AIRFLOW DAG
# =========================================================

with DAG(
    dag_id="pulsestream_daily_reconciliation",

    start_date=datetime(
        2026,
        9,
        28,
        tzinfo=timezone.utc,
    ),

    schedule="@daily",

    catchup=False,

    tags=[
        "pulsestream",
        "batch",
        "reconciliation",
    ],

    # ---------------------------------------------------------
    # DAG-level callbacks
    # ---------------------------------------------------------

    on_success_callback=pipeline_success_callback,

    on_failure_callback=pipeline_failure_callback,

) as dag:

    # ---------------------------------------------------------
    # Start pipeline
    # ---------------------------------------------------------

    start_pipeline = PythonOperator(
        task_id="start_pipeline",
        python_callable=create_pipeline_run,
    )

    # ---------------------------------------------------------
    # Wait for daily laboratory file
    # ---------------------------------------------------------

    wait_for_daily_labs = FileSensor(
        task_id="wait_for_daily_labs",

        filepath=(
            "mnt/c/Users/yasiru/Desktop/"
            "pulsestream/data/incoming/"
            "labs_{{ ds_nodash }}.csv"
        ),

        fs_conn_id="fs_default",

        poke_interval=10,

        timeout=300,

        mode="poke",
    )

    # ---------------------------------------------------------
    # Validate daily laboratory data
    # ---------------------------------------------------------

    validate_labs = PythonOperator(
        task_id="validate_daily_labs",
        python_callable=validate_daily_labs,
    )

    # ---------------------------------------------------------
    # Stage valid laboratory data
    # ---------------------------------------------------------

    stage_labs = PythonOperator(
        task_id="stage_daily_labs",
        python_callable=stage_daily_labs,
    )

    # ---------------------------------------------------------
    # Calculate authoritative daily risk
    # ---------------------------------------------------------

    calculate_risk = PythonOperator(
        task_id="calculate_daily_risk",
        python_callable=calculate_daily_risk,
    )

    # ---------------------------------------------------------
    # Persist daily risk report
    # ---------------------------------------------------------

    write_risk_report = PythonOperator(
        task_id="write_daily_risk_report",
        python_callable=write_daily_risk_report,
    )

    # ---------------------------------------------------------
    # Quality gate
    # ---------------------------------------------------------

    run_quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate,
    )

    # ---------------------------------------------------------
    # Pipeline dependency
    # ---------------------------------------------------------

    (
        start_pipeline
        >> wait_for_daily_labs
        >> validate_labs
        >> stage_labs
        >> calculate_risk
        >> write_risk_report
        >> run_quality_gate
    )