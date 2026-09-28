from datetime import datetime, timezone

from api.database import get_db_connection


def register_patients(patients):
    conn = get_db_connection()

    try:
        with conn.cursor() as cursor:
            for patient in patients:
                cursor.execute(
                    """
                    INSERT INTO patients (
                        patient_id,
                        baseline_hr,
                        baseline_spo2,
                        admitted_at
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (patient_id)
                    DO UPDATE SET
                        baseline_hr = EXCLUDED.baseline_hr,
                        baseline_spo2 = EXCLUDED.baseline_spo2;
                    """,
                    (
                        patient["patient_id"],
                        patient["baseline_hr"],
                        patient["baseline_spo2"],
                        datetime.now(timezone.utc),
                    ),
                )

        conn.commit()

    finally:
        conn.close()