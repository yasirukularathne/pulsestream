from datetime import date

from fastapi import FastAPI, Query

from api.database import get_db_connection


app = FastAPI(
    title="PulseStream API",
    description="Serving API for the PulseStream hospital monitoring platform.",
    version="1.0.0",
)


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "pulsestream-api",
    }


@app.get("/daily-risk")
def get_daily_risk(
    sim_date: date = Query(..., description="Simulation date in YYYY-MM-DD format")
):
    conn = get_db_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    patient_id,
                    sim_date,
                    trend_summary,
                    lab_summary,
                    operational_risk_flag,
                    data_quality_note
                FROM daily_risk_report
                WHERE sim_date = %s
                ORDER BY patient_id;
                """,
                (sim_date,),
            )

            rows = cursor.fetchall()

        return {
            "sim_date": sim_date,
            "count": len(rows),
            "patients": [
                {
                    "patient_id": row[0],
                    "sim_date": row[1],
                    "trend_summary": row[2],
                    "lab_summary": row[3],
                    "operational_risk_flag": row[4],
                    "data_quality_note": row[5],
                }
                for row in rows
            ],
        }

    finally:
        conn.close()

@app.get("/patients/{patient_id}/trends")
def get_patient_trends(patient_id: str):
    conn = get_db_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    patient_id,
                    window_start,
                    window_end,
                    avg_hr,
                    avg_spo2,
                    avg_temp,
                    abnormal_count,
                    risk_flag_realtime
                FROM vitals_trends
                WHERE patient_id = %s
                ORDER BY window_start;
                """,
                (patient_id,),
            )

            rows = cursor.fetchall()

        return {
            "patient_id": patient_id,
            "count": len(rows),
            "trends": [
                {
                    "patient_id": row[0],
                    "window_start": row[1],
                    "window_end": row[2],
                    "avg_hr": float(row[3]) if row[3] is not None else None,
                    "avg_spo2": float(row[4]) if row[4] is not None else None,
                    "avg_temp": float(row[5]) if row[5] is not None else None,
                    "abnormal_count": row[6],
                    "risk_flag_realtime": row[7],
                }
                for row in rows
            ],
        }

    finally:
        conn.close()

@app.get("/patients/{patient_id}/risk")
def get_patient_risk(patient_id: str):
    conn = get_db_connection()

    try:
        with conn.cursor() as cursor:
            # Latest realtime risk
            cursor.execute(
                """
                SELECT
                    risk_flag_realtime,
                    window_start,
                    window_end,
                    abnormal_count
                FROM vitals_trends
                WHERE patient_id = %s
                ORDER BY window_end DESC
                LIMIT 1;
                """,
                (patient_id,),
            )

            realtime_row = cursor.fetchone()

            # Latest daily operational risk
            cursor.execute(
                """
                SELECT
                    sim_date,
                    operational_risk_flag,
                    trend_summary,
                    lab_summary,
                    data_quality_note
                FROM daily_risk_report
                WHERE patient_id = %s
                ORDER BY sim_date DESC
                LIMIT 1;
                """,
                (patient_id,),
            )

            daily_row = cursor.fetchone()

        return {
            "patient_id": patient_id,
            "realtime": (
                {
                    "risk_flag": realtime_row[0],
                    "window_start": realtime_row[1],
                    "window_end": realtime_row[2],
                    "abnormal_count": realtime_row[3],
                }
                if realtime_row
                else None
            ),
            "daily": (
                {
                    "sim_date": daily_row[0],
                    "operational_risk_flag": daily_row[1],
                    "trend_summary": daily_row[2],
                    "lab_summary": daily_row[3],
                    "data_quality_note": daily_row[4],
                }
                if daily_row
                else None
            ),
        }

    finally:
        conn.close()

@app.get("/alerts")
def get_alerts():
    conn = get_db_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    patient_id,
                    rule_triggered,
                    severity,
                    triggered_at,
                    resolved_at,
                    source_layer
                FROM alerts
                ORDER BY triggered_at DESC;
                """
            )

            rows = cursor.fetchall()

        return {
            "count": len(rows),
            "alerts": [
                {
                    "id": row[0],
                    "patient_id": row[1],
                    "rule_triggered": row[2],
                    "severity": row[3],
                    "triggered_at": row[4],
                    "resolved_at": row[5],
                    "source_layer": row[6],
                }
                for row in rows
            ],
        }

    finally:
        conn.close()
@app.get("/kpis")
def get_kpis():
    conn = get_db_connection()

    try:
        with conn.cursor() as cursor:
            # Active patients from the patient registry
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM patients;
                """
            )
            active_patients = cursor.fetchone()[0]

            # Latest realtime risk per patient
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT ON (patient_id)
                        patient_id,
                        risk_flag_realtime
                    FROM vitals_trends
                    ORDER BY patient_id, window_end DESC
                ) latest
                WHERE risk_flag_realtime IN ('watch', 'elevated');
                """
            )
            watch_or_elevated_patients = cursor.fetchone()[0]

            # Currently unresolved alerts
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM alerts
                WHERE resolved_at IS NULL;
                """
            )
            active_alerts = cursor.fetchone()[0]

        risk_percentage = (
            round(
                (watch_or_elevated_patients / active_patients) * 100,
                2,
            )
            if active_patients > 0
            else 0.0
        )

        return {
            "active_patients": active_patients,
            "watch_or_elevated_patients": watch_or_elevated_patients,
            "risk_percentage": risk_percentage,
            "active_alerts": active_alerts,
        }

    finally:
        conn.close()
