import requests
import pandas as pd
import streamlit as st


API_BASE_URL = "http://127.0.0.1:8000"
SIM_DATE = "2026-09-28"


# ---------------------------------------------------------
# Page configuration
# ---------------------------------------------------------

st.set_page_config(
    page_title="PulseStream",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------
# Styling
# ---------------------------------------------------------

st.markdown(
    """
    <style>
        .main {
            padding-top: 1rem;
        }

        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }

        .dashboard-title {
            font-size: 2.2rem;
            font-weight: 700;
            margin-bottom: 0.2rem;
        }

        .dashboard-subtitle {
            color: #6b7280;
            font-size: 1rem;
            margin-bottom: 1.5rem;
        }

        .section-title {
            font-size: 1.35rem;
            font-weight: 650;
            margin-top: 1rem;
            margin-bottom: 0.8rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------
# API helper
# ---------------------------------------------------------

def get_api_data(endpoint, params=None):
    try:
        response = requests.get(
            f"{API_BASE_URL}{endpoint}",
            params=params,
            timeout=5,
        )

        response.raise_for_status()

        return response.json()

    except requests.RequestException:
        return None


# ---------------------------------------------------------
# Header
# ---------------------------------------------------------

st.markdown(
    '<div class="dashboard-title">🏥 PulseStream</div>',
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="dashboard-subtitle">'
    "Real-Time Hospital Patient Monitoring & Data Reconciliation Platform"
    "</div>",
    unsafe_allow_html=True,
)


# ---------------------------------------------------------
# API health
# ---------------------------------------------------------

health_data = get_api_data("/health")

if health_data:
    st.success("● API Connected")
else:
    st.error("● API Unavailable — Start FastAPI on port 8000.")


# ---------------------------------------------------------
# Sidebar
# ---------------------------------------------------------

with st.sidebar:

    st.header("PulseStream")

    st.caption("Monitoring Dashboard")

    st.divider()

    st.subheader("Simulation")

    st.write(
        f"Simulation date: **{SIM_DATE}**"
    )

    st.divider()

    st.subheader("System")

    if health_data:
        st.success("API: Online")
    else:
        st.error("API: Offline")

    st.caption(
        "Operational risk monitoring only"
    )


# ---------------------------------------------------------
# KPI section
# ---------------------------------------------------------

st.markdown(
    '<div class="section-title">Operational Overview</div>',
    unsafe_allow_html=True,
)


kpi_data = get_api_data("/kpis")

pipeline_health_data = get_api_data(
    "/pipeline-health"
)


if kpi_data:

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Active Patients",
            kpi_data.get(
                "active_patients",
                0,
            ),
        )

    with col2:
        st.metric(
            "Watch / Elevated",
            kpi_data.get(
                "watch_or_elevated_patients",
                0,
            ),
        )

    with col3:
        st.metric(
            "Risk Percentage",
            f"{kpi_data.get('risk_percentage', 0):.2f}%",
        )

    with col4:
        st.metric(
            "Active Alerts",
            kpi_data.get(
                "active_alerts",
                0,
            ),
        )

else:

    st.warning(
        "Unable to load KPI information."
    )


st.divider()


# ---------------------------------------------------------
# Daily patient risk
# ---------------------------------------------------------

st.markdown(
    '<div class="section-title">Patient Risk Overview</div>',
    unsafe_allow_html=True,
)


daily_risk_data = get_api_data(
    "/daily-risk",
    params={
        "sim_date": SIM_DATE
    },
)


patients = []


if daily_risk_data:

    patients = daily_risk_data.get(
        "patients",
        [],
    )

    if patients:

        normal_count = sum(
            1
            for patient in patients
            if patient.get(
                "operational_risk_flag"
            ) == "normal"
        )

        watch_count = sum(
            1
            for patient in patients
            if patient.get(
                "operational_risk_flag"
            ) == "watch"
        )

        elevated_count = sum(
            1
            for patient in patients
            if patient.get(
                "operational_risk_flag"
            ) == "elevated"
        )

        summary_col1, summary_col2, summary_col3 = st.columns(3)

        with summary_col1:
            st.success(
                f"🟢 Normal: {normal_count}"
            )

        with summary_col2:
            st.warning(
                f"🟡 Watch: {watch_count}"
            )

        with summary_col3:
            st.error(
                f"🔴 Elevated: {elevated_count}"
            )

        table_data = []

        for patient in patients:

            table_data.append(
                {
                    "Patient ID": patient.get(
                        "patient_id",
                        "-",
                    ),
                    "Risk": patient.get(
                        "operational_risk_flag",
                        "unknown",
                    ).upper(),
                    "Trend Summary": patient.get(
                        "trend_summary",
                        "-",
                    ),
                    "Lab Summary": patient.get(
                        "lab_summary",
                        "-",
                    ),
                    "Data Quality": patient.get(
                        "data_quality_note",
                        "-",
                    ),
                }
            )

        st.dataframe(
            table_data,
            width="stretch",
            hide_index=True,
        )

    else:

        st.info(
            f"No daily risk records available "
            f"for {SIM_DATE}."
        )

else:

    st.error(
        "Failed to load daily patient risk data."
    )


st.divider()


# ---------------------------------------------------------
# Realtime patient monitoring
# ---------------------------------------------------------

st.markdown(
    '<div class="section-title">Realtime Patient Monitoring</div>',
    unsafe_allow_html=True,
)


if patients:

    patient_ids = [
        patient.get("patient_id")
        for patient in patients
        if patient.get("patient_id")
    ]

    selected_patient = st.selectbox(
        "Select Patient",
        patient_ids,
        key="selected_patient",
    )

    patient_trends = get_api_data(
        f"/patients/{selected_patient}/trends"
    )

    patient_risk = get_api_data(
        f"/patients/{selected_patient}/risk"
    )

    if patient_trends and patient_risk:

        trends = patient_trends.get(
            "trends",
            [],
        )

        realtime_risk = patient_risk.get(
            "realtime"
        )

        # -------------------------------------------------
        # Current realtime risk
        # -------------------------------------------------

        if realtime_risk:

            risk_flag = realtime_risk.get(
                "risk_flag",
                "unknown",
            )

            abnormal_count = realtime_risk.get(
                "abnormal_count",
                0,
            )

            risk_col, abnormal_col = st.columns(2)

            with risk_col:

                if risk_flag == "elevated":

                    st.error(
                        f"🔴 Realtime Risk: "
                        f"{risk_flag.upper()}"
                    )

                elif risk_flag == "watch":

                    st.warning(
                        f"🟡 Realtime Risk: "
                        f"{risk_flag.upper()}"
                    )

                else:

                    st.success(
                        f"🟢 Realtime Risk: "
                        f"{risk_flag.upper()}"
                    )

            with abnormal_col:

                st.metric(
                    "Abnormal Events",
                    abnormal_count,
                )


        # -------------------------------------------------
        # Latest monitoring window
        # -------------------------------------------------

        if trends:

            latest = trends[-1]

            st.write(
                "### Latest Monitoring Window"
            )

            metric1, metric2, metric3 = st.columns(3)

            with metric1:

                avg_hr = latest.get(
                    "avg_hr"
                )

                st.metric(
                    "Average Heart Rate",
                    f"{avg_hr:.2f}"
                    if avg_hr is not None
                    else "N/A",
                )

            with metric2:

                avg_spo2 = latest.get(
                    "avg_spo2"
                )

                st.metric(
                    "Average SpO₂",
                    f"{avg_spo2:.2f}%"
                    if avg_spo2 is not None
                    else "N/A",
                )

            with metric3:

                avg_temp = latest.get(
                    "avg_temp"
                )

                st.metric(
                    "Average Temperature",
                    f"{avg_temp:.2f} °C"
                    if avg_temp is not None
                    else "N/A",
                )


            # -------------------------------------------------
            # Vital sign trend charts
            # -------------------------------------------------

            st.write(
                "### Vital Sign Trends"
            )

            chart_data = []

            for trend in trends:

                chart_data.append(
                    {
                        "Window": trend.get(
                            "window_end"
                        ),
                        "Heart Rate": trend.get(
                            "avg_hr"
                        ),
                        "SpO₂": trend.get(
                            "avg_spo2"
                        ),
                        "Temperature": trend.get(
                            "avg_temp"
                        ),
                    }
                )

            if chart_data:

                chart_df = pd.DataFrame(
                    chart_data
                )

                chart_df["Window"] = pd.to_datetime(
                    chart_df["Window"]
                )

                chart_df = chart_df.set_index(
                    "Window"
                )

                st.write(
                    "#### Heart Rate"
                )

                st.line_chart(
                    chart_df[
                        ["Heart Rate"]
                    ],
                    width="stretch",
                )

                st.write(
                    "#### SpO₂"
                )

                st.line_chart(
                    chart_df[
                        ["SpO₂"]
                    ],
                    width="stretch",
                )

                st.write(
                    "#### Temperature"
                )

                st.line_chart(
                    chart_df[
                        ["Temperature"]
                    ],
                    width="stretch",
                )


            # -------------------------------------------------
            # Recent trend windows
            # -------------------------------------------------

            st.write(
                "### Recent Trend Windows"
            )

            trend_table = []

            for trend in trends:

                trend_table.append(
                    {
                        "Window Start": trend.get(
                            "window_start",
                            "-",
                        ),
                        "Window End": trend.get(
                            "window_end",
                            "-",
                        ),
                        "Avg HR": trend.get(
                            "avg_hr",
                            "-",
                        ),
                        "Avg SpO₂": trend.get(
                            "avg_spo2",
                            "-",
                        ),
                        "Avg Temperature": trend.get(
                            "avg_temp",
                            "-",
                        ),
                        "Abnormal Events": trend.get(
                            "abnormal_count",
                            0,
                        ),
                        "Risk": trend.get(
                            "risk_flag_realtime",
                            "-",
                        ).upper(),
                    }
                )

            st.dataframe(
                trend_table,
                width="stretch",
                hide_index=True,
            )

        else:

            st.info(
                f"No realtime trend data available "
                f"for {selected_patient}."
            )

    else:

        st.warning(
            f"Unable to load realtime monitoring data "
            f"for {selected_patient}."
        )

else:

    st.info(
        "No patients available for realtime monitoring."
    )


st.divider()


# ---------------------------------------------------------
# Daily reconciliation
# ---------------------------------------------------------

st.markdown(
    '<div class="section-title">Daily Reconciliation</div>',
    unsafe_allow_html=True,
)


if daily_risk_data:

    report_patients = daily_risk_data.get(
        "patients",
        []
    )

    report_count = len(report_patients)

    expected_patients = (
        kpi_data.get("active_patients", 0)
        if kpi_data
        else 0
    )

    normal_reports = sum(
        1
        for patient in report_patients
        if patient.get(
            "operational_risk_flag"
        ) == "normal"
    )

    watch_reports = sum(
        1
        for patient in report_patients
        if patient.get(
            "operational_risk_flag"
        ) == "watch"
    )

    elevated_reports = sum(
        1
        for patient in report_patients
        if patient.get(
            "operational_risk_flag"
        ) == "elevated"
    )

    reconciliation_col1, reconciliation_col2, reconciliation_col3 = (
        st.columns(3)
    )

    with reconciliation_col1:

        st.metric(
            "Reports Generated",
            report_count,
        )

    with reconciliation_col2:

        st.metric(
            "Expected Patients",
            expected_patients,
        )

    with reconciliation_col3:

        if (
            report_count == expected_patients
            and expected_patients > 0
        ):

            st.success(
                "✓ Reconciliation Complete"
            )

        else:

            st.warning(
                "⚠ Reconciliation Incomplete"
            )


    st.write(
        "#### Daily Risk Distribution"
    )

    reconciliation_summary = pd.DataFrame(
        {
            "Risk": [
                "Normal",
                "Watch",
                "Elevated",
            ],
            "Patients": [
                normal_reports,
                watch_reports,
                elevated_reports,
            ],
        }
    )

    st.bar_chart(
        reconciliation_summary.set_index(
            "Risk"
        ),
        width="stretch",
    )


    st.write(
        f"Daily reconciliation for **{SIM_DATE}** "
        f"contains **{report_count}** patient reports."
    )

else:

    st.error(
        "Daily reconciliation data unavailable."
    )


st.divider()


# ---------------------------------------------------------
# Pipeline Health
# ---------------------------------------------------------

st.markdown(
    '<div class="section-title">Pipeline Health</div>',
    unsafe_allow_html=True,
)


if pipeline_health_data:

    pipeline_runs = pipeline_health_data.get(
        "runs",
        [],
    )

    if pipeline_runs:

        latest_run = pipeline_runs[0]

        status = latest_run.get(
            "status",
            "unknown",
        )

        dag_id = latest_run.get(
            "dag_id",
            "-",
        )

        run_id = latest_run.get(
            "run_id",
            "-",
        )

        started_at = latest_run.get(
            "started_at",
            "-",
        )

        ended_at = latest_run.get(
            "ended_at",
            "-",
        )

        records_processed = latest_run.get(
            "records_processed",
            0,
        )

        records_rejected = latest_run.get(
            "records_rejected",
            0,
        )


        # ---------------------------------------------
        # Calculate duration
        # ---------------------------------------------

        duration_text = "N/A"

        try:

            start_time = pd.to_datetime(
                started_at
            )

            end_time = pd.to_datetime(
                ended_at
            )

            duration_seconds = (
                end_time - start_time
            ).total_seconds()

            duration_text = (
                f"{duration_seconds:.2f} seconds"
            )

        except (TypeError, ValueError):

            duration_text = "N/A"


        # ---------------------------------------------
        # Pipeline status
        # ---------------------------------------------

        if status == "success":

            st.success(
                "🟢 Pipeline Status: SUCCESS"
            )

        elif status == "failed":

            st.error(
                "🔴 Pipeline Status: FAILED"
            )

        elif status == "running":

            st.warning(
                "🟡 Pipeline Status: RUNNING"
            )

        else:

            st.info(
                f"Pipeline Status: {status.upper()}"
            )


        # ---------------------------------------------
        # Pipeline metrics
        # ---------------------------------------------

        pipeline_col1, pipeline_col2, pipeline_col3, pipeline_col4 = (
            st.columns(4)
        )

        with pipeline_col1:

            st.metric(
                "Records Processed",
                records_processed,
            )

        with pipeline_col2:

            st.metric(
                "Records Rejected",
                records_rejected,
            )

        with pipeline_col3:

            st.metric(
                "Run Duration",
                duration_text,
            )

        with pipeline_col4:

            st.metric(
                "Recent Runs",
                pipeline_health_data.get(
                    "count",
                    0,
                ),
            )


        # ---------------------------------------------
        # Pipeline details
        # ---------------------------------------------

        st.write(
            "#### Latest Pipeline Run"
        )

        pipeline_details = {
            "DAG": dag_id,
            "Run ID": run_id,
            "Status": status.upper(),
            "Started": started_at,
            "Ended": ended_at,
        }

        st.dataframe(
            pd.DataFrame(
                [
                    pipeline_details
                ]
            ),
            width="stretch",
            hide_index=True,
        )

    else:

        st.info(
            "No pipeline runs have been recorded yet."
        )

else:

    st.error(
        "Failed to load pipeline health information."
    )


st.divider()


# ---------------------------------------------------------
# Alerts
# ---------------------------------------------------------

st.markdown(
    '<div class="section-title">Recent Alerts</div>',
    unsafe_allow_html=True,
)


alerts_data = get_api_data("/alerts")


if alerts_data:

    alerts = alerts_data.get(
        "alerts",
        [],
    )

    if alerts:

        for alert in alerts[:10]:

            patient_id = alert.get(
                "patient_id",
                "-",
            )

            severity = alert.get(
                "severity",
                "unknown",
            )

            rule = alert.get(
                "rule_triggered",
                "-",
            )

            triggered_at = alert.get(
                "triggered_at",
                "-",
            )

            if severity == "elevated":

                st.error(
                    f"🔴 **{patient_id} — ELEVATED**  \n"
                    f"{rule}  \n"
                    f"Triggered: {triggered_at}"
                )

            elif severity == "watch":

                st.warning(
                    f"🟡 **{patient_id} — WATCH**  \n"
                    f"{rule}  \n"
                    f"Triggered: {triggered_at}"
                )

            else:

                st.info(
                    f"ℹ️ **{patient_id}**  \n"
                    f"{rule}  \n"
                    f"Triggered: {triggered_at}"
                )

    else:

        st.success(
            "No active alerts."
        )

else:

    st.error(
        "Failed to load alert information."
    )


st.divider()


# ---------------------------------------------------------
# Footer
# ---------------------------------------------------------

st.caption(
    "PulseStream • Synthetic operational monitoring data • "
    "Not clinical decision support"
)