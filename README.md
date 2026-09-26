## Real-Time Hospital Patient Monitoring & Data Reconciliation Platform

PulseStream is a **Lambda Architecture-based Big Data Engineering platform** designed to process synthetic hospital patient monitoring data in real time and reconcile daily laboratory results through a batch-processing pipeline.

The platform combines **Apache Kafka, Apache Spark Structured Streaming, Apache Airflow, PostgreSQL, FastAPI, Streamlit, Prometheus, Grafana, and Docker** to demonstrate a complete end-to-end data engineering workflow.

> **Important:** PulseStream uses synthetic data and is designed for **operational risk flagging only**. It is not a clinical decision-support system, diagnostic system, or medical prediction system.

---

## 📚 Academic Context

**Course:** EC8203 – Applied Big Data Engineering Mini-Project  
**Project:** PulseStream  
**Architecture:** Lambda Architecture  
**Data:** 100% synthetic  
**Primary Focus:** Real-time data processing, batch reconciliation, data quality, reliability, and observability

---

## 🎯 Project Objective

Hospital wards continuously generate patient vital-sign data while laboratory results are typically available as periodic batch data.

PulseStream demonstrates how these two different data sources can be processed using a unified Big Data architecture.

The platform answers the operational question:

> **"Which patients currently show abnormal vital-sign trends, and how do previous-day laboratory results affect the operational risk flag?"**

The system provides:

- Real-time patient vital monitoring
- Streaming data validation
- Duplicate and late-event handling
- Dead-letter processing
- Windowed trend analysis
- Rule-based operational risk flags
- Daily laboratory reconciliation
- Data-quality tracking
- Pipeline monitoring
- Operational dashboards

---

# 🏗️ Architecture

PulseStream follows a **Lambda Architecture** consisting of:

1. **Speed Layer**
2. **Batch Layer**
3. **Serving Layer**

```text
                         ┌─────────────────────────┐
                         │  Synthetic Vital Data   │
                         │       Producer          │
                         └────────────┬────────────┘
                                      │
                                      ▼
                              ┌───────────────┐
                              │ Apache Kafka  │
                              │ vitals.events │
                              └───────┬───────┘
                                      │
                                      ▼
                         ┌─────────────────────────┐
                         │ Spark Structured        │
                         │ Streaming               │
                         │                         │
                         │ • Validation            │
                         │ • Deduplication         │
                         │ • Watermarking          │
                         │ • Windowing             │
                         │ • Risk Rules            │
                         └────────────┬────────────┘
                                      │
                                      ▼
                              ┌───────────────┐
                              │  PostgreSQL   │
                              │               │
                              │ Speed Layer   │
                              └───────┬───────┘
                                      │
                                      │
       ┌──────────────────────────────┘
       │
       │                 BATCH LAYER
       │
       ▼
┌──────────────────┐
│ Synthetic Daily  │
│ Lab CSV          │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Apache Airflow   │
│                  │
│ FileSensor       │
│ Validation       │
│ Data Quality     │
│ Reconciliation   │
└────────┬─────────┘
         │
         ▼
┌──────────────────────────┐
│ PostgreSQL               │
│                          │
│ Daily Risk Report        │
│ Lab Results              │
│ Pipeline Runs            │
└────────────┬─────────────┘
             │
             ▼
     ┌──────────────────┐
     │    FastAPI       │
     │    Serving API   │
     └────────┬─────────┘
              │
              ▼
     ┌──────────────────┐
     │    Streamlit     │
     │    Dashboard     │
     └──────────────────┘


       ┌─────────────────────┐
       │ Prometheus          │
       │        +            │
       │ Grafana             │
       └─────────────────────┘
              │
              ▼
       Observability
⚡ Speed Layer

The speed layer processes patient vital-sign events continuously.

Streaming flow
Synthetic Producer
       ↓
Kafka
       ↓
Spark Structured Streaming
       ↓
Validation
       ↓
Deduplication
       ↓
Watermarking
       ↓
90-second Windows
       ↓
Risk Rules
       ↓
PostgreSQL
Kafka Topics
Topic	Purpose	Partitions
vitals.events	Valid/received vital events	3
vitals.dead-letter	Invalid events	1

Events are keyed by:

patient_id

This allows events belonging to the same patient to be consistently routed through Kafka partitions.

🧪 Synthetic Data Generation

PulseStream does not use real patient data.

The synthetic data generator simulates realistic operational conditions that a hospital monitoring platform may need to handle.

Patient simulation

The system generates:

15–30 synthetic patients
Patient-specific baseline heart rate
Patient-specific baseline SpO₂
Deterministic random generation using a configurable seed

Example:

{
  "patient_id": "P0001",
  "baseline_hr": 82,
  "baseline_spo2": 97.42
}
❤️ Vital Sign Generation

Each event contains:

patient_id
event_id
heart_rate
spo2
systolic_bp
diastolic_bp
temperature
timestamp

Example:

{
  "patient_id": "P0001",
  "heart_rate": 84,
  "spo2": 97.2,
  "systolic_bp": 121,
  "diastolic_bp": 78,
  "temperature": 36.8,
  "timestamp": "2026-09-22T12:00:05Z"
}
🚨 Simulated Data Quality Problems

The generator intentionally creates data-quality scenarios so the pipeline can demonstrate real-world streaming behavior.

Abnormal readings

Approximately 4% of generated events may contain abnormal values such as:

High heart rate
Low SpO₂
High temperature
High blood pressure
Invalid events

Approximately 1.5% of events may contain invalid values such as:

Heart rate = 500
SpO₂ = -10
Systolic BP = 500
Temperature = 60°C

These events are expected to be rejected by the streaming validation layer.

Duplicate events

Approximately 1% of events may be duplicated using the same event_id.

Out-of-order events

A configurable probability is used to simulate events whose timestamps arrive earlier than the previous event timestamp.

Sensor dropout

The generator can simulate monitoring gaps between:

20–60 seconds
Event cadence

Normal events are generated using a randomized cadence of:

3–8 seconds
Bursty events

The architecture supports bursts of events to simulate periods where several measurements arrive close together.

🛡️ Data Validation

PulseStream uses shared validation logic to reduce differences between streaming and batch processing.

The current validation rules include:

Field	Valid Range
Heart Rate	20–250
SpO₂	0–100
Systolic BP	20–300
Diastolic BP	20–300
Temperature	25–45°C
Blood Pressure	Systolic > Diastolic

Invalid streaming records are routed to the:

vitals.dead-letter

topic.

Invalid batch records are stored in:

rejected_rows
⏱️ Event-Time Processing

The streaming layer uses event-time concepts rather than relying only on processing time.

Watermark
30 seconds
Tumbling window
90 seconds
Deduplication

Events are deduplicated using:

event_id

within the configured watermark period.

Events arriving beyond the allowed watermark are treated as late events and excluded from closed trend windows while remaining measurable for data-quality monitoring.

⚠️ Operational Risk Flags

PulseStream generates operational risk flags using deterministic rule-based logic.

The available levels are:

normal
watch
elevated

These labels are intended to support operational monitoring only.

The system does not:

diagnose patients
predict medical outcomes
prescribe treatment
calculate clinical scoring systems
replace medical professionals
📦 Batch Layer

The batch layer processes daily laboratory data.

A simulated hospital day lasts:

5 real minutes

At the end of the simulated day, a laboratory CSV file is generated.

Example:

data/incoming/labs_YYYYMMDD.csv
Example laboratory record
patient_id,test_type,result_value,reference_range,collected_at
P0001,CBC,12.4,11-15,2026-09-22T14:30:00
P0002,Electrolytes,138,135-145,2026-09-22T14:31:00

The batch generator can simulate:

Missing reference ranges
Malformed rows
Duplicate patient/test records
Partial laboratory coverage
🔄 Airflow Batch Pipeline

Apache Airflow orchestrates the daily reconciliation workflow.

Lab CSV
   ↓
FileSensor
   ↓
Schema Validation
   ↓
Data Quality Validation
   ↓
Stage Laboratory Data
   ↓
Join with Vital Trends
   ↓
Daily Risk Calculation
   ↓
Write Daily Report
   ↓
Quality Gate
   ↓
Pipeline Status

The batch pipeline is designed to be idempotent.

Re-running the same simulated date does not create duplicate daily reports.

🗄️ PostgreSQL Data Model

PulseStream stores operational data in PostgreSQL.

Main tables
Table	Purpose
patients	Synthetic patient baselines
raw_vitals	Raw accepted vital events
vitals_trends	Windowed streaming trends
daily_lab_results	Laboratory batch results
rejected_rows	Rejected batch/stream records
daily_risk_report	Daily reconciled risk
alerts	Operational alerts
pipeline_runs	Pipeline execution metadata
🌐 Serving Layer

FastAPI exposes the processed data to downstream applications.

Endpoints
GET /health
GET /metrics
GET /realtime/patients
GET /realtime/patients/{patient_id}
GET /daily/report?date=
GET /alerts?active=true
GET /kpis

Example real-time response:

{
  "patient_id": "P0042",
  "window_end": "2026-09-22T12:05:30Z",
  "avg_hr": 88,
  "avg_spo2": 96,
  "abnormal_count": 1,
  "risk_flag_realtime": "watch"
}
📊 Streamlit Dashboard

The Streamlit dashboard provides an operational view of the pipeline.

Dashboard sections
Ward KPIs
Active patients
Patients currently under watch
Elevated patients
Data freshness
Processing latency
Patient Risk Table

Displays current operational risk status for monitored patients.

Vital Trends

Selected patient:

Heart rate trend
SpO₂ trend
Temperature trend
Active Alerts

Displays currently active operational alerts.

Daily Reconciliation

Compares:

Real-time trend
        +
Previous-day laboratory information
        ↓
Daily operational risk
Pipeline Health

Displays:

Kafka status
Processing latency
Data freshness
Error rate
Batch pipeline status
📈 Observability

PulseStream includes Prometheus and Grafana for monitoring both technical and business-level metrics.

Pipeline metrics
vitals_events_received_total
vitals_events_processed_total
vitals_processing_latency_seconds
kafka_consumer_lag
vitals_invalid_total
vitals_duplicate_total
vitals_late_total
airflow_batch_duration_seconds
postgres_write_latency_seconds
api_request_latency_seconds
pipeline_error_rate
patient_data_freshness_seconds
🚨 Alerting

The monitoring layer includes operational alert conditions such as:

Patient data freshness
No vital events received for a monitored patient > 120 seconds
Consecutive abnormal readings
≥ 3 consecutive abnormal readings within 90 seconds
Pipeline errors
Pipeline error rate > 5% over 5 minutes
Batch failure
Airflow daily reconciliation DAG failure
🧾 Structured Logging

PulseStream uses a common structured JSON logging format.

Example:

{
  "timestamp": "2026-09-22T12:05:31Z",
  "component": "spark",
  "level": "WARN",
  "event": "invalid_vitals_record",
  "patient_id": "P0007",
  "details": {
    "reason": "range_error"
  }
}

This makes logs easier to search, analyze, and monitor.

🐳 Deployment

The project is designed to run using Docker Compose.

Planned services include:

Kafka
PostgreSQL
Airflow Webserver
Airflow Scheduler
Airflow Init
Spark
FastAPI
Streamlit
Prometheus
Grafana

Kafka runs using:

Apache Kafka 4.0.1
KRaft mode

ZooKeeper is intentionally not used.

📁 Project Structure
pulsestream/
│
├── airflow/
│   └── dags/
│
├── api/
│
├── consumers/
│
├── dashboard/
│
├── data/
│   ├── incoming/
│   └── processed/
│
├── database/
│   └── schema.sql
│
├── docker/
│
├── configs/
│
├── docs/
│
├── kafka/
│
├── monitoring/
│   ├── grafana/
│   └── prometheus/
│
├── producers/
│   ├── abnormality.py
│   ├── cadence.py
│   ├── corruption.py
│   ├── duplicates.py
│   ├── dropout_timing.py
│   ├── event_emitter.py
│   ├── event_metadata.py
│   ├── event_sequence.py
│   ├── event_timing.py
│   ├── patient_generator.py
│   ├── sensor_dropout.py
│   ├── timestamp_utils.py
│   └── vitals_generator.py
│
├── shared/
│   └── validation.py
│
├── spark/
│
├── tests/
│
├── .env.example
├── .gitignore
├── README.md
└── requirements.txt
⚙️ Configuration

Create a local .env file based on:

.env.example

Example:

POSTGRES_USER=pulsestream
POSTGRES_PASSWORD=changeme
POSTGRES_DB=pulsestream

KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC_VITALS=vitals.events
KAFKA_TOPIC_DLQ=vitals.dead-letter

SIM_DAY_SECONDS=300
PRODUCER_PATIENT_COUNT=20
PRODUCER_SEED=42

API_PORT=8000

GRAFANA_ADMIN_PASSWORD=changeme

Never commit .env files or real credentials to Git.

🧪 Testing

PulseStream follows a test-driven incremental development approach.

The current test suite covers the synthetic data-generation layer.

Current test coverage includes:

Patient generation
Vital generation
Abnormality generation
Invalid data generation
Event metadata
Event IDs
Duplicate events
Event cadence
Sensor dropout
Out-of-order timestamps
Shared validation
Deterministic generation

Current test status:

36 passed

Run tests with:

python -m pytest -v
🔬 Failure Scenarios

The system is designed to demonstrate resilience against realistic pipeline failures.

Kafka unavailable

Expected behavior:

Producer
   ↓
Retry / error handling
   ↓
Structured logging
Spark restart

The streaming application should restart and continue processing available events.

PostgreSQL unavailable

API and pipeline components should expose controlled errors rather than crashing unexpectedly.

Malformed batch file

Airflow should reject the invalid input and record the failure.

Duplicate events

Duplicate event IDs should not produce duplicate trend records.

Late events

Late events should be handled according to the configured watermark policy.

Producer stopped

Freshness monitoring should detect the absence of incoming patient data.

📊 Business KPIs

PulseStream tracks several operational KPIs.

Active Patients

Number of patients currently being monitored.

Risk Distribution

Percentage of monitored patients currently classified as:

normal
watch
elevated
Time-to-Flag

Time between an abnormal event occurring and its operational flag becoming available.

Daily Reconciliation Completion

Percentage of simulated days for which the batch reconciliation completes successfully.

Patient Data Freshness

Time since the latest event was received for each monitored patient.

🔐 Data & Safety Considerations

PulseStream is intentionally designed around synthetic data.

The project:

Does not process real patient records
Does not store personally identifiable medical information
Does not provide medical diagnosis
Does not provide treatment recommendations
Does not implement clinical decision-support algorithms
Uses operational rule-based flags only

The purpose is to demonstrate data engineering architecture and reliability, not medical decision-making.

🛠️ Technology Stack
Technology	Purpose
Python	Data generation and application development
Apache Kafka	Real-time event streaming
Apache Spark	Stream processing
Apache Airflow	Batch orchestration
PostgreSQL	Persistent storage
FastAPI	Backend API
Streamlit	Operational dashboard
Prometheus	Metrics collection
Grafana	Monitoring and visualization
Docker	Containerized deployment
Pytest	Automated testing
Git / GitHub	Version control
