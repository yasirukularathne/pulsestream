# PulseStream

## Real-Time Hospital Patient Monitoring & Data Reconciliation Platform

PulseStream is a data-engineering prototype that demonstrates how continuous patient vital-sign events and daily laboratory results can be processed using a Lambda Architecture.

The platform combines real-time stream processing, batch reconciliation, operational storage, API serving, dashboard visualization and observability.

> **Important:** PulseStream uses 100% synthetic data. The operational risk indicators (`normal`, `watch`, `elevated`) are for monitoring demonstration only and are not clinical decision support or medical diagnosis.

---

## 1. Project Overview

Hospital monitoring environments can generate continuous streams of vital-sign measurements while laboratory results may arrive as scheduled daily batches.

PulseStream demonstrates how these two workloads can be handled using separate processing paths while maintaining a common operational data store.

### Business Question

> Which patients show concerning vital-sign trends right now, and how do yesterday's laboratory results affect the daily operational risk picture?

### Main Outputs

- Realtime patient vital-sign trends
- Operational risk indicators
- Realtime operational alerts
- Daily laboratory reconciliation
- Daily patient risk report
- Pipeline health information
- Monitoring metrics and dashboards

---

## 2. Architecture

PulseStream follows a Lambda Architecture consisting of:

### Speed Layer

```text
Bedside Vitals Generator
        |
        v
Apache Kafka
vitals.events
        |
        v
Spark Structured Streaming
        |
        +--> Validation
        +--> Deduplication
        +--> 30s Watermark
        +--> 90s Tumbling Windows
        +--> Trend Aggregation
        +--> Operational Risk Flag
        |
        +--> PostgreSQL

Invalid streaming events are routed to:

vitals.dead-letter
Batch Layer
Daily Laboratory CSV
        |
        v
Apache Airflow
        |
        v
FileSensor
        |
        v
Validation + Data Quality
        |
        v
Stage Valid Labs
        |
        v
Daily Risk Reconciliation
        |
        v
PostgreSQL
Serving Layer
PostgreSQL
     |
     v
FastAPI
     |
     +--> REST API
     |
     +--> Streamlit Dashboard
Observability
FastAPI Metrics
      |
      v
Prometheus
      |
      v
Grafana
      |
      v
Operational Alerts
3. Technology Stack
Technology	Purpose
Apache Kafka 4.0.1	Real-time event ingestion
Apache Spark Structured Streaming 4.2.0	Stream processing
Apache Airflow 3.1.0	Batch orchestration
PostgreSQL 16	Operational data storage
FastAPI	REST API serving
Streamlit	Monitoring dashboard
Prometheus	Metrics collection
Grafana 12.1.1	Monitoring and alerting
Docker Compose	Infrastructure management
pytest	Automated testing
Python	Data generation and application development
4. Project Structure
pulsestream/
│
├── airflow/
│   └── dags/
│       └── pulsestream_daily_reconciliation.py
│
├── api/
│   ├── __init__.py
│   ├── database.py
│   └── main.py
│
├── dashboard/
│   └── app.py
│
├── data/
│   ├── incoming/
│   └── processed/
│
├── database/
│   └── schema.sql
│
├── kafka/
│
├── monitoring/
│   └── prometheus/
│       └── prometheus.yml
│
├── producers/
│   ├── producer.py
│   ├── vitals_generator.py
│   ├── patient_generator.py
│   ├── lab_generator.py
│   ├── abnormality.py
│   ├── corruption.py
│   ├── duplicates.py
│   ├── sensor_dropout.py
│   └── ...
│
├── shared/
│   └── ...
│
├── spark/
│   └── kafka_stream.py
│
├── tests/
│   └── ...
│
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
5. Data Flow
Realtime Vital-Sign Flow

Synthetic patient vital-sign measurements are generated continuously.

Each event contains information such as:

patient_id
event_id
heart_rate
spo2
systolic_bp
diastolic_bp
temperature
timestamp

Events are published to:

vitals.events

Spark Structured Streaming then performs validation, deduplication, watermarking and window-based aggregation.

6. Streaming Processing

The streaming pipeline implements:

Validation

Events are checked for schema and acceptable measurement ranges.

Invalid events are routed to:

vitals.dead-letter
Deduplication

event_id is used to prevent duplicate events from entering the persistent raw-vitals store.

Event-Time Processing

A:

30-second watermark

is used to handle late-arriving events.

Window Aggregation

Patient measurements are aggregated using:

90-second tumbling windows

The pipeline calculates:

Average heart rate
Average SpO2
Average temperature
Number of abnormal events
Operational Risk Indicator

The prototype uses a simple rule-based classification:

0 abnormal events  -> normal
1 abnormal event   -> watch
2+ abnormal events -> elevated

These classifications are operational monitoring indicators only.

7. Batch Processing

Laboratory results are generated as daily CSV files.

Example fields include:

patient_id
test_type
result_value
reference_range
collected_at
sim_date

Apache Airflow detects the incoming laboratory file using a FileSensor.

The workflow performs:

File detection
Laboratory validation
Data-quality checks
Valid record staging
Rejected-row tracking
Daily risk reconciliation
Quality gate
Pipeline status recording

Pipeline execution information is stored in:

pipeline_runs
8. PostgreSQL Storage

PulseStream uses PostgreSQL as the operational data store.

Main tables include:

patients
raw_vitals
vitals_trends
daily_lab_results
rejected_rows
daily_risk_report
alerts
pipeline_runs

Idempotency is supported through database constraints and conflict-safe inserts.

Examples include:

event_id primary key for raw vital events
unique patient/window constraint for realtime trends
unique alert constraint for duplicate prevention
9. API

FastAPI provides access to the processed data.

Health Check
GET /health

Example:

{
  "status": "healthy",
  "service": "pulsestream-api"
}
Operational KPIs
GET /kpis

Example:

{
  "active_patients": 20,
  "watch_or_elevated_patients": 2,
  "risk_percentage": 10.0,
  "active_alerts": 12
}
Other Endpoints
GET /daily-risk
GET /patients/{patient_id}/trends
GET /patients/{patient_id}/risk
GET /alerts
GET /pipeline-health
GET /metrics
10. Dashboard

The Streamlit dashboard provides:

Active patient count
Watch/elevated patient count
Risk percentage
Active alerts
Patient risk overview
Realtime patient trends
Pipeline health
Daily reconciliation results
Recent pipeline runs
Operational alerts
11. Observability

PulseStream exposes Prometheus metrics through:

GET /metrics

Current metrics include:

pulsestream_api_requests_total
pulsestream_active_patients
pulsestream_watch_or_elevated_patients

Prometheus uses a 5-second scrape interval.

Grafana provides:

Active Patients
API Requests
API Request Rate
Watch / Elevated Patients

A Grafana-managed operational alert is configured for:

pulsestream_watch_or_elevated_patients > 0
12. Testing

The project includes automated tests using pytest.

Current verification:

36 passed

Additional testing covers:

Kafka ingestion
Event validation
Invalid-event handling
Dead-letter routing
Duplicate handling
Window aggregation
Operational risk classification
PostgreSQL persistence
Batch validation
Idempotency
API health
Pipeline health
13. Reliability Testing

Controlled failure tests were performed for:

Kafka

Kafka was stopped during event production.

The producer reported a connection failure.

After Kafka was restarted, event production successfully resumed.

PostgreSQL

PostgreSQL was stopped and the database connection failed as expected.

After PostgreSQL was restarted, the application successfully reconnected.

Spark

The Spark streaming application was stopped and restarted using its existing checkpoint location.

The application restored streaming state and continued processing new events.

14. Verified Test Results
Streaming
Events generated:        201
Accepted raw rows:       196
Batch
Laboratory records:       81
Valid records:            75
Rejected records:          6
Automated Tests
36 passed
Current Operational KPIs
Active patients:              20
Watch / elevated patients:     2
Risk percentage:              10.0%
Active alerts:                 12
15. Docker Infrastructure

Docker Compose provides reproducible infrastructure for:

Apache Kafka
PostgreSQL
Prometheus

Grafana is also containerized separately.

Application components such as Spark, Airflow and Streamlit are run in their respective host/WSL development environments.

Therefore, the project does not claim that every application component is contained in a single Docker Compose deployment.

16. Running the Project
16.1 Start Infrastructure
docker compose up -d

Check services:

docker compose ps

Expected infrastructure:

pulsestream-kafka
pulsestream-postgres
pulsestream-prometheus
16.2 Start FastAPI

From the project root:

uvicorn api.main:app --reload --port 8000

Check:

http://localhost:8000/health
16.3 Start Streamlit
streamlit run dashboard/app.py
16.4 Start Spark

On Windows, configure Java and Hadoop:

set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-21.0.12.101-hotspot
set HADOOP_HOME=C:\hadoop
set PATH=%HADOOP_HOME%\bin;%JAVA_HOME%\bin;%PATH%

Then:

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 spark\kafka_stream.py
16.5 Run the Producer
python producers/producer.py
16.6 Run Tests
python -m pytest -q
17. Configuration

Copy the example environment file:

.env.example

Example configuration:

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

Do not commit production credentials or secrets to the repository.

18. Scope and Limitations

PulseStream is an academic data-engineering prototype.

The project intentionally uses:

Synthetic patient data
A simulated hospital ward
Simplified operational risk rules
Local development infrastructure

The system is not intended to provide medical diagnosis or clinical decision support.

Production deployment would require additional security, scalability, data governance, reliability and clinical validation.

19. Future Improvements

Possible future extensions include:

Distributed/cloud deployment
Larger-scale event simulation
Kafka consumer-lag monitoring
Spark processing-latency metrics
More advanced data-quality monitoring
Enhanced alert routing
Historical operational analytics
Stronger authentication and authorization
Production-grade deployment automation
20. Academic Context

Module: EC8203 — Applied Big Data Engineering

Project: PulseStream — Real-Time Hospital Patient Monitoring & Data Reconciliation Platform

Architecture: Lambda Architecture

Data: 100% synthetic

Primary technologies: Kafka, Spark Structured Streaming, Airflow, PostgreSQL, FastAPI, Streamlit, Prometheus and Grafana
