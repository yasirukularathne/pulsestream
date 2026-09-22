CREATE TABLE patients (
    patient_id VARCHAR(20) PRIMARY KEY,
    baseline_hr INT NOT NULL,
    baseline_spo2 DECIMAL(5,2) NOT NULL,
    admitted_at TIMESTAMP NOT NULL
);

CREATE TABLE raw_vitals (
    event_id VARCHAR(100) PRIMARY KEY,
    patient_id VARCHAR(20) NOT NULL,
    heart_rate INT,
    spo2 DECIMAL(5,2),
    systolic_bp INT,
    diastolic_bp INT,
    temperature DECIMAL(4,2),
    event_ts TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP NOT NULL
);


CREATE TABLE vitals_trends (
    id SERIAL PRIMARY KEY,
    patient_id VARCHAR(20) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    avg_hr DECIMAL(6,2),
    avg_spo2 DECIMAL(5,2),
    avg_temp DECIMAL(5,2),
    abnormal_count INT DEFAULT 0,
    risk_flag_realtime VARCHAR(20) NOT NULL
);

CREATE TABLE daily_lab_results (
    id SERIAL PRIMARY KEY,
    patient_id VARCHAR(20) NOT NULL,
    test_type VARCHAR(50) NOT NULL,
    result_value VARCHAR(100),
    reference_range VARCHAR(100),
    collected_at TIMESTAMP NOT NULL,
    sim_date DATE NOT NULL
);

CREATE TABLE rejected_rows (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    raw_content TEXT,
    reason VARCHAR(100) NOT NULL,
    rejected_at TIMESTAMP NOT NULL
);

CREATE TABLE daily_risk_report (
    id SERIAL PRIMARY KEY,
    patient_id VARCHAR(20) NOT NULL,
    sim_date DATE NOT NULL,
    trend_summary TEXT,
    lab_summary TEXT,
    operational_risk_flag VARCHAR(20) NOT NULL,
    data_quality_note TEXT
);

CREATE TABLE alerts (
    id SERIAL PRIMARY KEY,
    patient_id VARCHAR(20) NOT NULL,
    rule_triggered VARCHAR(200) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    triggered_at TIMESTAMP NOT NULL,
    resolved_at TIMESTAMP,
    source_layer VARCHAR(20) NOT NULL
);

CREATE TABLE pipeline_runs (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    status VARCHAR(30) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    records_processed INT DEFAULT 0,
    records_rejected INT DEFAULT 0
);