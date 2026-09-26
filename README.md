# PulseStream

## Real-Time Hospital Patient Monitoring & Data Reconciliation Platform

PulseStream is a Lambda Architecture-based Big Data Engineering platform designed to process synthetic hospital patient monitoring data in real time and reconcile daily laboratory results through a batch-processing pipeline.

The platform combines Apache Kafka, Apache Spark Structured Streaming, Apache Airflow, PostgreSQL, FastAPI, Streamlit, Prometheus, Grafana, and Docker to demonstrate a complete end-to-end data engineering workflow.

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
