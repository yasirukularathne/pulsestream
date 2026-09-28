import os
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    lit,
    max as spark_max,
    min as spark_min,
    struct,
    sum as spark_sum,
    to_json,
    when,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


load_dotenv()


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

KAFKA_TOPIC_VITALS = os.getenv(
    "KAFKA_TOPIC_VITALS",
    "vitals.events",
)

KAFKA_TOPIC_DLQ = os.getenv(
    "KAFKA_TOPIC_DLQ",
    "vitals.dead-letter",
)

POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "localhost",
)

POSTGRES_PORT = int(
    os.getenv(
        "POSTGRES_PORT",
        "5433",
    )
)

POSTGRES_DB = os.getenv(
    "POSTGRES_DB",
    "pulsestream",
)

POSTGRES_USER = os.getenv(
    "POSTGRES_USER",
    "pulsestream",
)

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "changeme",
)


# ============================================================
# Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("PulseStreamVitals")
    .master("local[2]")
    .config(
        "spark.sql.shuffle.partitions",
        "4",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# Event Schema
# ============================================================

event_schema = StructType(
    [
        StructField(
            "patient_id",
            StringType(),
            False,
        ),
        StructField(
            "heart_rate",
            IntegerType(),
            True,
        ),
        StructField(
            "spo2",
            DoubleType(),
            True,
        ),
        StructField(
            "systolic_bp",
            IntegerType(),
            True,
        ),
        StructField(
            "diastolic_bp",
            IntegerType(),
            True,
        ),
        StructField(
            "temperature",
            DoubleType(),
            True,
        ),
        StructField(
            "event_id",
            StringType(),
            False,
        ),
        StructField(
            "timestamp",
            StringType(),
            False,
        ),
    ]
)


# ============================================================
# Read from Kafka
# ============================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS,
    )
    .option(
        "subscribe",
        KAFKA_TOPIC_VITALS,
    )
    .option(
        "startingOffsets",
        "latest",
    )
    .option(
        "failOnDataLoss",
        "false",
    )
    .load()
)


# ============================================================
# Parse Kafka JSON
# ============================================================

parsed_stream = (
    raw_stream
    .selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS json_value",
    )
    .select(
        from_json(
            col("json_value"),
            event_schema,
        ).alias("data")
    )
    .select("data.*")
)


# ============================================================
# Data Validation
# ============================================================

validated_stream = (
    parsed_stream
    .withColumn(
        "validation_status",
        when(
            col("patient_id").isNull()
            | col("event_id").isNull()
            | col("timestamp").isNull()
            | col("heart_rate").isNull()
            | col("spo2").isNull()
            | col("systolic_bp").isNull()
            | col("diastolic_bp").isNull()
            | col("temperature").isNull(),
            lit("invalid"),
        )
        .when(
            (col("heart_rate") < 60)
            | (col("heart_rate") > 180),
            lit("invalid"),
        )
        .when(
            (col("spo2") < 70)
            | (col("spo2") > 100),
            lit("invalid"),
        )
        .when(
            (col("systolic_bp") < 70)
            | (col("systolic_bp") > 250),
            lit("invalid"),
        )
        .when(
            (col("diastolic_bp") < 40)
            | (col("diastolic_bp") > 150),
            lit("invalid"),
        )
        .when(
            (col("temperature") < 30)
            | (col("temperature") > 45),
            lit("invalid"),
        )
        .otherwise(
            lit("valid")
        ),
    )
)


# ============================================================
# Invalid Events → Dead Letter Queue
# ============================================================

invalid_stream = (
    validated_stream
    .filter(
        col("validation_status") == "invalid"
    )
    .select(
        to_json(
            struct(
                "patient_id",
                "heart_rate",
                "spo2",
                "systolic_bp",
                "diastolic_bp",
                "temperature",
                "event_id",
                "timestamp",
            )
        ).alias("value")
    )
)


dlq_query = (
    invalid_stream
    .writeStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS,
    )
    .option(
        "topic",
        KAFKA_TOPIC_DLQ,
    )
    .option(
        "checkpointLocation",
        "spark/checkpoints/dlq-test",
    )
    .outputMode("append")
    .start()
)


# ============================================================
# Valid Events
# ============================================================

valid_stream = (
    validated_stream
    .filter(
        col("validation_status") == "valid"
    )
    .drop("validation_status")
)


# ============================================================
# Event Timestamp
# ============================================================

timestamped_stream = (
    valid_stream
    .withColumn(
        "event_timestamp",
        col("timestamp").cast("timestamp"),
    )
)


# ============================================================
# Watermark + Deduplication
# ============================================================

deduplicated_stream = (
    timestamped_stream
    .withWatermark(
        "event_timestamp",
        "30 seconds",
    )
    .dropDuplicates(
        ["event_id"]
    )
)


# ============================================================
# RAW VITALS → PostgreSQL
# ============================================================

def write_raw_vitals_to_postgres(
    batch_df,
    batch_id,
):
    rows = batch_df.collect()

    if not rows:
        return

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

        ingested_at = datetime.now(
            timezone.utc
        ).replace(
            tzinfo=None
        )

        inserted_count = 0

        for row in rows:

            cursor.execute(
                """
                INSERT INTO raw_vitals (
                    event_id,
                    patient_id,
                    heart_rate,
                    spo2,
                    systolic_bp,
                    diastolic_bp,
                    temperature,
                    event_ts,
                    ingested_at
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (event_id)
                DO NOTHING;
                """,
                (
                    row["event_id"],
                    row["patient_id"],
                    row["heart_rate"],
                    row["spo2"],
                    row["systolic_bp"],
                    row["diastolic_bp"],
                    row["temperature"],
                    row["event_timestamp"],
                    ingested_at,
                ),
            )

            if cursor.rowcount == 1:
                inserted_count += 1

        connection.commit()

        print(
            f"PostgreSQL raw batch {batch_id}: "
            f"inserted {inserted_count} raw vital rows"
        )

    except Exception as exc:

        if connection:
            connection.rollback()

        print(
            f"PostgreSQL raw batch {batch_id} failed: "
            f"{exc}"
        )

        raise

    finally:

        if connection:
            connection.close()


# ============================================================
# Write Accepted Raw Events to PostgreSQL
# ============================================================

raw_vitals_query = (
    deduplicated_stream
    .writeStream
    .outputMode("append")
    .foreachBatch(
        write_raw_vitals_to_postgres
    )
    .option(
        "checkpointLocation",
        "spark/checkpoints/raw-vitals",
    )
    .trigger(
        processingTime="5 seconds"
    )
    .start()
)


# ============================================================
# Abnormality Detection
# ============================================================

classified_stream = (
    deduplicated_stream
    .withColumn(
        "is_abnormal",
        when(
            (col("heart_rate") < 60)
            | (col("heart_rate") > 180)
            | (col("spo2") < 90)
            | (col("systolic_bp") < 90)
            | (col("systolic_bp") > 180)
            | (col("diastolic_bp") < 60)
            | (col("diastolic_bp") > 120)
            | (col("temperature") < 36)
            | (col("temperature") > 38),
            1,
        )
        .otherwise(0),
    )
)


# ============================================================
# 90-Second Tumbling Window
# ============================================================

windowed_stream = (
    classified_stream
    .groupBy(
        col("patient_id"),
        window(
            col("event_timestamp"),
            "90 seconds",
        ),
    )
    .agg(
        avg("heart_rate").alias(
            "avg_heart_rate"
        ),
        spark_min("heart_rate").alias(
            "min_hr"
        ),
        spark_max("heart_rate").alias(
            "max_hr"
        ),
        avg("spo2").alias(
            "avg_spo2"
        ),
        spark_min("spo2").alias(
            "min_spo2"
        ),
        spark_max("spo2").alias(
            "max_spo2"
        ),
        avg("systolic_bp").alias(
            "avg_systolic_bp"
        ),
        avg("diastolic_bp").alias(
            "avg_diastolic_bp"
        ),
        avg("temperature").alias(
            "avg_temperature"
        ),
        count("*").alias(
            "event_count"
        ),
        spark_sum("is_abnormal").alias(
            "abnormal_count"
        ),
    )
)


# ============================================================
# Risk Flag
# ============================================================

risk_stream = (
    windowed_stream
    .withColumn(
        "risk_flag",
        when(
            col("abnormal_count") >= 2,
            lit("elevated"),
        )
        .when(
            col("abnormal_count") == 1,
            lit("watch"),
        )
        .otherwise(
            lit("normal")
        ),
    )
)


# ============================================================
# PostgreSQL Sink
# ============================================================

def write_to_postgres(
    batch_df,
    batch_id,
):
    rows = batch_df.collect()

    if not rows:
        return

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

        for row in rows:

            patient_id = row["patient_id"]

            window_start = row[
                "window"
            ]["start"]

            window_end = row[
                "window"
            ]["end"]

            avg_hr = row[
                "avg_heart_rate"
            ]

            avg_spo2 = row[
                "avg_spo2"
            ]

            avg_temp = row[
                "avg_temperature"
            ]

            abnormal_count = row[
                "abnormal_count"
            ]

            risk_flag = row[
                "risk_flag"
            ]

            # ------------------------------------------------
            # Insert realtime trend
            # ------------------------------------------------

            cursor.execute(
                """
                INSERT INTO vitals_trends (
                    patient_id,
                    window_start,
                    window_end,
                    avg_hr,
                    avg_spo2,
                    avg_temp,
                    abnormal_count,
                    risk_flag_realtime
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (
                    patient_id,
                    window_start,
                    window_end
                )
                DO NOTHING
                """,
                (
                    patient_id,
                    window_start,
                    window_end,
                    avg_hr,
                    avg_spo2,
                    avg_temp,
                    abnormal_count,
                    risk_flag,
                ),
            )

            # ------------------------------------------------
            # Create alert for watch/elevated
            # ------------------------------------------------

            if risk_flag in (
                "watch",
                "elevated",
            ):

                cursor.execute(
                    """
                    INSERT INTO alerts (
                        patient_id,
                        rule_triggered,
                        severity,
                        triggered_at,
                        source_layer
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
                        patient_id,
                        f"{abnormal_count} abnormal vital events in 90-second window",
                        risk_flag,
                        window_end,
                        "speed",
                    ),
                )

        connection.commit()

        print(
            f"Batch {batch_id}: "
            f"persisted {len(rows)} realtime trend rows"
        )

    except Exception as error:

        if connection:
            connection.rollback()

        print(
            f"Batch {batch_id}: "
            f"PostgreSQL write failed: {error}"
        )

        raise

    finally:

        if connection:
            connection.close()


# ============================================================
# Write Realtime Aggregations to PostgreSQL
# ============================================================

postgres_query = (
    risk_stream
    .writeStream
    .outputMode("append")
    .foreachBatch(
        write_to_postgres
    )
    .option(
        "checkpointLocation",
        "spark/checkpoints/postgres-test",
    )
    .trigger(
        processingTime="5 seconds"
    )
    .start()
)


# ============================================================
# Wait for Streaming Queries
# ============================================================

spark.streams.awaitAnyTermination()