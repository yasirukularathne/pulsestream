from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, when
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "vitals.events"
KAFKA_DLQ_TOPIC = "vitals.dead-letter"


EVENT_SCHEMA = StructType([
    StructField("patient_id", StringType(), False),
    StructField("heart_rate", IntegerType(), True),
    StructField("spo2", DoubleType(), True),
    StructField("systolic_bp", IntegerType(), True),
    StructField("diastolic_bp", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("event_id", StringType(), False),
    StructField("timestamp", TimestampType(), True),
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("PulseStreamKafkaReader")
        .master("local[2]")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # ---------------------------------------------------------
        # 1. Read events from Kafka
        # ---------------------------------------------------------
        stream_df = (
            spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                KAFKA_BOOTSTRAP_SERVERS,
            )
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
        )

        # ---------------------------------------------------------
        # 2. Extract Kafka key, original JSON and Kafka timestamp
        # ---------------------------------------------------------
        raw_df = stream_df.selectExpr(
            "CAST(key AS STRING) AS patient_id",
            "CAST(value AS STRING) AS event_json",
            "timestamp AS kafka_timestamp",
        )

        # ---------------------------------------------------------
        # 3. Parse JSON according to event schema
        # ---------------------------------------------------------
        parsed_df = (
            raw_df
            .withColumn(
                "event",
                from_json(col("event_json"), EVENT_SCHEMA),
            )
            .select(
                "event.*",
                "event_json",
                "kafka_timestamp",
            )
        )

        # ---------------------------------------------------------
        # 4. Validate vital-sign data
        #
        # These are DATA-QUALITY validation ranges,
        # not clinical decision thresholds.
        # ---------------------------------------------------------
        validated_df = (
            parsed_df
            .withColumn(
                "validation_status",
                when(
                    (col("heart_rate") < 60) |
                    (col("heart_rate") > 180) |
                    (col("spo2") < 70) |
                    (col("spo2") > 100) |
                    (col("systolic_bp") < 70) |
                    (col("systolic_bp") > 250) |
                    (col("diastolic_bp") < 40) |
                    (col("diastolic_bp") > 150) |
                    (col("temperature") < 30) |
                    (col("temperature") > 45),
                    lit("invalid"),
                )
                .otherwise(lit("valid")),
            )
        )

        # ---------------------------------------------------------
        # 5. Select invalid events for Dead Letter Queue
        # ---------------------------------------------------------
        invalid_df = (
            validated_df
            .filter(col("validation_status") == "invalid")
            .select(
                col("patient_id").alias("key"),
                col("event_json").alias("value"),
            )
        )

        # ---------------------------------------------------------
        # 6. Write invalid events to Kafka DLQ
        # ---------------------------------------------------------
        dlq_query = (
            invalid_df
            .selectExpr(
                "CAST(key AS STRING) AS key",
                "CAST(value AS STRING) AS value",
            )
            .writeStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                KAFKA_BOOTSTRAP_SERVERS,
            )
            .option(
                "topic",
                KAFKA_DLQ_TOPIC,
            )
            .option(
                "checkpointLocation",
                "spark/checkpoints/dlq",
            )
            .outputMode("append")
            .start()
        )

        # ---------------------------------------------------------
        # 7. Temporary console output for validation testing
        #
        # This only shows TEST_INVALID so we can verify
        # the invalid classification.
        # ---------------------------------------------------------
        query = (
            validated_df
            .filter(
                col("patient_id") == "TEST_INVALID"
            )
            .writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .option("numRows", 10)
            .trigger(processingTime="5 seconds")
            .start()
        )

        # Keep Spark streaming alive
        dlq_query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()