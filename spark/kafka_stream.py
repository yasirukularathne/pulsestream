from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
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

        raw_df = stream_df.selectExpr(
            "CAST(key AS STRING) AS patient_id",
            "CAST(value AS STRING) AS event_json",
            "timestamp AS kafka_timestamp",
        )

        parsed_df = (
            raw_df
            .withColumn(
                "event",
                from_json(col("event_json"), EVENT_SCHEMA),
            )
            .select(
                "event.*",
                "kafka_timestamp",
            )
        )

        query = (
            parsed_df.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .option("numRows", 10)
            .trigger(processingTime="5 seconds")
            .start()
        )

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()