from pyspark.sql import SparkSession

from src.config import CHECKPOINT_LOCATION, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from src.spark.streaming.schema import SMART_METER_SCHEMA
from src.spark.streaming.sink import write_zone_metrics
from src.spark.streaming.transformer import create_zone_metrics, parse_kafka_events


def create_spark_session() -> SparkSession:
    return SparkSession.builder.appName("SmartGridZoneMetrics").getOrCreate()


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Spark Structured Streaming consumer")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    events_df = parse_kafka_events(kafka_df, SMART_METER_SCHEMA)
    zone_metrics_df = create_zone_metrics(events_df)

    query = (
        zone_metrics_df
        .writeStream
        .outputMode("update")
        .foreachBatch(write_zone_metrics)
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
