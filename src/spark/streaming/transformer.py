from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def parse_kafka_events(
    kafka_df: DataFrame,
    event_schema,
) -> DataFrame:
    """
    Deserialize Kafka JSON values into typed Spark columns.
    """

    return (
        kafka_df.select(
            F.from_json(
                F.col("value").cast("string"),
                event_schema,
            ).alias("event")
        )
        .select("event.*")
        .withColumn(
            "event_time",
            F.to_timestamp("timestamp"),
        )
    )


def create_zone_metrics(
    events_df: DataFrame,
) -> DataFrame:
    """
    Aggregate smart-meter events into 15-minute event-time windows per grid zone.
    """

    return (
        events_df.withWatermark(
            "event_time",
            "30 minutes",
        )
        .groupBy(
            F.window(
                "event_time",
                "15 minutes",
            ),
            F.col("grid_zone"),
        )
        .agg(
            F.sum("power_consumption_kw").alias("total_load_kw"), # current load represented by the latest 15-minute interval in the window.
            F.sum("consumption_kwh").alias("total_consumption_kwh"), # Energy consumed during the 15-minute interval.
            F.sum("solar_energy_kwh").alias("solar_generation_kwh"),
            F.sum("wind_energy_kwh").alias("wind_generation_kwh"),
            F.sum("renewable_energy_kwh").alias("renewable_generation_kwh"),
            F.sum("grid_import_kwh").alias("grid_import_kwh"),
            F.sum("grid_export_kwh").alias("grid_export_kwh"),
            F.count("*").alias("meter_reading_count"),
            F.approx_count_distinct("meter_id").alias("meter_count"),
            F.approx_count_distinct("household_id").alias("household_count"),
        )
        .withColumn(
            "renewable_contribution_pct",
            F.when(
                F.col("total_consumption_kwh") > 0,
                (
                    F.col("renewable_generation_kwh")
                    / F.col("total_consumption_kwh")
                    * 100
                ),
            ).otherwise(F.lit(0.0)),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "grid_zone",
            "total_load_kw",
            "total_consumption_kwh",
            "solar_generation_kwh",
            "wind_generation_kwh",
            "renewable_generation_kwh",
            "grid_import_kwh",
            "grid_export_kwh",
            "renewable_contribution_pct",
            "meter_reading_count",
            "meter_count",
            "household_count",
        )
    )
