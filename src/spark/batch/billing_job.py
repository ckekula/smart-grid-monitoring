import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.batch.batch_sink import write_daily_billing

BATCH_FILE = os.environ["BATCH_FILE"]

POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "jdbc:postgresql://postgres:5432/smart_grid",
)

POSTGRES_USER = os.getenv(
    "POSTGRES_USER",
    "airflow",
)

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "airflow",
)

POSTGRES_DRIVER = "org.postgresql.Driver"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SmartGridDailyBilling")
        .getOrCreate()
    )


def read_daily_data(spark):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(BATCH_FILE)
    )


def transform_daily_billing(df):
    df = (
        df
        .withColumn(
            "event_time",
            F.to_timestamp("Timestamp"),
        )
        .withColumn(
            "billing_date",
            F.to_date("event_time"),
        )
        .withColumn(
            "consumption_kwh",
            F.col("Power Consumption (kW)") * F.lit(0.25),
        )
        .withColumn(
            "solar_generation_kwh",
            F.col("Solar Power (kW)") * F.lit(0.25),
        )
        .withColumn(
            "wind_generation_kwh",
            F.col("Wind Power (kW)") * F.lit(0.25),
        )
    )

    df = (
        df
        .withColumn(
            "renewable_power_kw",
            F.col("Solar Power (kW)")
            + F.col("Wind Power (kW)"),
        )
        .withColumn(
            "grid_import_kw",
            F.greatest(
                F.col("Power Consumption (kW)")
                - F.col("renewable_power_kw"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "grid_export_kw",
            F.greatest(
                F.col("renewable_power_kw")
                - F.col("Power Consumption (kW)"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "grid_import_kwh",
            F.col("grid_import_kw") * F.lit(0.25),
        )
        .withColumn(
            "grid_export_kwh",
            F.col("grid_export_kw") * F.lit(0.25),
        )
        .withColumn(
            "renewable_generation_kwh",
            F.col("solar_generation_kwh")
            + F.col("wind_generation_kwh"),
        )
    )

    daily = (
        df
        .groupBy(
            "billing_date",
            "Household ID",
            "Grid Zone",
        )
        .agg(
            F.sum("consumption_kwh")
            .alias("total_consumption_kwh"),

            F.sum("solar_generation_kwh")
            .alias("solar_generation_kwh"),

            F.sum("wind_generation_kwh")
            .alias("wind_generation_kwh"),

            F.sum("renewable_generation_kwh")
            .alias("renewable_generation_kwh"),

            F.sum("grid_import_kwh")
            .alias("grid_import_kwh"),

            F.sum("grid_export_kwh")
            .alias("grid_export_kwh"),

            F.avg("Tariff Rate (USD/kWh)")
            .alias("average_tariff_usd_kwh"),

            F.count("*")
            .alias("reading_count"),

            F.countDistinct("Meter ID")
            .alias("meter_count"),
        )
    )

    daily = daily.withColumn(
        "energy_charge_usd",
        F.col("grid_import_kwh")
        * F.col("average_tariff_usd_kwh"),
    )

    return daily.select(
        F.col("billing_date"),

        F.col("Household ID")
        .alias("household_id"),

        F.col("Grid Zone")
        .alias("grid_zone"),

        "total_consumption_kwh",
        "solar_generation_kwh",
        "wind_generation_kwh",
        "renewable_generation_kwh",
        "grid_import_kwh",
        "grid_export_kwh",
        "average_tariff_usd_kwh",
        "energy_charge_usd",
        "meter_count",
        "reading_count",
    )


def write_to_postgres(df):
    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", "smart_grid.daily_household_billing")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", POSTGRES_DRIVER)
        .option("batchsize", 500)
        .mode("append")
        .save()
    )


def main():
    spark = create_spark_session()

    try:
        source_df = read_daily_data(spark)
        billing_df = transform_daily_billing(source_df)
        print(f"Processing batch file: {BATCH_FILE}")

        billing_df.show(20, truncate=False)
        write_daily_billing(billing_df, 0)
        print("Daily billing batch completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
