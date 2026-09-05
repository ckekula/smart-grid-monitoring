import os

from pyspark.sql import DataFrame

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

TARGET_TABLE = (
    "smart_grid.daily_household_billing"
)


def write_daily_billing(
    batch_df: DataFrame,
    batch_id: int,
):
    if batch_df.isEmpty():
        return

    staging_table = (
        f"smart_grid.billing_staging_{batch_id}"
    )

    (
        batch_df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", staging_table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", POSTGRES_DRIVER)
        .option("batchsize", 500)
        .mode("overwrite")
        .save()
    )

    jvm = (
        batch_df
        .sparkSession
        .sparkContext
        ._jvm
    )

    connection = None
    statement = None

    try:
        properties = jvm.java.util.Properties()

        properties.setProperty(
            "user",
            POSTGRES_USER,
        )

        properties.setProperty(
            "password",
            POSTGRES_PASSWORD,
        )

        connection = (
            jvm.java.sql.DriverManager
            .getConnection(
                POSTGRES_URL,
                properties,
            )
        )

        connection.setAutoCommit(False)

        statement = connection.createStatement()

        sql = f"""
        INSERT INTO {TARGET_TABLE} (
            billing_date,
            household_id,
            grid_zone,
            total_consumption_kwh,
            solar_generation_kwh,
            wind_generation_kwh,
            renewable_generation_kwh,
            grid_import_kwh,
            grid_export_kwh,
            average_tariff_usd_kwh,
            energy_charge_usd,
            meter_count,
            reading_count,
            processed_at
        )
        SELECT
            billing_date,
            household_id,
            grid_zone,
            total_consumption_kwh,
            solar_generation_kwh,
            wind_generation_kwh,
            renewable_generation_kwh,
            grid_import_kwh,
            grid_export_kwh,
            average_tariff_usd_kwh,
            energy_charge_usd,
            meter_count,
            reading_count,
            CURRENT_TIMESTAMP
        FROM {staging_table}

        ON CONFLICT (
            billing_date,
            household_id
        )

        DO UPDATE SET
            grid_zone =
                EXCLUDED.grid_zone,

            total_consumption_kwh =
                EXCLUDED.total_consumption_kwh,

            solar_generation_kwh =
                EXCLUDED.solar_generation_kwh,

            wind_generation_kwh =
                EXCLUDED.wind_generation_kwh,

            renewable_generation_kwh =
                EXCLUDED.renewable_generation_kwh,

            grid_import_kwh =
                EXCLUDED.grid_import_kwh,

            grid_export_kwh =
                EXCLUDED.grid_export_kwh,

            average_tariff_usd_kwh =
                EXCLUDED.average_tariff_usd_kwh,

            energy_charge_usd =
                EXCLUDED.energy_charge_usd,

            meter_count =
                EXCLUDED.meter_count,

            reading_count =
                EXCLUDED.reading_count,

            processed_at =
                CURRENT_TIMESTAMP;
        """

        statement.executeUpdate(sql)

        connection.commit()

    except Exception:
        if connection is not None:
            connection.rollback()

        raise

    finally:
        if statement is not None:
            statement.close()

        if connection is not None:
            connection.close()

    # Clean up staging table.
    cleanup_connection = None
    cleanup_statement = None

    try:
        properties = jvm.java.util.Properties()

        properties.setProperty(
            "user",
            POSTGRES_USER,
        )

        properties.setProperty(
            "password",
            POSTGRES_PASSWORD,
        )

        cleanup_connection = (
            jvm.java.sql.DriverManager
            .getConnection(
                POSTGRES_URL,
                properties,
            )
        )

        cleanup_statement = (
            cleanup_connection
            .createStatement()
        )

        cleanup_statement.executeUpdate(
            f"""
            DROP TABLE IF EXISTS
            {staging_table}
            """
        )

    finally:
        if cleanup_statement is not None:
            cleanup_statement.close()

        if cleanup_connection is not None:
            cleanup_connection.close()