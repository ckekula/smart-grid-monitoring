from pyspark.sql import DataFrame

from src.config import (
    POSTGRES_DRIVER,
    POSTGRES_PASSWORD,
    POSTGRES_URL,
    POSTGRES_USER,
    ZONE_METRICS_TABLE,
)


def write_zone_metrics(
    batch_df: DataFrame,
    batch_id: int,
) -> None:
    """
    Persist one Spark micro-batch into PostgreSQL.

    A temporary staging table is used so the final write can be
    performed with PostgreSQL's INSERT ... ON CONFLICT DO UPDATE.

    This makes the sink idempotent when Spark retries a micro-batch.
    """

    if batch_df.isEmpty():
        return

    staging_table = f"smart_grid.zone_metrics_staging_{batch_id}"

    (
        batch_df
        .write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", staging_table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", POSTGRES_DRIVER)
        .option("batchsize", 100)
        .mode("overwrite")
        .save()
    )

    jvm = batch_df.sparkSession.sparkContext._jvm

    connection = None
    statement = None

    try:
        properties = jvm.java.util.Properties()
        properties.setProperty("user", POSTGRES_USER)
        properties.setProperty("password", POSTGRES_PASSWORD)

        connection = (jvm.java.sql.DriverManager.getConnection(POSTGRES_URL, properties))
        connection.setAutoCommit(False)
        statement = connection.createStatement()

        sql = f"""
        INSERT INTO {ZONE_METRICS_TABLE} (
            window_start,
            window_end,
            grid_zone,
            total_load_kw,
            total_consumption_kwh,
            solar_generation_kwh,
            wind_generation_kwh,
            renewable_generation_kwh,
            grid_import_kwh,
            grid_export_kwh,
            renewable_contribution_pct,
            meter_reading_count,
            meter_count,
            household_count,
            processed_at
        )
        SELECT
            window_start,
            window_end,
            grid_zone,
            total_load_kw,
            total_consumption_kwh,
            solar_generation_kwh,
            wind_generation_kwh,
            renewable_generation_kwh,
            grid_import_kwh,
            grid_export_kwh,
            renewable_contribution_pct,
            meter_reading_count,
            meter_count,
            household_count,
            CURRENT_TIMESTAMP
        FROM {staging_table}
        ON CONFLICT (window_start, grid_zone)
        DO UPDATE SET
            window_end = EXCLUDED.window_end,
            total_load_kw = EXCLUDED.total_load_kw,
            total_consumption_kwh = EXCLUDED.total_consumption_kwh,
            solar_generation_kwh = EXCLUDED.solar_generation_kwh,
            wind_generation_kwh = EXCLUDED.wind_generation_kwh,
            renewable_generation_kwh = EXCLUDED.renewable_generation_kwh,
            grid_import_kwh = EXCLUDED.grid_import_kwh,
            grid_export_kwh = EXCLUDED.grid_export_kwh,
            renewable_contribution_pct = EXCLUDED.renewable_contribution_pct,
            meter_reading_count = EXCLUDED.meter_reading_count,
            meter_count = EXCLUDED.meter_count,
            household_count = EXCLUDED.household_count,
            processed_at = CURRENT_TIMESTAMP;
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

    # Remove the staging table.
    cleanup_connection = None
    cleanup_statement = None

    try:
        properties = jvm.java.util.Properties()
        properties.setProperty("user", POSTGRES_USER)
        properties.setProperty("password", POSTGRES_PASSWORD)

        cleanup_connection = (
            jvm.java.sql.DriverManager
            .getConnection(POSTGRES_URL, properties)
        )
        cleanup_statement = cleanup_connection.createStatement()
        cleanup_statement.executeUpdate(f"DROP TABLE IF EXISTS {staging_table}")

    finally:
        if cleanup_statement is not None:
            cleanup_statement.close()
        if cleanup_connection is not None:
            cleanup_connection.close()