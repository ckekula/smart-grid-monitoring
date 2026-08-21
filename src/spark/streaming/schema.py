from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

SMART_METER_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("timestamp", StringType(), False),

        StructField("meter_id", StringType(), False),
        StructField("household_id", StringType(), False),
        StructField("grid_zone", StringType(), False),

        StructField("power_consumption_kw", DoubleType(), False),
        StructField("solar_power_kw", DoubleType(), False),
        StructField("wind_power_kw", DoubleType(), False),
        StructField("grid_supply_kw", DoubleType(), False),

        StructField("renewable_power_kw", DoubleType(), False),
        StructField("grid_import_kw", DoubleType(), False),
        StructField("grid_export_kw", DoubleType(), False),

        StructField("consumption_kwh", DoubleType(), False),
        StructField("solar_energy_kwh", DoubleType(), False),
        StructField("wind_energy_kwh", DoubleType(), False),
        StructField("renewable_energy_kwh", DoubleType(), False),
        StructField("grid_import_kwh", DoubleType(), False),
        StructField("grid_export_kwh", DoubleType(), False),

        StructField("tariff_rate_usd_kwh", DoubleType(), False),

        StructField("temperature_c", DoubleType(), False),
        StructField("humidity_percent", DoubleType(), False),

        StructField("overload_condition", BooleanType(), False),
        StructField("transformer_fault", BooleanType(), False),
    ]
)