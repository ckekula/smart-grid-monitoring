from dataclasses import asdict, dataclass

import pandas as pd

INTERVAL_HOURS = 4


@dataclass
class SmartMeterEvent:
    event_id: str
    timestamp: str

    meter_id: str
    household_id: str
    grid_zone: str

    power_consumption_kw: float
    solar_power_kw: float
    wind_power_kw: float
    grid_supply_kw: float

    renewable_power_kw: float
    grid_import_kw: float
    grid_export_kw: float

    consumption_kwh: float
    solar_energy_kwh: float
    wind_energy_kwh: float
    renewable_energy_kwh: float
    grid_import_kwh: float
    grid_export_kwh: float

    tariff_rate_usd_kwh: float

    temperature_c: float
    humidity_percent: float

    overload_condition: bool
    transformer_fault: bool

    def to_dict(self) -> dict:
        return asdict(self)


def transform_row(row: pd.Series, event_id: str) -> SmartMeterEvent:
    """
    Convert one raw CSV row into a canonical smart-meter event.

    Source measurements are 15-minute average/interval power values
    expressed in kW. Energy values are calculated as: kWh = kW * 0.25
    """

    power_consumption_kw = float(row["Power Consumption (kW)"])
    solar_power_kw = float(row["Solar Power (kW)"])
    wind_power_kw = float(row["Wind Power (kW)"])
    grid_supply_kw = float(row["Grid Supply (kW)"])

    renewable_power_kw = solar_power_kw + wind_power_kw

    # Electricity required from the grid when renewable power is insufficient.
    grid_import_kw = max(power_consumption_kw - renewable_power_kw, 0.0)

    # Excess renewable generation exported back to the grid.
    grid_export_kw = max(renewable_power_kw - power_consumption_kw, 0.0)

    consumption_kwh = power_consumption_kw * INTERVAL_HOURS
    solar_energy_kwh = solar_power_kw * INTERVAL_HOURS
    wind_energy_kwh = wind_power_kw * INTERVAL_HOURS
    renewable_energy_kwh = renewable_power_kw * INTERVAL_HOURS
    grid_import_kwh = grid_import_kw * INTERVAL_HOURS
    grid_export_kwh = grid_export_kw * INTERVAL_HOURS

    timestamp = pd.Timestamp(row["Timestamp"]).to_pydatetime()

    return SmartMeterEvent(
        event_id=event_id,
        timestamp=timestamp.isoformat(),

        meter_id=str(row["Meter ID"]),
        household_id=str(row["Household ID"]),
        grid_zone=str(row["Grid Zone"]),

        power_consumption_kw=power_consumption_kw,
        solar_power_kw=solar_power_kw,
        wind_power_kw=wind_power_kw,
        grid_supply_kw=grid_supply_kw,

        renewable_power_kw=renewable_power_kw,
        grid_import_kw=grid_import_kw,
        grid_export_kw=grid_export_kw,

        consumption_kwh=consumption_kwh,
        solar_energy_kwh=solar_energy_kwh,
        wind_energy_kwh=wind_energy_kwh,
        renewable_energy_kwh=renewable_energy_kwh,
        grid_import_kwh=grid_import_kwh,
        grid_export_kwh=grid_export_kwh,

        tariff_rate_usd_kwh=float(row["Tariff Rate (USD/kWh)"]),

        temperature_c=float(row["Temperature (°C)"]),
        humidity_percent=float(row["Humidity (%)"]),

        overload_condition=bool(row["Overload Condition"]),
        transformer_fault=bool(row["Transformer Fault"]),
    )


def transform_dataset(df: pd.DataFrame) -> list[dict]:
    """
    Transform the complete dataset into canonical smart-meter events.
    """
    df = df.sort_values("Timestamp").reset_index(drop=True)

    events = []

    for index, row in df.iterrows():
        timestamp = pd.Timestamp(row["Timestamp"])

        event_id = (
            f"{timestamp.strftime('%Y%m%d%H%M')}"
            f"-{row['Meter ID']}"
        )

        event = transform_row(row=row, event_id=event_id)
        events.append(event.to_dict())

    return events
