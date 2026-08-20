import uuid
from datetime import datetime, timedelta

import pandas as pd

from .model import MeterReading


class MeterSimulator:
    def __init__(
        self,
        dataframe: pd.DataFrame,
        num_meters: int = 100,
        num_zones: int = 3,
        simulated_minute_seconds: float = 0.083333,
    ):
        self.dataframe = dataframe

        self.num_meters = num_meters
        self.num_zones = num_zones

        self.simulated_minute_seconds = (simulated_minute_seconds)

        self.current_index = 0

        self.source_start_time = (dataframe.iloc[0]["Timestamp"])
        self.real_start_time = datetime.now('UTC') # Real clock begins when the simulator starts.

        self.meter_ids = [f"M{i:04d}" for i in range(1, num_meters + 1)]
        self.household_ids = [f"H{i:04d}" for i in range(1, num_meters + 1)]
        self.zones = [f"ZONE_{chr(65 + i)}" for i in range(num_zones)]

    def _get_meter(self, index: int) -> str:
        return self.meter_ids[index % self.num_meters]

    def _get_household(self, index: int) -> str:
        return self.household_ids[index % self.num_meters]

    def _get_zone(self, index: int) -> str:
        return self.zones[index % self.num_zones]

    def _calculate_simulated_timestamp(
        self,
        source_timestamp: pd.Timestamp,
    ) -> datetime:

        source_time = source_timestamp.to_pydatetime() # Extract time-of-day from source data
        minutes_since_midnight = (
            source_time.hour * 60
            + source_time.minute
            + source_time.second / 60
        )
        simulated_elapsed_seconds = (minutes_since_midnight * self.simulated_minute_seconds)

        return (self.real_start_time + timedelta(seconds=simulated_elapsed_seconds))

    def _build_event(
        self,
        row: pd.Series,
        index: int,
    ) -> MeterReading:

        source_timestamp = row["Timestamp"]

        simulated_timestamp = (
            self._calculate_simulated_timestamp(
                source_timestamp
            )
        )

        return MeterReading(
            event_id=str(uuid.uuid4()),
            meter_id=self._get_meter(index),
            household_id=self._get_household(index),
            grid_zone=self._get_zone(index),
            timestamp=simulated_timestamp.isoformat(),

            power_consumption_kw=float(row["Power Consumption (kW)"]),
            solar_generation_kw=float(row["Solar Power (kW)"]),
            wind_generation_kw=float(row["Wind Power (kW)"]),
            grid_supply_kw=float(row["Grid Supply (kW)"]),
            voltage_v=float(row["Voltage (V)"]),
            current_a=float(row["Current (A)"]),
            reactive_power_kvar=float(row["Reactive Power (kVAR)"]),
            power_factor=float(row["Power Factor"]),
            voltage_fluctuation_pct=float(row["Voltage Fluctuation (%)"]),

            overload_condition=bool(row["Overload Condition"]),
            transformer_fault=bool(row["Transformer Fault"]),

            temperature_c=float(row["Temperature (°C)"]),
            humidity_pct=float(row["Humidity (%)"]),
            electricity_price_usd_kwh=float(row["Electricity Price (USD/kWh)"]),
            predicted_load_kw=float(row["Predicted Load (kW)"]),
        )

    def next_event(self) -> MeterReading:
        row = self.dataframe.iloc[self.current_index]
        event = self._build_event(row, self.current_index)
        self.current_index = (self.current_index + 1) % len(self.dataframe)

        return event