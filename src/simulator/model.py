import json
from dataclasses import asdict, dataclass


@dataclass
class MeterReading:
    event_id: str
    meter_id: str
    household_id: str
    grid_zone: str

    timestamp: str

    power_consumption_kw: float
    solar_generation_kw: float
    wind_generation_kw: float
    grid_supply_kw: float

    voltage_v: float
    current_a: float
    reactive_power_kvar: float
    power_factor: float
    voltage_fluctuation_pct: float

    overload_condition: bool
    transformer_fault: bool

    temperature_c: float
    humidity_pct: float

    electricity_price_usd_kwh: float
    predicted_load_kw: float

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> bytes:
        return json.dumps(
            self.to_dict(),
            separators=(",", ":"),
        ).encode("utf-8")
