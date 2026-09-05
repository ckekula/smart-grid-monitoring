from datetime import datetime

from pydantic import BaseModel, ConfigDict


class ZoneMetricResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    window_start: datetime
    window_end: datetime
    grid_zone: str

    total_load_kw: float
    total_consumption_kwh: float

    solar_generation_kwh: float
    wind_generation_kwh: float
    renewable_generation_kwh: float

    grid_import_kwh: float
    grid_export_kwh: float

    renewable_contribution_pct: float

    meter_reading_count: int
    meter_count: int
    household_count: int

    processed_at: datetime


class CurrentZoneResponse(BaseModel):
    grid_zone: str

    window_start: datetime
    window_end: datetime

    current_load_kw: float

    total_consumption_kwh: float
    solar_generation_kwh: float
    wind_generation_kwh: float
    renewable_generation_kwh: float

    grid_import_kwh: float
    grid_export_kwh: float

    renewable_contribution_pct: float

    meter_count: int
    household_count: int

    processed_at: datetime


class CurrentZonesResponse(BaseModel):
    zones: list[CurrentZoneResponse]