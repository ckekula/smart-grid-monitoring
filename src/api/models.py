from datetime import datetime

from sqlalchemy import (
    DateTime,
    Float,
    Integer,
    String,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class RealtimeZoneMetric(Base):
    __tablename__ = "realtime_zone_metrics"
    __table_args__ = {
        "schema": "smart_grid",
    }

    window_start: Mapped[datetime] = mapped_column(
        DateTime,
        primary_key=True,
    )

    window_end: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
    )

    grid_zone: Mapped[str] = mapped_column(
        String(20),
        primary_key=True,
    )

    total_load_kw: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    total_consumption_kwh: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    solar_generation_kwh: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    wind_generation_kwh: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    renewable_generation_kwh: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    grid_import_kwh: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    grid_export_kwh: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    renewable_contribution_pct: Mapped[float] = mapped_column(
        Float,
        nullable=False,
    )

    meter_reading_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    meter_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    household_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    processed_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
    )