from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import RealtimeZoneMetric
from api.schemas import (
    CurrentZoneResponse,
    CurrentZonesResponse,
    ZoneMetricResponse,
)

router = APIRouter(
    prefix="/api/v1/zones",
    tags=["Grid Zones"],
)


@router.get(
    "/current",
    response_model=CurrentZonesResponse,
)
def get_current_zones(
    db: Session = Depends(get_db),
):
    """
    Return the latest completed 15-minute metrics for every grid zone.
    """

    # Get the latest window.
    latest_window = db.scalar(
        select(RealtimeZoneMetric.window_end)
        .order_by(
            desc(RealtimeZoneMetric.window_end)
        )
        .limit(1)
    )

    if latest_window is None:
        return CurrentZonesResponse(zones=[])

    # Get all zones belonging to that latest window.
    metrics = db.scalars(
        select(RealtimeZoneMetric)
        .where(
            RealtimeZoneMetric.window_end == latest_window
        )
        .order_by(
            RealtimeZoneMetric.grid_zone
        )
    ).all()

    return CurrentZonesResponse(
        zones=[
            CurrentZoneResponse(
                grid_zone=metric.grid_zone,
                window_start=metric.window_start,
                window_end=metric.window_end,
                current_load_kw=metric.total_load_kw,
                total_consumption_kwh=metric.total_consumption_kwh,
                solar_generation_kwh=metric.solar_generation_kwh,
                wind_generation_kwh=metric.wind_generation_kwh,
                renewable_generation_kwh=metric.renewable_generation_kwh,
                grid_import_kwh=metric.grid_import_kwh,
                grid_export_kwh=metric.grid_export_kwh,
                renewable_contribution_pct=metric.renewable_contribution_pct,
                meter_count=metric.meter_count,
                household_count=metric.household_count,
                processed_at=metric.processed_at,
            )
            for metric in metrics
        ]
    )


@router.get(
    "/{zone}/current",
    response_model=CurrentZoneResponse,
)
def get_current_zone(
    zone: str,
    db: Session = Depends(get_db),
):
    """
    Return the latest completed 15-minute metrics for one grid zone.
    """

    zone = zone.upper()

    metric = db.scalar(
        select(RealtimeZoneMetric)
        .where(
            RealtimeZoneMetric.grid_zone == zone
        )
        .order_by(
            desc(RealtimeZoneMetric.window_end)
        )
        .limit(1)
    )

    if metric is None:
        raise HTTPException(
            status_code=404,
            detail=f"No metrics found for grid zone '{zone}'",
        )

    return CurrentZoneResponse(
        grid_zone=metric.grid_zone,
        window_start=metric.window_start,
        window_end=metric.window_end,
        current_load_kw=metric.total_load_kw,
        total_consumption_kwh=metric.total_consumption_kwh,
        solar_generation_kwh=metric.solar_generation_kwh,
        wind_generation_kwh=metric.wind_generation_kwh,
        renewable_generation_kwh=metric.renewable_generation_kwh,
        grid_import_kwh=metric.grid_import_kwh,
        grid_export_kwh=metric.grid_export_kwh,
        renewable_contribution_pct=metric.renewable_contribution_pct,
        meter_count=metric.meter_count,
        household_count=metric.household_count,
        processed_at=metric.processed_at,
    )


@router.get(
    "/{zone}/history",
    response_model=list[ZoneMetricResponse],
)
def get_zone_history(
    zone: str,
    limit: int = Query(
        default=96,
        ge=1,
        le=2496,
        description="Number of 15-minute windows to return",
    ),
    start: datetime | None = Query(
        default=None,
        description="Start of requested time range",
    ),
    end: datetime | None = Query(
        default=None,
        description="End of requested time range",
    ),
    db: Session = Depends(get_db),
):
    """
    Return historical 15-minute metrics for a grid zone.
    """

    zone = zone.upper()

    query = (
        select(RealtimeZoneMetric)
        .where(
            RealtimeZoneMetric.grid_zone == zone
        )
    )

    if start is not None:
        query = query.where(
            RealtimeZoneMetric.window_start >= start
        )

    if end is not None:
        query = query.where(
            RealtimeZoneMetric.window_end <= end
        )

    query = (
        query
        .order_by(
            desc(RealtimeZoneMetric.window_start)
        )
        .limit(limit)
    )

    metrics = db.scalars(query).all()

    return list(metrics)