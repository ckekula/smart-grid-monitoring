from datetime import UTC, datetime

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import RealtimeZoneMetric

router = APIRouter(
    prefix="/api/v1/alerts",
    tags=["Alerts"],
)


RENEWABLE_THRESHOLD_PERCENT = 20.0


@router.get("/renewable")
def renewable_alerts(
    db: Session = Depends(get_db),
):
    """
    Check the latest renewable contribution for each grid zone.

    An alert is raised when renewable contribution falls below
    RENEWABLE_THRESHOLD_PERCENT.
    """

    latest_metrics = []

    zones = db.scalars(
        select(RealtimeZoneMetric.grid_zone).distinct()
    ).all()

    for zone in zones:
        metric = db.scalar(
            select(RealtimeZoneMetric)
            .where(
                RealtimeZoneMetric.grid_zone == zone
            )
            .order_by(
                RealtimeZoneMetric.window_start.desc()
            )
            .limit(1)
        )

        if metric is not None:
            latest_metrics.append(metric)

    alerts = []

    for metric in latest_metrics:
        renewable_pct = metric.renewable_contribution_pct

        if renewable_pct < RENEWABLE_THRESHOLD_PERCENT:
            alerts.append(
                {
                    "grid_zone": metric.grid_zone,
                    "alert_type": "LOW_RENEWABLE_CONTRIBUTION",
                    "severity": "WARNING",
                    "message": (
                        f"Renewable contribution for zone "
                        f"{metric.grid_zone} is "
                        f"{renewable_pct:.2f}%, below the "
                        f"{RENEWABLE_THRESHOLD_PERCENT:.2f}% threshold."
                    ),
                    "renewable_contribution_pct": renewable_pct,
                    "threshold_pct": RENEWABLE_THRESHOLD_PERCENT,
                    "window_start": metric.window_start,
                    "window_end": metric.window_end,
                    "detected_at": datetime.now(UTC),
                }
            )

    return {
        "status": "alert" if alerts else "ok",
        "threshold_pct": RENEWABLE_THRESHOLD_PERCENT,
        "alert_count": len(alerts),
        "alerts": alerts,
    }
