from datetime import UTC, datetime, timedelta

from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import DailyHouseholdBilling, RealtimeZoneMetric
from api.routes.alerts import router as alerts_router
from api.routes.billing import router as billing_router
from api.routes.zones import router as zones_router

app = FastAPI(
    title="Smart Grid Energy Monitoring API",
    description=(
        "Real-time grid load and renewable contribution monitoring."
    ),
    version="1.0.0",
)


app.include_router(zones_router)
app.include_router(billing_router)
app.include_router(alerts_router)

@app.get("/health")
def health(
    db: Session = Depends(get_db),
):
    try:
        db.execute(text("SELECT 1"))

        # Latest streaming record
        latest_processed = db.scalar(
            select(RealtimeZoneMetric.processed_at)
            .order_by(
                RealtimeZoneMetric.processed_at.desc()
            )
            .limit(1)
        )

        # Latest batch billing date
        latest_billing_date = db.scalar(
            select(
                func.max(
                    DailyHouseholdBilling.billing_date
                )
            )
        )

        # Streaming freshness
        streaming_healthy = False

        if latest_processed is not None:
            now = datetime.now(UTC)

            if latest_processed.tzinfo is None:
                latest_processed = latest_processed.replace(
                    tzinfo=UTC
                )

            streaming_healthy = (
                now - latest_processed
                <= timedelta(minutes=5)
            )

        # Overall status
        overall_healthy = (
            streaming_healthy
            and latest_billing_date is not None
        )

        return {
            "status": "ok" if overall_healthy else "degraded",
            "database": "connected",
            "streaming_pipeline": (
                "healthy"
                if streaming_healthy
                else "stale"
            ),
            "batch_pipeline": (
                "healthy"
                if latest_billing_date is not None
                else "no_data"
            ),
            "latest_processed": latest_processed,
            "latest_billing_date": latest_billing_date,
        }

    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unhealthy",
                "database": "unavailable",
                "error": str(exc),
            },
        )
