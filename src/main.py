from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import RealtimeZoneMetric
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

@app.get("/health")
def health(
    db: Session = Depends(get_db),
):
    try:
        db.execute(text("SELECT 1"))

        latest_processed = db.scalar(
            select(RealtimeZoneMetric.processed_at)
            .order_by(
                RealtimeZoneMetric.processed_at.desc()
            )
            .limit(1)
        )

        return {
            "status": "ok",
            "database": "connected",
            "latest_processed": latest_processed,
        }

    except Exception:
        raise HTTPException(
            status_code=503,
            detail={
                "status": "unhealthy",
                "database": "unavailable",
            },
        )
