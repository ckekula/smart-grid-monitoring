from datetime import date

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import DailyHouseholdBilling
from api.schemas import (
    BillingHistoryResponse,
    BillingResponse,
    DailyBillingSummary,
)

router = APIRouter(
    prefix="/api/v1/billing",
    tags=["Billing"],
)


@router.get("/current", response_model=list[BillingResponse])
def get_current_billing(
    db: Session = Depends(get_db),
):
    latest_date = db.scalar(
        select(func.max(DailyHouseholdBilling.billing_date))
    )

    if latest_date is None:
        return []

    records = db.scalars(
        select(DailyHouseholdBilling)
        .where(
            DailyHouseholdBilling.billing_date == latest_date
        )
        .order_by(DailyHouseholdBilling.household_id)
    ).all()

    return records


@router.get(
    "/{household_id}",
    response_model=BillingResponse,
)
def get_household_billing(
    household_id: str,
    db: Session = Depends(get_db),
):
    latest_date = db.scalar(
        select(func.max(DailyHouseholdBilling.billing_date))
        .where(
            DailyHouseholdBilling.household_id == household_id
        )
    )

    if latest_date is None:
        raise HTTPException(
            status_code=404,
            detail=f"No billing data found for household {household_id}",
        )

    record = db.scalar(
        select(DailyHouseholdBilling)
        .where(
            DailyHouseholdBilling.household_id == household_id,
            DailyHouseholdBilling.billing_date == latest_date,
        )
    )

    if record is None:
        raise HTTPException(
            status_code=404,
            detail=f"No billing data found for household {household_id}",
        )

    return record


@router.get(
    "/{household_id}/history",
    response_model=BillingHistoryResponse,
)
def get_household_billing_history(
    household_id: str,
    db: Session = Depends(get_db),
):
    records = db.scalars(
        select(DailyHouseholdBilling)
        .where(
            DailyHouseholdBilling.household_id == household_id
        )
        .order_by(
            desc(DailyHouseholdBilling.billing_date)
        )
    ).all()

    if not records:
        raise HTTPException(
            status_code=404,
            detail=f"No billing data found for household {household_id}",
        )

    return {
        "household_id": household_id,
        "records": records,
    }


@router.get(
    "/daily/{billing_date}",
    response_model=DailyBillingSummary,
)
def get_daily_billing_summary(
    billing_date: date,
    db: Session = Depends(get_db),
):
    result = db.execute(
        select(
            DailyHouseholdBilling.billing_date,
            func.count(
                DailyHouseholdBilling.household_id
            ).label("household_count"),
            func.sum(
                DailyHouseholdBilling.total_consumption_kwh
            ).label("total_consumption_kwh"),
            func.sum(
                DailyHouseholdBilling.renewable_generation_kwh
            ).label("total_renewable_generation_kwh"),
            func.sum(
                DailyHouseholdBilling.grid_import_kwh
            ).label("total_grid_import_kwh"),
            func.sum(
                DailyHouseholdBilling.grid_export_kwh
            ).label("total_grid_export_kwh"),
            func.sum(
                DailyHouseholdBilling.energy_charge_usd
            ).label("total_energy_charge_usd"),
        )
        .where(
            DailyHouseholdBilling.billing_date == billing_date
        )
        .group_by(
            DailyHouseholdBilling.billing_date
        )
    ).first()

    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"No billing data found for {billing_date}",
        )

    return result