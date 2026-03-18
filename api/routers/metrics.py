"""All /api/metrics/* endpoints."""

from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas.responses import (
    ByRepResponse,
    LeadSourceResponse,
    MetaMixin,
    QualificationResponse,
    SummaryResponse,
)
from db.queries.lead_source import get_lead_source_breakdown, get_qualification_breakdown
from db.queries.metrics_by_rep import get_by_rep
from db.queries.metrics_summary import get_summary
from db.session import get_db

router = APIRouter(prefix="/api/metrics", tags=["metrics"])


def _meta(start: date, end: date, date_by: str) -> MetaMixin:
    return MetaMixin(
        date_start=start,
        date_end=end,
        date_by=date_by,
        generated_at=datetime.now(timezone.utc),
    )


def _date_params(
    start: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end: date = Query(..., description="End date (YYYY-MM-DD)"),
    date_by: str = Query("appointment", description="Date dimension: 'appointment' or 'created'"),
) -> tuple[date, date, str]:
    if date_by not in ("appointment", "created"):
        date_by = "appointment"
    return start, end, date_by


@router.get("/summary", response_model=SummaryResponse)
async def summary(
    rep_id: str | None = Query(None, description="Filter to a single rep by GHL owner ID"),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Team-level KPIs: show rates, qual rate, close rate, units closed, projected value."""
    start, end, date_by = params
    data = await get_summary(db, start, end, date_by, rep_id)
    return SummaryResponse(data=data, meta=_meta(start, end, date_by))


@router.get("/by-rep", response_model=ByRepResponse)
async def by_rep(
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """All KPIs broken down per rep — all reps returned in one call."""
    start, end, date_by = params
    data = await get_by_rep(db, start, end, date_by)
    return ByRepResponse(data=data, meta=_meta(start, end, date_by))


@router.get("/lead-source", response_model=LeadSourceResponse)
async def lead_source(
    rep_id: str | None = Query(None),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Attribution breakdown by canonical channel."""
    start, end, date_by = params
    data = await get_lead_source_breakdown(db, start, end, date_by, rep_id)
    return LeadSourceResponse(data=data, meta=_meta(start, end, date_by))


@router.get("/qualification", response_model=QualificationResponse)
async def qualification(
    rep_id: str | None = Query(None),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Lead Quality field distribution + all custom field breakdowns (1st call shows only)."""
    start, end, date_by = params
    data = await get_qualification_breakdown(db, start, end, date_by, rep_id)
    return QualificationResponse(data=data, meta=_meta(start, end, date_by))
