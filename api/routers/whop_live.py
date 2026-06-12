"""Live Whop Revenue endpoint — per-rep real-time cash + contract value for the P&L page.

Browser-facing and unauthenticated, matching the dashboard router convention (these
serve static/pnl.html directly). The underlying data is refreshed nightly by the
daily_whop_refresh cron (sync/whop_refresh.py).
"""

import calendar
import logging
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from db.queries.whop_live import get_available_deal_months, get_whop_live_summary_for_month
from db.session import get_db
from sync.whop_refresh import refresh_current_month_payment_metrics

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/dashboard", tags=["whop-live"])


def _parse_month_range(month: str) -> tuple[date, date]:
    """Parse a 'YYYY-MM' string into (first_day, last_day) dates. Raises ValueError if invalid."""
    year_s, mon_s = month.split("-")
    year, mon = int(year_s), int(mon_s)
    if not (1 <= mon <= 12):
        raise ValueError(f"month out of range: {month}")
    last_day = calendar.monthrange(year, mon)[1]
    return date(year, mon, 1), date(year, mon, last_day)


class WhopLiveDealItem(BaseModel):
    """One deal row inside a rep's collapsible table."""
    ghl_opportunity_id: str
    ghl_opportunity_name: str | None
    ghl_close_date: str | None
    first_payment_date: str | None
    gross_contract_value: float | None
    total_paid: float | None
    net_cash_collected: float | None
    provider_fee_pct: float | None
    is_splitit: bool | None
    is_claritypay: bool | None
    plan_months_flag: bool | None
    match_confidence: str
    total_installments: int | None


class WhopLiveRepRow(BaseModel):
    """Aggregated row for one rep in the Live Whop Revenue section."""
    rep_name: str
    deal_count: int
    gross_contract_value: float
    net_cash_collected: float
    flagged_count: int
    deals: list[WhopLiveDealItem]


class WhopLiveResponse(BaseModel):
    """Response shape for GET /pnl/whop-live."""
    month: str
    reps: list[WhopLiveRepRow]
    totals: dict
    last_refreshed: str | None


@router.get("/pnl/whop-live", response_model=WhopLiveResponse)
async def pnl_whop_live(
    month: str = Query(..., pattern=r"^\d{4}-\d{2}$", description="Month in YYYY-MM format"),
    db: AsyncSession = Depends(get_db),
) -> WhopLiveResponse:
    """Return real-time Whop revenue grouped by rep for a calendar month."""
    try:
        month_start, month_end = _parse_month_range(month)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid month: {month}")

    data = await get_whop_live_summary_for_month(db, month_start, month_end)

    reps = [
        WhopLiveRepRow(
            rep_name=r["rep_name"],
            deal_count=r["deal_count"],
            gross_contract_value=r["gross_contract_value"],
            net_cash_collected=r["net_cash_collected"],
            flagged_count=r["flagged_count"],
            deals=[WhopLiveDealItem(**d) for d in r["deals"]],
        )
        for r in data["reps"]
    ]
    return WhopLiveResponse(
        month=month,
        reps=reps,
        totals=data["totals"],
        last_refreshed=data["last_refreshed"],
    )


@router.get("/pnl/whop-live/months")
async def whop_live_months(db: AsyncSession = Depends(get_db)) -> list[str]:
    """Return the deal-months ('YYYY-MM') that drive the Live lens month picker — current month included."""
    return await get_available_deal_months(db)


@router.post("/pnl/whop-refresh")
async def whop_refresh() -> dict:
    """Manually run the current-month Whop payment refresh — the same job the EOD cron runs.

    Idempotent: rows refreshed within the last 6h are skipped, so re-running is safe.
    Returns {ok, stats:{refreshed, skipped, errors, flagged}}.
    """
    try:
        stats = await refresh_current_month_payment_metrics()
        return {"ok": True, "stats": stats}
    except Exception as exc:
        logger.error(f"Whop refresh failed: {exc}", exc_info=True)
        return {"ok": False, "error": str(exc), "stats": {}}
