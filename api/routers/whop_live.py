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
    whop_email: str | None = None
    gross_contract_value: float | None
    upfront_cash: float | None = None
    total_paid: float | None
    whop_projected: float | None = None
    net_cash_collected: float | None
    remaining_ar: float | None = None
    provider_fee_pct: float | None
    payment_count: int | None = None
    is_splitit: bool | None
    is_claritypay: bool | None
    plan_months_flag: bool | None
    match_confidence: str
    total_installments: int | None
    needs_review: bool = False
    is_confirmed: bool = False


class WhopLiveRepRow(BaseModel):
    """Aggregated row for one rep in the Live Whop Revenue section."""
    rep_name: str
    deal_count: int
    gross_contract_value: float
    net_cash_collected: float
    flagged_count: int
    pending_count: int = 0
    pending_contract_value: float = 0.0
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
            pending_count=r.get("pending_count", 0),
            pending_contract_value=r.get("pending_contract_value", 0.0),
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


class ReviewDealInput(BaseModel):
    ghl_opportunity_id: str
    action: str  # 'confirm' | 'ignore' | 'reset'


@router.post("/pnl/whop-live/review-deal")
async def review_live_deal(body: ReviewDealInput, db: AsyncSession = Depends(get_db)) -> dict:
    """Resolve a needs-review, no-Whop deal in the Live lens (wire transfer / other).

    Actions:
      • confirm — the cash landed outside Whop → count it (is_confirmed=True).
      • ignore  — not a real/settling deal → hide it from the lens (is_ignored=True).
      • reset   — back to pending review (clears both flags).

    The matcher already leaves is_confirmed rows untouched; ignored rows are simply
    filtered out of the Live query.
    """
    from datetime import datetime, timezone

    from db.models import DealWhopMatch
    from sqlalchemy import select as sa_select

    if body.action not in ("confirm", "ignore", "reset"):
        raise HTTPException(status_code=422, detail=f"Invalid action: {body.action}")

    row = (await db.execute(
        sa_select(DealWhopMatch).where(DealWhopMatch.ghl_opportunity_id == body.ghl_opportunity_id)
    )).scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail=f"Deal {body.ghl_opportunity_id} not found")

    if body.action == "confirm":
        row.is_confirmed = True
        row.is_ignored = False
        row.confirmed_by = "manual-wire-confirm"
        row.confirmed_at = datetime.now(timezone.utc)
    elif body.action == "ignore":
        row.is_ignored = True
        row.is_confirmed = False
        row.confirmed_by = None
        row.confirmed_at = None
    else:  # reset
        row.is_confirmed = False
        row.is_ignored = False
        row.confirmed_by = None
        row.confirmed_at = None

    await db.commit()
    return {
        "ok": True,
        "ghl_opportunity_id": body.ghl_opportunity_id,
        "is_confirmed": row.is_confirmed,
        "is_ignored": row.is_ignored,
    }


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
