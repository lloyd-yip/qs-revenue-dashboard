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

from db.queries.collections import get_collections_for_range
from db.queries.deal_matches import get_suggested_matches
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
    entered_pipeline_date: str | None = None
    sales_cycle_days: int | None = None
    whop_email: str | None = None
    ghl_contact_email: str | None = None
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
    projected_total: float = 0.0
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
    month: str = Query(..., pattern=r"^\d{4}-\d{2}$", description="Month (YYYY-MM); range start when 'end' given"),
    end: str | None = Query(None, pattern=r"^\d{4}-\d{2}$", description="Optional range end month (YYYY-MM)"),
    db: AsyncSession = Depends(get_db),
) -> WhopLiveResponse:
    """New-deals view: deals whose FIRST Whop payment landed in the month (or the
    month range when `end` is given), grouped by rep."""
    try:
        month_start, _ = _parse_month_range(month)
        _, month_end = _parse_month_range(end) if end else _parse_month_range(month)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid month range: {month}..{end}")
    if month_end < month_start:
        raise HTTPException(status_code=422, detail="Range end is before start")

    data = await get_whop_live_summary_for_month(db, month_start, month_end)

    reps = [
        WhopLiveRepRow(
            rep_name=r["rep_name"],
            deal_count=r["deal_count"],
            gross_contract_value=r["gross_contract_value"],
            projected_total=r.get("projected_total", 0.0),
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


@router.get("/pnl/collections")
async def pnl_collections(
    start: str = Query(..., pattern=r"^\d{4}-\d{2}$", description="Range start month (YYYY-MM)"),
    end: str | None = Query(None, pattern=r"^\d{4}-\d{2}$", description="Range end month (YYYY-MM); defaults to start"),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Projected collections across ALL payment plans for a month range — how much
    lands each month (collected vs still-outstanding), refunds, and the plan
    breakdown. Future cash is estimated (equal monthly installments); financed
    deals settle upfront. See db/queries/collections.py."""
    try:
        range_start, _ = _parse_month_range(start)
        _, range_end = _parse_month_range(end) if end else _parse_month_range(start)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid range: {start}..{end}")
    if range_end < range_start:
        raise HTTPException(status_code=422, detail="Range end is before start")
    return await get_collections_for_range(db, range_start, range_end)


@router.get("/pnl/suggested-matches")
async def suggested_matches(db: AsyncSession = Depends(get_db)) -> dict:
    """Non-exact-email matches that carry a payment — for the Confirm Matches UI.
    Also returns the Whop business id so the frontend can deep-link the payer."""
    matches = await get_suggested_matches(db)
    return {"matches": matches, "whop_biz": "biz_I0rQ5yItozATsc"}


class ReviewDealInput(BaseModel):
    ghl_opportunity_id: str
    action: str  # 'confirm' | 'ignore' | 'reset' | 'unmatch'


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

    if body.action not in ("confirm", "ignore", "reset", "unmatch"):
        raise HTTPException(status_code=422, detail=f"Invalid action: {body.action}")

    row = (await db.execute(
        sa_select(DealWhopMatch).where(DealWhopMatch.ghl_opportunity_id == body.ghl_opportunity_id)
    )).scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail=f"Deal {body.ghl_opportunity_id} not found")

    if body.action == "confirm":
        row.is_confirmed = True
        row.is_ignored = False
        row.confirmed_by = "manual-confirm"
        row.confirmed_at = datetime.now(timezone.utc)
    elif body.action == "ignore":
        row.is_ignored = True
        row.is_confirmed = False
        row.confirmed_by = None
        row.confirmed_at = None
    elif body.action == "unmatch":
        # Reject a wrong attribution — clear the payment link entirely so it stops
        # counting (the payment was not this deal's). Same as the manual Paul fix.
        row.is_confirmed = False
        row.confirmed_by = None
        row.confirmed_at = None
        row.match_confidence = "unmatched"
        row.match_score = 0
        row.match_method = "manual_link_removed"
        for col in ("whop_membership_id", "whop_email", "whop_name", "whop_product_id",
                    "whop_plan_name", "whop_created_at", "total_paid", "upfront_cash",
                    "net_cash_collected", "total_refunded", "remaining_ar", "payment_count",
                    "total_installments", "first_payment_date", "total_contract_value",
                    "provider_fee_pct", "is_splitit", "is_claritypay", "plan_months_flag",
                    "is_financing"):
            setattr(row, col, None)
        row.metrics_updated_at = datetime.now(timezone.utc)
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
