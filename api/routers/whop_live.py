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


# Range bounds accept a whole month ('YYYY-MM') or an exact day ('YYYY-MM-DD') —
# the day form drives the frontend's "Custom dates" mode.
_BOUND_PATTERN = r"^\d{4}-\d{2}(-\d{2})?$"


def _parse_bound(value: str, *, is_end: bool) -> date:
    """'YYYY-MM' → first (start) / last (end) day of that month; 'YYYY-MM-DD' → that exact day."""
    if len(value) == 7:
        first, last = _parse_month_range(value)
        return last if is_end else first
    return date.fromisoformat(value)


class WhopLiveDealItem(BaseModel):
    """One deal row inside a rep's collapsible table."""
    # None for orphan Whop payments (a Whop coaching payment with no GHL deal) —
    # those are keyed by whop_membership_id instead.
    ghl_opportunity_id: str | None = None
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
    is_orphan: bool = False
    whop_membership_id: str | None = None


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
    orphans_pending: list[WhopLiveDealItem] = []
    last_refreshed: str | None


def _safe_items(raw: list[dict]) -> list[WhopLiveDealItem]:
    """Build deal items, skipping (and logging) any single malformed row.

    Defence-in-depth: a schema/data mismatch on ONE row must never 500 the whole
    New Deals section — the rest of the reps' deals should still render. The
    dropped row is logged with enough context to chase down and fix at the source.
    """
    items: list[WhopLiveDealItem] = []
    for d in raw:
        try:
            items.append(WhopLiveDealItem(**d))
        except Exception as exc:
            logger.error(
                "Skipping malformed whop-live item (opp=%s, membership=%s): %s",
                (d or {}).get("ghl_opportunity_id"),
                (d or {}).get("whop_membership_id"),
                exc,
            )
    return items


@router.get("/pnl/whop-live", response_model=WhopLiveResponse)
async def pnl_whop_live(
    month: str = Query(..., pattern=_BOUND_PATTERN, description="Month (YYYY-MM) or exact day (YYYY-MM-DD); range start when 'end' given"),
    end: str | None = Query(None, pattern=_BOUND_PATTERN, description="Optional range end — month (YYYY-MM) or exact day (YYYY-MM-DD)"),
    db: AsyncSession = Depends(get_db),
) -> WhopLiveResponse:
    """New-deals view: deals whose FIRST Whop payment landed in the month (or the
    month/day range when `end` is given), grouped by rep."""
    try:
        month_start = _parse_bound(month, is_end=False)
        month_end = _parse_bound(end or month, is_end=True)
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
            deals=_safe_items(r["deals"]),
        )
        for r in data["reps"]
    ]
    return WhopLiveResponse(
        month=month,
        reps=reps,
        totals=data["totals"],
        orphans_pending=_safe_items(data.get("orphans_pending", [])),
        last_refreshed=data["last_refreshed"],
    )


@router.get("/pnl/whop-live/months")
async def whop_live_months(db: AsyncSession = Depends(get_db)) -> list[str]:
    """Return the deal-months ('YYYY-MM') that drive the Live lens month picker — current month included."""
    return await get_available_deal_months(db)


@router.get("/pnl/collections")
async def pnl_collections(
    start: str = Query(..., pattern=_BOUND_PATTERN, description="Range start — month (YYYY-MM) or exact day (YYYY-MM-DD)"),
    end: str | None = Query(None, pattern=_BOUND_PATTERN, description="Range end — month (YYYY-MM) or exact day (YYYY-MM-DD); defaults to start"),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Projected collections across ALL payment plans for a month range — how much
    lands each month (collected vs still-outstanding), refunds, and the plan
    breakdown. Day-level bounds filter installments by their (estimated) dates.
    Future cash is estimated (equal monthly installments); financed
    deals settle upfront. See db/queries/collections.py."""
    try:
        range_start = _parse_bound(start, is_end=False)
        range_end = _parse_bound(end or start, is_end=True)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid range: {start}..{end}")
    if range_end < range_start:
        raise HTTPException(status_code=422, detail="Range end is before start")
    return await get_collections_for_range(db, range_start, range_end)


class OrphanReviewInput(BaseModel):
    whop_membership_id: str
    action: str  # 'confirm' | 'ignore' | 'reset'


@router.post("/pnl/whop-orphan/review")
async def review_orphan(body: OrphanReviewInput, db: AsyncSession = Depends(get_db)) -> dict:
    """Resolve an orphan Whop coaching payment (no GHL deal). confirm → counts under
    Unassigned; ignore → hidden; reset → back to pending."""
    from db.queries.whop_orphans import set_orphan_status
    status = {"confirm": "confirmed", "ignore": "ignored", "reset": "pending"}.get(body.action)
    if status is None:
        raise HTTPException(status_code=422, detail=f"Invalid action: {body.action}")
    ok = await set_orphan_status(db, body.whop_membership_id, status)
    if not ok:
        raise HTTPException(status_code=404, detail=f"Orphan {body.whop_membership_id} not found")
    return {"ok": True, "whop_membership_id": body.whop_membership_id, "status": status}


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
