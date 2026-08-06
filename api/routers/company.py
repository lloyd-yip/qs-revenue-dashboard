"""Company-wide CEO overview endpoint — powers static/company.html.

Browser-facing and unauthenticated, matching the dashboard / whop_live router
convention (these serve static pages directly and read-only). The heavy lifting is
composed in db/queries/company.py, which reuses the existing metric queries — this
router only parses the month and returns the assembled payload.
"""

import calendar
import logging
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from db.queries.company import get_company_overview
from db.queries.whop_live import get_available_deal_months
from db.session import get_db
from sync.stripe_sync import sync_stripe_charges

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/dashboard", tags=["company"])

# Range bounds accept a whole month ('YYYY-MM') or an exact day ('YYYY-MM-DD'),
# matching the Deals/Collections filter bar so the CEO page shares its controls.
_BOUND_PATTERN = r"^\d{4}-\d{2}(-\d{2})?$"


def _parse_bound(value: str, *, is_end: bool) -> date:
    """'YYYY-MM' → first (start) / last (end) day of that month; 'YYYY-MM-DD' → that exact day."""
    if len(value) == 7:
        year, mon = (int(x) for x in value.split("-"))
        if not (1 <= mon <= 12):
            raise ValueError(f"month out of range: {value}")
        last = calendar.monthrange(year, mon)[1]
        return date(year, mon, last if is_end else 1)
    return date.fromisoformat(value)


@router.get("/company/overview")
async def company_overview(
    start: str = Query(..., pattern=_BOUND_PATTERN, description="Range start — month (YYYY-MM) or exact day (YYYY-MM-DD)"),
    end: str | None = Query(None, pattern=_BOUND_PATTERN, description="Range end — month (YYYY-MM) or exact day (YYYY-MM-DD); defaults to start"),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Company-wide CEO snapshot for a month range: live cash inflow by channel (Whop
    + bank wires), the collections snapshot, who still owes (payment-plan installments
    due/overdue), and the reconciled Xero P&L. Day-level bounds enable the same custom
    date-range mode as the Collections tab. See db/queries/company.py."""
    try:
        range_start = _parse_bound(start, is_end=False)
        range_end = _parse_bound(end or start, is_end=True)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid range: {start}..{end}")
    if range_end < range_start:
        raise HTTPException(status_code=422, detail="Range end is before start")
    return await get_company_overview(db, range_start, range_end)


@router.get("/company/months")
async def company_months(db: AsyncSession = Depends(get_db)) -> list[str]:
    """Months ('YYYY-MM') that have deal activity — drives the CEO page month picker,
    current month included. Reuses the Deals/Live month list."""
    return await get_available_deal_months(db)


@router.post("/company/stripe-refresh")
async def company_stripe_refresh() -> dict:
    """Pull the latest succeeded Stripe charges into stripe_charges — the same job the
    nightly cron runs. Includes GHL sub-account subscriptions + commissions (no amount
    floor). Idempotent (upsert on charge id). Returns {ok, fetched, upserted, skipped}.

    Browser-facing/no-auth like the sibling /pnl/whop-refresh; degrades to
    ok=False when STRIPE_SECRET_KEY is not configured."""
    try:
        return await sync_stripe_charges()
    except Exception as exc:
        logger.error("Stripe refresh failed: %s", exc, exc_info=True)
        return {"ok": False, "error": str(exc), "fetched": 0, "upserted": 0}
