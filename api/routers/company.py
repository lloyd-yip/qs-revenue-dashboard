"""Company-wide CEO overview endpoint — powers static/company.html.

Browser-facing and unauthenticated, matching the dashboard / whop_live router
convention (these serve static pages directly and read-only). The heavy lifting is
composed in db/queries/company.py, which reuses the existing metric queries — this
router only parses the month and returns the assembled payload.
"""

import logging
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from db.queries.company import get_company_overview
from db.queries.whop_live import get_available_deal_months
from db.session import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/dashboard", tags=["company"])

_MONTH_PATTERN = r"^\d{4}-\d{2}$"


def _parse_month(month: str) -> tuple[date, date]:
    """'YYYY-MM' → (first_day, last_day). Raises ValueError on an invalid month."""
    import calendar

    year, mon = (int(x) for x in month.split("-"))
    if not (1 <= mon <= 12):
        raise ValueError(f"month out of range: {month}")
    return date(year, mon, 1), date(year, mon, calendar.monthrange(year, mon)[1])


@router.get("/company/overview")
async def company_overview(
    month: str = Query(..., pattern=_MONTH_PATTERN, description="Month as YYYY-MM"),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Company-wide CEO snapshot for a month: live cash inflow by channel (Whop +
    bank wires), the collections snapshot, who still owes (payment-plan installments
    due/overdue), and the reconciled Xero P&L. See db/queries/company.py."""
    try:
        month_start, month_end = _parse_month(month)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"Invalid month: {month}")
    return await get_company_overview(db, month_start, month_end)


@router.get("/company/months")
async def company_months(db: AsyncSession = Depends(get_db)) -> list[str]:
    """Months ('YYYY-MM') that have deal activity — drives the CEO page month picker,
    current month included. Reuses the Deals/Live month list."""
    return await get_available_deal_months(db)
