"""Time-bucketed metric queries for chart granularity (day / week / month)."""

from datetime import date

from sqlalchemy import and_, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity
from db.queries.common import base_filter, bookable_1st_call_expr, has_1st_call, showed_1st_call_expr

_TRUNC_MAP = {"day": "day", "week": "week", "month": "month"}


async def get_time_series(
    session: AsyncSession,
    start: date,
    end: date,
    granularity: str = "week",
    date_by: str = "appointment",
    rep_id: str | None = None,
) -> list[dict]:
    """Return per-period show rate data for the line chart.

    Each row: { period (ISO string), calls_booked, shows, show_rate }
    Periods with zero bookings are included only if data exists — gaps are handled
    by Chart.js spanGaps on the frontend.
    """
    trunc_unit = _TRUNC_MAP.get(granularity, "week")

    bf = base_filter(start, end, date_by, rep_id)
    is_1st = has_1st_call(start, end, date_by)
    showed_1st = showed_1st_call_expr()

    # Date column to bucket on
    if date_by == "appointment":
        date_col = Opportunity.call1_appointment_date
    else:
        date_col = Opportunity.created_at_ghl

    period_expr = func.date_trunc(trunc_unit, date_col)

    result = await session.execute(
        select(
            period_expr.label("period"),
            func.count(case((is_1st, 1))).label("calls_booked"),
            func.count(
                case((and_(is_1st, showed_1st), 1))
            ).label("shows"),
            func.count(
                case((and_(is_1st, bookable_1st_call_expr()), 1))
            ).label("bookable"),
        )
        .where(bf)
        .group_by(period_expr)
        .order_by(period_expr)
    )

    def safe_rate(num: int, den: int) -> float | None:
        return round(num / den, 4) if den else None

    return [
        {
            "period": row.period.isoformat() if row.period else None,
            "calls_booked": row.calls_booked,
            "shows": row.shows,
            "show_rate": safe_rate(row.shows, row.bookable),
        }
        for row in result.all()
    ]
