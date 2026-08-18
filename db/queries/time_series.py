"""Time-bucketed metric queries for chart intervals (day / week / month / quarter / year)."""

from datetime import date

from sqlalchemy import and_, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity
from db.queries.common import (
    QUALIFIED_LEAD_QUALITY,
    base_filter,
    bookable_1st_call_expr,
    has_1st_call,
    showed_1st_call_expr,
)
from sync.ghl_client import DEAL_WON_STAGE_ID

# Chart interval → Postgres date_trunc unit. All five are native date_trunc units,
# so widening this needs no other query change.
_TRUNC_MAP = {
    "day": "day",
    "week": "week",
    "month": "month",
    "quarter": "quarter",
    "year": "year",
}


async def get_time_series(
    session: AsyncSession,
    start: date,
    end: date,
    granularity: str = "week",
    date_by: str = "appointment",
    rep_id: str | None = None,
) -> list[dict]:
    """Return per-period funnel-rate data for the trends line chart.

    Each row: { period (ISO string), calls_booked, shows, qualified_shows,
                units_closed, show_rate, qual_rate, close_rate }

    Rate definitions match metrics_summary exactly so the chart and the KPI cards
    can never disagree: show_rate = shows ÷ occurred (bookable), qual_rate =
    qualified shows ÷ shows, close_rate = cohort units closed ÷ shows.
    Only periods that actually contain a 1st call are returned; a rate is None when
    its denominator is 0, and the frontend spans those gaps.
    """
    trunc_unit = _TRUNC_MAP.get(granularity, "week")

    bf = base_filter(start, end, date_by, rep_id)
    is_1st = has_1st_call(start, end, date_by)
    showed_1st = showed_1st_call_expr()

    # Date column to bucket on — must match the date_by filter dimension, else points
    # land on the wrong timeline (and can fall outside the visible window).
    if date_by == "appointment":
        date_col = Opportunity.call1_appointment_date
    elif date_by == "booked":
        date_col = Opportunity.call1_booking_date
    else:  # created
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
            func.count(
                case((
                    and_(is_1st, showed_1st, Opportunity.lead_quality.in_(QUALIFIED_LEAD_QUALITY)),
                    1,
                ))
            ).label("qualified_shows"),
            # Cohort close: of this period's 1st calls, how many are now won.
            # Gated by is_1st so it stays a subset of shows → close_rate never exceeds 100%.
            func.count(
                case((and_(is_1st, Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID), 1))
            ).label("units_closed"),
        )
        # Every metric above is already gated on is_1st, so restricting the rows to
        # is_1st changes no count — it only drops all-zero phantom buckets. Those come
        # from 'appointment' mode, where base_filter admits an opp whose 2nd call is in
        # range while its 1st call sits years earlier: it buckets on that old call1 date
        # and contributes 0 to every metric, stretching the axis back to 2024 and
        # squashing the real months into a sliver.
        .where(and_(bf, is_1st))
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
            "qualified_shows": row.qualified_shows,
            "units_closed": row.units_closed,
            "show_rate": safe_rate(row.shows, row.bookable),
            "qual_rate": safe_rate(row.qualified_shows, row.shows),
            "close_rate": safe_rate(row.units_closed, row.shows),
        }
        for row in result.all()
    ]
