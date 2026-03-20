"""Team-level metric queries."""

from datetime import date

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity
from db.queries.common import (
    QUALIFIED_LEAD_QUALITY,
    base_filter,
    has_1st_call,
    has_2nd_call,
    showed_1st_call_expr,
    showed_2nd_call_expr,
)
from sync.ghl_client import DEAL_WON_STAGE_ID, DISQUALIFIED_STAGE_ID


async def get_summary(
    session: AsyncSession,
    start: date,
    end: date,
    date_by: str,
    rep_id: str | None = None,
) -> dict:
    """Compute all team-level KPIs in a single query."""

    bf = base_filter(start, end, date_by, rep_id)
    is_1st = has_1st_call(start, end, date_by)
    is_2nd = has_2nd_call(start, end, date_by)
    showed_1st = showed_1st_call_expr()
    showed_2nd = showed_2nd_call_expr()

    result = await session.execute(
        select(
            # Calls booked (1st call) — opps with a 1st call date in scope
            func.count(case((is_1st, 1))).label("calls_booked_1st"),
            # Shows (1st call) — showed + had 1st call + no compliance failure
            func.count(
                case((and_(is_1st, showed_1st, ~Opportunity.outcome_unfilled), 1))
            ).label("shows_1st"),
            # Bookable 1st calls (show rate denominator — exclude compliance failures)
            func.count(
                case((and_(is_1st, ~Opportunity.outcome_unfilled), 1))
            ).label("bookable_1st"),
            # Calls booked (2nd call)
            func.count(case((is_2nd, 1))).label("calls_booked_2nd"),
            # Shows (2nd call)
            func.count(case((and_(is_2nd, showed_2nd), 1))).label("shows_2nd"),
            # Qualified shows (1st call only)
            func.count(
                case((
                    and_(is_1st, showed_1st, Opportunity.lead_quality.in_(QUALIFIED_LEAD_QUALITY)),
                    1,
                ))
            ).label("qualified_shows"),
            # Shows with lead_quality filled (qual rate denominator)
            func.count(
                case((and_(is_1st, showed_1st, Opportunity.lead_quality.isnot(None)), 1))
            ).label("shows_with_quality_filled"),
            # DQ'd (1st call shows only)
            func.count(
                case((
                    and_(
                        is_1st,
                        showed_1st,
                        or_(
                            Opportunity.lead_quality == "DQ",
                            Opportunity.pipeline_stage_id == DISQUALIFIED_STAGE_ID,
                        ),
                    ),
                    1,
                ))
            ).label("dq_count"),
            # Units closed
            func.count(
                case((Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID, 1))
            ).label("units_closed"),
            # Projected contract value
            func.coalesce(
                func.sum(
                    case((Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID, Opportunity.monetary_value))
                ),
                0,
            ).label("projected_contract_value"),
            # Total shows (1st + 2nd) for close rate denominator
            func.count(
                case((
                    or_(
                        and_(is_1st, showed_1st),
                        and_(is_2nd, showed_2nd),
                    ),
                    1,
                ))
            ).label("total_shows"),
            # Rep compliance failures
            func.count(
                case((Opportunity.rep_compliance_failure.is_(True), 1))
            ).label("compliance_failures"),
        ).where(bf)
    )
    row = result.one()

    def safe_rate(numerator: int, denominator: int) -> float | None:
        return round(numerator / denominator, 4) if denominator else None

    return {
        "calls_booked_1st": row.calls_booked_1st,
        "shows_1st": row.shows_1st,
        "show_rate_1st": safe_rate(row.shows_1st, row.bookable_1st),
        "no_show_rate_1st": safe_rate(row.bookable_1st - row.shows_1st, row.bookable_1st),
        "calls_booked_2nd": row.calls_booked_2nd,
        "shows_2nd": row.shows_2nd,
        "show_rate_2nd": safe_rate(row.shows_2nd, row.calls_booked_2nd),
        "qualification_rate": safe_rate(row.qualified_shows, row.shows_1st),
        "dq_rate": safe_rate(row.dq_count, row.shows_1st),
        "close_rate": safe_rate(row.units_closed, row.total_shows),
        "units_closed": row.units_closed,
        "projected_contract_value": float(row.projected_contract_value),
        "total_shows": row.total_shows,
        "compliance_failures": row.compliance_failures,
    }
