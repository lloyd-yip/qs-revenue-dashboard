"""Per-rep metric queries — returns all reps in a single query."""

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


async def get_rep_closes(
    session: AsyncSession,
    rep_id: str | None,
    start: date,
    end: date,
    date_by: str,
) -> list[dict]:
    """Closed deals for a specific rep (or all reps when rep_id is None).

    Returns opportunity name, closer, close date (updated_at_ghl), and deal value.
    Ordered by close date descending.
    """
    bf = base_filter(start, end, date_by)
    conditions = [bf, Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID]
    if rep_id is not None:
        conditions.append(Opportunity.opportunity_owner_id == rep_id)

    result = await session.execute(
        select(
            Opportunity.opportunity_name,
            Opportunity.opportunity_owner_name,
            Opportunity.updated_at_ghl,
            Opportunity.monetary_value,
        )
        .where(and_(*conditions))
        .order_by(Opportunity.updated_at_ghl.desc())
    )

    return [
        {
            "name": row.opportunity_name or "—",
            "rep": row.opportunity_owner_name or "Unassigned",
            "close_date": row.updated_at_ghl.strftime("%b %d, %Y") if row.updated_at_ghl else "—",
            "value": float(row.monetary_value) if row.monetary_value else None,
        }
        for row in result.all()
    ]


async def get_by_rep(
    session: AsyncSession,
    start: date,
    end: date,
    date_by: str,
) -> list[dict]:
    """All KPIs broken down per rep in a single aggregated query."""

    bf = base_filter(start, end, date_by)
    is_1st = has_1st_call(start, end, date_by)
    is_2nd = has_2nd_call(start, end, date_by)
    showed_1st = showed_1st_call_expr()
    showed_2nd = showed_2nd_call_expr()

    result = await session.execute(
        select(
            Opportunity.opportunity_owner_id.label("rep_id"),
            Opportunity.opportunity_owner_name.label("rep_name"),
            func.count(case((is_1st, 1))).label("calls_booked_1st"),
            func.count(
                case((and_(is_1st, showed_1st, ~Opportunity.outcome_unfilled), 1))
            ).label("shows_1st"),
            func.count(
                case((and_(is_1st, ~Opportunity.outcome_unfilled), 1))
            ).label("bookable_1st"),
            func.count(case((is_2nd, 1))).label("calls_booked_2nd"),
            func.count(case((and_(is_2nd, showed_2nd), 1))).label("shows_2nd"),
            func.count(
                case((
                    and_(is_1st, showed_1st, Opportunity.lead_quality.in_(QUALIFIED_LEAD_QUALITY)),
                    1,
                ))
            ).label("qualified_shows"),
            func.count(
                case((and_(is_1st, showed_1st, Opportunity.lead_quality.isnot(None)), 1))
            ).label("shows_with_quality_filled"),
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
            func.count(
                case((Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID, 1))
            ).label("units_closed"),
            func.coalesce(
                func.sum(
                    case((Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID, Opportunity.monetary_value))
                ),
                0,
            ).label("projected_contract_value"),
            func.count(
                case((
                    or_(
                        and_(is_1st, showed_1st),
                        and_(is_2nd, showed_2nd),
                    ),
                    1,
                ))
            ).label("total_shows"),
            func.count(
                case((Opportunity.rep_compliance_failure.is_(True), 1))
            ).label("compliance_failures"),
            # DQ'd immediately after 1st call (never booked 2nd)
            func.count(
                case((
                    and_(
                        is_1st,
                        showed_1st,
                        or_(
                            Opportunity.lead_quality == "DQ",
                            Opportunity.pipeline_stage_id == DISQUALIFIED_STAGE_ID,
                        ),
                        Opportunity.call2_appointment_date.is_(None),
                    ),
                    1,
                ))
            ).label("dq_after_call1_count"),
            # Call outcome not logged (show/no-show never marked by rep)
            func.count(
                case((and_(is_1st, Opportunity.outcome_unfilled.is_(True)), 1))
            ).label("outcome_not_logged_count"),
        )
        .where(bf)
        .group_by(Opportunity.opportunity_owner_id, Opportunity.opportunity_owner_name)
        .order_by(Opportunity.opportunity_owner_name)
    )

    def safe_rate(num: int, den: int) -> float | None:
        return round(num / den, 4) if den else None

    return [
        {
            "rep_id": row.rep_id,
            "rep_name": row.rep_name or "Unassigned",
            "calls_booked_1st": row.calls_booked_1st,
            "shows_1st": row.shows_1st,
            "show_rate_1st": safe_rate(row.shows_1st, row.bookable_1st),
            "no_show_rate_1st": safe_rate(row.bookable_1st - row.shows_1st, row.bookable_1st),
            "calls_booked_2nd": row.calls_booked_2nd,
            "shows_2nd": row.shows_2nd,
            "show_rate_2nd": safe_rate(row.shows_2nd, row.calls_booked_2nd),
            "qualification_rate": safe_rate(row.qualified_shows, row.shows_1st),
            "dq_rate": safe_rate(row.dq_count, row.shows_1st),
            "dq_after_call1_rate": safe_rate(row.dq_after_call1_count, row.shows_1st),
            "close_rate": safe_rate(row.units_closed, row.total_shows),
            "units_closed": row.units_closed,
            "projected_contract_value": float(row.projected_contract_value),
            "total_shows": row.total_shows,
            "compliance_failures": row.compliance_failures,
            "outcome_not_logged_count": row.outcome_not_logged_count,
        }
        for row in result.all()
    ]
