"""Per-rep metric queries — returns all reps in a single query."""

from datetime import date, timedelta

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity
from db.queries.common import (
    QUALIFIED_LEAD_QUALITY,
    base_filter,
    bookable_1st_call_expr,
    bookable_2nd_call_expr,
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
            Opportunity.projected_deal_size,
        )
        .where(and_(*conditions))
        .order_by(Opportunity.updated_at_ghl.desc())
    )

    return [
        {
            "name": row.opportunity_name or "—",
            "rep": row.opportunity_owner_name or "Unassigned",
            "close_date": row.updated_at_ghl.strftime("%b %d, %Y") if row.updated_at_ghl else "—",
            "value": float(row.projected_deal_size) if row.projected_deal_size else None,
        }
        for row in result.all()
    ]


async def get_rep_opps(
    session: AsyncSession,
    rep_id: str | None,
    opp_type: str,  # "booked" | "showed" | "not_logged"
    start: date,
    end: date,
    date_by: str,
) -> list[dict]:
    """Booked, showed, or outcome-unfilled 1st-call opps for a rep (drill-down modal).

    opp_type='booked'     → all opps with a 1st call in range
    opp_type='showed'     → subset that actually showed
    opp_type='not_logged' → opps where rep never logged the call outcome
    """
    bf = base_filter(start, end, date_by, rep_id)
    is_1st = has_1st_call(start, end, date_by)

    if opp_type == "showed":
        showed_1st = showed_1st_call_expr()
        row_filter = and_(bf, is_1st, showed_1st)
    elif opp_type == "not_logged":
        row_filter = and_(bf, is_1st, Opportunity.outcome_unfilled.is_(True))
    else:
        row_filter = and_(bf, is_1st)

    result = await session.execute(
        select(
            Opportunity.opportunity_name,
            Opportunity.ghl_opportunity_id,
            Opportunity.call1_appointment_date,
            Opportunity.pipeline_stage_name,
            Opportunity.opportunity_owner_name,
            Opportunity.call1_appointment_status,
        )
        .where(row_filter)
        .order_by(Opportunity.call1_appointment_date.desc())
    )

    return [
        {
            "name": row.opportunity_name or "—",
            "ghl_opportunity_id": row.ghl_opportunity_id,
            "appt_date": row.call1_appointment_date.strftime("%b %d, %Y") if row.call1_appointment_date else "—",
            "stage": row.pipeline_stage_name or "—",
            "rep": row.opportunity_owner_name or "Unassigned",
            "status": row.call1_appointment_status or "—",
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
                case((and_(is_1st, bookable_1st_call_expr()), 1))
            ).label("bookable_1st"),
            func.count(case((is_2nd, 1))).label("calls_booked_2nd"),
            func.count(case((and_(is_2nd, showed_2nd), 1))).label("shows_2nd"),
            func.count(
                case((and_(is_2nd, bookable_2nd_call_expr()), 1))
            ).label("bookable_2nd"),
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
                        Opportunity.lead_quality == "DQ",
                    ),
                    1,
                ))
            ).label("dq_count"),
            func.count(
                case((Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID, 1))
            ).label("units_closed"),
            func.coalesce(
                func.sum(
                    case((Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID, Opportunity.projected_deal_size))
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
            # DQ'd after 2nd call booked (passed call 1 screen but still didn't qualify)
            func.count(
                case((
                    and_(
                        is_1st,
                        showed_1st,
                        Opportunity.lead_quality == "DQ",
                        Opportunity.call2_appointment_date.isnot(None),
                    ),
                    1,
                ))
            ).label("dq_after_call2_count"),
            # Call outcome not logged (show/no-show never marked by rep)
            func.count(
                case((and_(is_1st, Opportunity.outcome_unfilled.is_(True)), 1))
            ).label("outcome_not_logged_count"),
            # Avg deal cycle (days from contact creation to close) — won deals only.
            # close proxy: close_date → call2_appointment_date → updated_at_ghl
            # start proxy: contact_created_at → created_at_ghl
            func.avg(
                case((
                    Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID,
                    func.extract(
                        "epoch",
                        func.coalesce(
                            Opportunity.close_date,
                            Opportunity.call2_appointment_date,
                            Opportunity.updated_at_ghl,
                        ) - func.coalesce(
                            Opportunity.contact_created_at,
                            Opportunity.created_at_ghl,
                        ),
                    ) / 86400.0,
                ))
            ).label("avg_cycle_days"),
        )
        .where(bf)
        .group_by(Opportunity.opportunity_owner_id, Opportunity.opportunity_owner_name)
        .order_by(Opportunity.opportunity_owner_name)
    )

    def safe_rate(num: int, den: int) -> float | None:
        return round(num / den, 4) if den else None  # type: ignore[call-overload]

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
            "show_rate_2nd": safe_rate(row.shows_2nd, row.bookable_2nd),
            "qualification_rate": safe_rate(row.qualified_shows, row.shows_1st),
            "dq_rate": safe_rate(row.dq_count, row.shows_1st),
            "dq_after_call2_rate": safe_rate(row.dq_after_call2_count, row.shows_1st),
            "close_rate": safe_rate(row.units_closed, row.total_shows),
            "units_closed": row.units_closed,
            "projected_contract_value": float(row.projected_contract_value),
            "total_shows": row.total_shows,
            "compliance_failures": row.compliance_failures,
            "outcome_not_logged_count": row.outcome_not_logged_count,
            "avg_cycle_days": round(float(row.avg_cycle_days), 1) if row.avg_cycle_days is not None else None,
        }
        for row in result.all()
    ]


async def get_daily_activity(
    session: AsyncSession,
    rep_id: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict]:
    """Day-by-day booked / showed / qual counts for a 7-day window.

    Defaults to the rolling last 7 days when start_date/end_date are omitted.
    Accepts an optional rep_id to scope to a single rep.
    """
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=6)  # 7 days inclusive

    showed_1st = showed_1st_call_expr()

    conditions = [
        Opportunity.is_excluded.is_(False),
        Opportunity.call1_appointment_date.isnot(None),
        func.date(Opportunity.call1_appointment_date) >= start_date,
        func.date(Opportunity.call1_appointment_date) <= end_date,
    ]
    if rep_id:
        conditions.append(Opportunity.opportunity_owner_id == rep_id)

    result = await session.execute(
        select(
            func.date(Opportunity.call1_appointment_date).label("day"),
            func.count(Opportunity.id).label("booked"),
            func.count(case((showed_1st, 1))).label("showed"),
            func.count(
                case((
                    and_(
                        showed_1st,
                        Opportunity.lead_quality.in_(QUALIFIED_LEAD_QUALITY),
                    ),
                    1,
                ))
            ).label("qual"),
        )
        .where(and_(*conditions))
        .group_by(func.date(Opportunity.call1_appointment_date))
        .order_by(func.date(Opportunity.call1_appointment_date))
    )

    def safe_rate(num: int, den: int) -> float | None:
        return round(float(num) / den, 4) if den else None  # type: ignore[call-overload]

    return [
        {
            "day": row.day.isoformat() if hasattr(row.day, "isoformat") else str(row.day),
            "booked": row.booked,
            "showed": row.showed,
            "show_rate": safe_rate(row.showed, row.booked),
            "qual": row.qual,
            "qual_rate": safe_rate(row.qual, row.showed),
        }
        for row in result.all()
    ]
