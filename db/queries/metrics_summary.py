"""Team-level metric queries."""

from datetime import date

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Appointment, Opportunity
from db.queries.common import (
    ALL_TEAM_SENTINEL,
    QUALIFIED_LEAD_QUALITY,
    base_filter,
    bookable_1st_call_expr,
    bookable_2nd_call_expr,
    has_1st_call,
    has_2nd_call,
    sales_rep_filter,
    showed_1st_call_expr,
    showed_2nd_call_expr,
)
from sync.ghl_client import DEAL_WON_STAGE_ID, DISQUALIFIED_STAGE_ID


def _opp_rep_conditions(rep_id: str | None) -> list:
    """Opportunity restriction (is_excluded + rep filter) WITHOUT a date filter —
    the appointment stats scope by appointment_date separately."""
    conds = [Opportunity.is_excluded.is_(False)]
    if rep_id == ALL_TEAM_SENTINEL:
        pass
    elif rep_id:
        conds.append(Opportunity.opportunity_owner_id == rep_id)
    else:
        conds.append(sales_rep_filter())
    return conds


async def get_appointment_call_stats(
    session: AsyncSession, start: date, end: date, rep_id: str | None = None
) -> dict:
    """Calendar-based, reschedule-aware call stats for the range (from the appointments
    table, independent of date_by):

      scheduled_{1st,2nd}     — opps with an ACTIVE (non-cancelled) call appointment in range
      rescheduled_{1st,2nd}   — of those, opps with >1 appointment of that type (moved >=1x)
      moved_to_future_{1st,2nd} — rescheduled opps whose latest appointment is still upcoming
    """
    opp_conds = _opp_rep_conditions(rep_id)
    out: dict = {}
    for call_type, suffix in (("call_1", "1st"), ("call_2", "2nd")):
        is_type = Appointment.appointment_type == call_type
        in_range_active = and_(
            is_type,
            func.date(Appointment.appointment_date) >= start,
            func.date(Appointment.appointment_date) <= end,
            Appointment.appointment_status != "Cancelled",
        )
        per_opp = (
            select(
                Opportunity.ghl_opportunity_id.label("oid"),
                func.count().filter(is_type).label("total"),
                func.count().filter(in_range_active).label("in_range"),
                func.max(Appointment.appointment_date).filter(is_type).label("last_date"),
            )
            .select_from(Opportunity)
            .join(Appointment, Appointment.ghl_contact_id == Opportunity.ghl_contact_id)
            .where(and_(*opp_conds))
            .group_by(Opportunity.ghl_opportunity_id)
            .subquery()
        )
        scheduled = func.count().filter(per_opp.c.in_range > 0)
        rescheduled = func.count().filter(and_(per_opp.c.in_range > 0, per_opp.c.total > 1))
        moved_future = func.count().filter(
            and_(per_opp.c.in_range > 0, per_opp.c.total > 1, per_opp.c.last_date > func.now())
        )
        r = (await session.execute(
            select(
                scheduled.label("scheduled"),
                rescheduled.label("rescheduled"),
                moved_future.label("moved_future"),
            ).select_from(per_opp)
        )).one()
        out[f"scheduled_{suffix}"] = r.scheduled or 0
        out[f"rescheduled_{suffix}"] = r.rescheduled or 0
        out[f"moved_to_future_{suffix}"] = r.moved_future or 0
    return out


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
            # Shows (1st call) — showed + had 1st call
            func.count(
                case((and_(is_1st, showed_1st), 1))
            ).label("shows_1st"),
            # Bookable 1st calls (show rate denominator: Showed + No Show + Cancelled)
            func.count(
                case((and_(is_1st, bookable_1st_call_expr()), 1))
            ).label("bookable_1st"),
            # Calls booked (2nd call)
            func.count(case((is_2nd, 1))).label("calls_booked_2nd"),
            # Shows (2nd call)
            func.count(case((and_(is_2nd, showed_2nd), 1))).label("shows_2nd"),
            # Bookable 2nd calls (show rate denominator: Showed + No Show + Cancelled)
            func.count(
                case((and_(is_2nd, bookable_2nd_call_expr()), 1))
            ).label("bookable_2nd"),
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
            # DQ'd (lead_quality field OR moved to Disqualified stage — reps don't always fill both)
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
            # DQ'd after having a 2nd call booked (passed call 1 screen but still didn't qualify)
            func.count(
                case((
                    and_(
                        is_1st,
                        showed_1st,
                        or_(
                            Opportunity.lead_quality == "DQ",
                            Opportunity.pipeline_stage_id == DISQUALIFIED_STAGE_ID,
                        ),
                        Opportunity.call2_appointment_date.isnot(None),
                    ),
                    1,
                ))
            ).label("dq_after_call2_count"),
            # Units closed
            func.count(
                case((Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID, 1))
            ).label("units_closed"),
            # Projected contract value (from Projected Deal Size custom field)
            func.coalesce(
                func.sum(
                    case((Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID, Opportunity.projected_deal_size))
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

    # Calendar-based, reschedule-aware call stats (scheduled / rescheduled / moved-to-future).
    appt = await get_appointment_call_stats(session, start, end, rep_id)

    def safe_rate(numerator: int, denominator: int) -> float | None:
        return round(numerator / denominator, 4) if denominator else None

    return {
        "calls_booked_1st": row.calls_booked_1st,
        "scheduled_1st": appt["scheduled_1st"],
        "occurred_1st": row.bookable_1st,           # calls that have happened (show-rate denominator)
        "shows_1st": row.shows_1st,
        "show_rate_1st": safe_rate(row.shows_1st, row.bookable_1st),
        "no_show_rate_1st": safe_rate(row.bookable_1st - row.shows_1st, row.bookable_1st),
        "rescheduled_1st": appt["rescheduled_1st"],
        "reschedule_rate_1st": safe_rate(appt["rescheduled_1st"], appt["scheduled_1st"]),
        "moved_to_future_1st": appt["moved_to_future_1st"],
        "calls_booked_2nd": row.calls_booked_2nd,
        "scheduled_2nd": appt["scheduled_2nd"],
        "occurred_2nd": row.bookable_2nd,
        "shows_2nd": row.shows_2nd,
        "show_rate_2nd": safe_rate(row.shows_2nd, row.bookable_2nd),
        "rescheduled_2nd": appt["rescheduled_2nd"],
        "reschedule_rate_2nd": safe_rate(appt["rescheduled_2nd"], appt["scheduled_2nd"]),
        "moved_to_future_2nd": appt["moved_to_future_2nd"],
        "qualification_rate": safe_rate(row.qualified_shows, row.shows_1st),
        "dq_rate": safe_rate(row.dq_count, row.shows_1st),
        "dq_after_call2_rate": safe_rate(row.dq_after_call2_count, row.shows_1st),
        "close_rate": safe_rate(row.units_closed, row.shows_1st),
        "close_rate_qual": safe_rate(row.units_closed, row.qualified_shows),
        "units_closed": row.units_closed,
        "projected_contract_value": float(row.projected_contract_value),
        "total_shows": row.total_shows,
        "compliance_failures": row.compliance_failures,
    }
