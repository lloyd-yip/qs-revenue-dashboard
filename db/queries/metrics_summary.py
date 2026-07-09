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


async def get_reschedule_stats(
    session: AsyncSession, start: date, end: date, date_by: str, rep_id: str | None = None
) -> dict:
    """Reschedule counts RESTRICTED TO THE BOOKED COHORT (opps with a 1st/2nd call in
    range per date_by), so they nest under the Booked count instead of counting the
    whole calendar:

      rescheduled_{1st,2nd}     — cohort opps with >1 appointment of that type (moved >=1x)
      moved_to_future_{1st,2nd} — of those, whose latest appointment is still upcoming
    """
    # Cohort = exactly the Booked cohort per date_by (base_filter includes rep + is_excluded
    # + the date filter), AND has_Nth_call — same as how calls_booked_{1st,2nd} is counted.
    bf = base_filter(start, end, date_by, rep_id)
    cohort = {
        "call_1": and_(bf, has_1st_call(start, end, date_by)),
        "call_2": and_(bf, has_2nd_call(start, end, date_by)),
    }
    out: dict = {}
    for call_type, suffix in (("call_1", "1st"), ("call_2", "2nd")):
        is_type = Appointment.appointment_type == call_type
        per_opp = (
            select(
                Opportunity.ghl_opportunity_id.label("oid"),
                func.count().filter(is_type).label("total"),
                func.max(Appointment.appointment_date).filter(is_type).label("last_date"),
            )
            .select_from(Opportunity)
            .join(Appointment, Appointment.ghl_contact_id == Opportunity.ghl_contact_id)
            .where(cohort[call_type])
            .group_by(Opportunity.ghl_opportunity_id)
            .subquery()
        )
        r = (await session.execute(
            select(
                func.count().filter(per_opp.c.total > 1).label("rescheduled"),
                func.count().filter(
                    and_(per_opp.c.total > 1, per_opp.c.last_date > func.now())
                ).label("moved_future"),
            ).select_from(per_opp)
        )).one()
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

    # "Scheduled" = of the booked cohort, calls whose APPOINTMENT date falls in the range.
    # In appointment mode this equals Booked; in booking/created mode it excludes calls
    # booked-in-range but scheduled for a future date outside the range — so the funnel
    # nests: Booked >= Scheduled >= Occurred >= Shows.
    appt1_in_range = and_(
        Opportunity.call1_appointment_date.isnot(None),
        func.date(Opportunity.call1_appointment_date) >= start,
        func.date(Opportunity.call1_appointment_date) <= end,
    )
    appt2_in_range = and_(
        Opportunity.call2_appointment_date.isnot(None),
        func.date(Opportunity.call2_appointment_date) >= start,
        func.date(Opportunity.call2_appointment_date) <= end,
    )

    result = await session.execute(
        select(
            # Calls booked (1st call) — opps with a 1st call date in scope
            func.count(case((is_1st, 1))).label("calls_booked_1st"),
            func.count(
                case((and_(is_1st, or_(appt1_in_range, bookable_1st_call_expr())), 1))
            ).label("scheduled_1st"),
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
            func.count(
                case((and_(is_2nd, or_(appt2_in_range, bookable_2nd_call_expr())), 1))
            ).label("scheduled_2nd"),
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

    # Reschedule counts, restricted to the booked cohort (so they nest under Booked).
    resched = await get_reschedule_stats(session, start, end, date_by, rep_id)

    # Deals CLOSED in the period (by close_date) — the intuitive "closed this period" count,
    # matching the drill-down and the Rep table. Shown alongside the cohort close_rate
    # (of this period's shows, how many have closed so far). row.units_closed / row.projected
    # remain the cohort figures used for close_rate.
    close_conds = [
        Opportunity.is_excluded.is_(False),
        Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID,
        Opportunity.close_date.isnot(None),
        func.date(Opportunity.close_date) >= start,
        func.date(Opportunity.close_date) <= end,
    ]
    if rep_id == ALL_TEAM_SENTINEL:
        pass
    elif rep_id:
        close_conds.append(Opportunity.opportunity_owner_id == rep_id)
    else:
        close_conds.append(sales_rep_filter())
    close_row = (await session.execute(
        select(
            func.count().label("closed_in_period"),
            func.coalesce(func.sum(Opportunity.projected_deal_size), 0).label("contract_value_period"),
        ).where(and_(*close_conds))
    )).one()

    def safe_rate(numerator: int, denominator: int) -> float | None:
        return round(numerator / denominator, 4) if denominator else None

    return {
        "calls_booked_1st": row.calls_booked_1st,
        "scheduled_1st": row.scheduled_1st,
        "occurred_1st": row.bookable_1st,           # calls that have happened (show-rate denominator)
        "shows_1st": row.shows_1st,
        "show_rate_1st": safe_rate(row.shows_1st, row.bookable_1st),
        "no_show_rate_1st": safe_rate(row.bookable_1st - row.shows_1st, row.bookable_1st),
        "rescheduled_1st": resched["rescheduled_1st"],
        "reschedule_rate_1st": safe_rate(resched["rescheduled_1st"], row.calls_booked_1st),
        "moved_to_future_1st": resched["moved_to_future_1st"],
        "calls_booked_2nd": row.calls_booked_2nd,
        "scheduled_2nd": row.scheduled_2nd,
        "occurred_2nd": row.bookable_2nd,
        "shows_2nd": row.shows_2nd,
        "show_rate_2nd": safe_rate(row.shows_2nd, row.bookable_2nd),
        "rescheduled_2nd": resched["rescheduled_2nd"],
        "reschedule_rate_2nd": safe_rate(resched["rescheduled_2nd"], row.calls_booked_2nd),
        "moved_to_future_2nd": resched["moved_to_future_2nd"],
        "qualification_rate": safe_rate(row.qualified_shows, row.shows_1st),
        "dq_rate": safe_rate(row.dq_count, row.shows_1st),
        "dq_after_call2_rate": safe_rate(row.dq_after_call2_count, row.shows_1st),
        # Close rate stays COHORT: of this period's 1st-call shows, how many have closed
        # so far (won opps within the call cohort ÷ shows). A leading indicator ≤100%.
        "close_rate": safe_rate(row.units_closed, row.shows_1st),
        "close_rate_qual": safe_rate(row.units_closed, row.qualified_shows),
        "cohort_won": row.units_closed,
        # Units Closed = deals CLOSED in the period (by close_date) — the headline count.
        "units_closed": close_row.closed_in_period,
        "projected_contract_value": float(close_row.contract_value_period),
        "total_shows": row.total_shows,
        "compliance_failures": row.compliance_failures,
    }
