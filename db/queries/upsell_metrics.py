"""Upsell metrics query — Client Delivery Revenue Pipeline.

Segments the upsell funnel by stage membership (ever-reached logic):
  Reached Outreach Sent  = stage IN [Outreach Sent, Call Scheduled, Closed Won, Closed Lost, Deal Value]
  Reached Call Scheduled = stage IN [Call Scheduled, Closed Won, Closed Lost, Deal Value]
  Closed Won             = stage = Closed Won
  Closed Lost            = stage = Closed Lost

Date anchor: created_at_ghl (opportunity creation date), filtered by selected range.
"""

from datetime import date

from sqlalchemy import and_, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity

UPSELL_PIPELINE_ID = "NjidsHukHHUpYtTcQefX"

# Stage IDs — Client Delivery Revenue Pipeline
STAGE_OUTREACH_SENT  = "e08df229-3462-4b6c-aa5a-7a16d6b18773"
STAGE_CALL_SCHEDULED = "859efac7-0f23-4d7e-8b53-032e95b69c58"
STAGE_CLOSED_WON     = "eafe20aa-00cd-445e-9f3b-95a21ed6f41a"
STAGE_CLOSED_LOST    = "8dc4ee8c-4150-4060-9503-61d0a9fbe37d"
STAGE_DEAL_VALUE     = "9a387dab-44d3-4e4e-a541-6b856b84fc93"

# Stages that indicate "reached Outreach Sent or beyond"
REACHED_OUTREACH = [
    STAGE_OUTREACH_SENT, STAGE_CALL_SCHEDULED,
    STAGE_CLOSED_WON, STAGE_CLOSED_LOST, STAGE_DEAL_VALUE,
]

# Stages that indicate "reached Call Scheduled or beyond"
REACHED_CALL_SCHEDULED = [
    STAGE_CALL_SCHEDULED, STAGE_CLOSED_WON, STAGE_CLOSED_LOST, STAGE_DEAL_VALUE,
]

# Terminal closed stages
CLOSED_STAGES = [STAGE_CLOSED_WON, STAGE_CLOSED_LOST, STAGE_DEAL_VALUE]


async def get_upsell_summary(
    session: AsyncSession,
    start: date,
    end: date,
) -> dict:
    """Return top-level upsell funnel metrics for the selected creation date range."""

    in_period = and_(
        Opportunity.pipeline_id == UPSELL_PIPELINE_ID,
        func.date(Opportunity.created_at_ghl) >= start,
        func.date(Opportunity.created_at_ghl) <= end,
    )

    is_outreach  = Opportunity.pipeline_stage_id.in_(REACHED_OUTREACH)
    is_call_sched = Opportunity.pipeline_stage_id.in_(REACHED_CALL_SCHEDULED)
    is_won       = Opportunity.pipeline_stage_id == STAGE_CLOSED_WON
    is_lost      = Opportunity.pipeline_stage_id == STAGE_CLOSED_LOST
    is_closed    = Opportunity.pipeline_stage_id.in_(CLOSED_STAGES)

    result = await session.execute(
        select(
            func.count().label("total_opps"),
            func.count(case((is_outreach,  1))).label("reached_outreach"),
            func.count(case((is_call_sched, 1))).label("reached_call_scheduled"),
            func.count(case((is_won,  1))).label("closed_won"),
            func.count(case((is_lost, 1))).label("closed_lost"),
            func.coalesce(
                func.avg(case((is_won, Opportunity.monetary_value))),
                0,
            ).label("avg_deal_value"),
            func.coalesce(
                func.sum(case((is_won, Opportunity.monetary_value))),
                0,
            ).label("total_revenue"),
        )
        .where(in_period)
    )

    row = result.one()

    def safe_rate(n, d):
        return round(n / d, 4) if d else None

    won  = row.closed_won
    lost = row.closed_lost

    return {
        "total_opps":            row.total_opps,
        "reached_outreach":      row.reached_outreach,
        "reached_call_scheduled": row.reached_call_scheduled,
        "closed_won":            won,
        "closed_lost":           lost,
        "outreach_to_call_rate": safe_rate(row.reached_call_scheduled, row.reached_outreach),
        "win_rate":              safe_rate(won, won + lost),
        "loss_rate":             safe_rate(lost, won + lost),
        "avg_deal_value":        float(row.avg_deal_value),
        "total_revenue":         float(row.total_revenue),
    }


async def get_upsell_by_rep(
    session: AsyncSession,
    start: date,
    end: date,
) -> list[dict]:
    """Return per-rep upsell funnel metrics for the selected creation date range."""

    in_period = and_(
        Opportunity.pipeline_id == UPSELL_PIPELINE_ID,
        func.date(Opportunity.created_at_ghl) >= start,
        func.date(Opportunity.created_at_ghl) <= end,
    )

    is_outreach   = Opportunity.pipeline_stage_id.in_(REACHED_OUTREACH)
    is_call_sched = Opportunity.pipeline_stage_id.in_(REACHED_CALL_SCHEDULED)
    is_won        = Opportunity.pipeline_stage_id == STAGE_CLOSED_WON
    is_lost       = Opportunity.pipeline_stage_id == STAGE_CLOSED_LOST
    is_closed     = Opportunity.pipeline_stage_id.in_(CLOSED_STAGES)

    result = await session.execute(
        select(
            func.coalesce(Opportunity.opportunity_owner_name, "(Unassigned)").label("rep_name"),
            func.count().label("total_opps"),
            func.count(case((is_outreach,   1))).label("reached_outreach"),
            func.count(case((is_call_sched, 1))).label("reached_call_scheduled"),
            func.count(case((is_won,  1))).label("closed_won"),
            func.count(case((is_lost, 1))).label("closed_lost"),
            func.coalesce(
                func.avg(case((is_won, Opportunity.monetary_value))),
                0,
            ).label("avg_deal_value"),
            func.coalesce(
                func.sum(case((is_won, Opportunity.monetary_value))),
                0,
            ).label("total_revenue"),
        )
        .where(in_period)
        .group_by(Opportunity.opportunity_owner_name)
        .order_by(func.count().desc())
    )

    def safe_rate(n, d):
        return round(n / d, 4) if d else None

    rows = []
    for row in result.all():
        won  = row.closed_won
        lost = row.closed_lost
        rows.append({
            "rep_name":              row.rep_name,
            "total_opps":            row.total_opps,
            "reached_outreach":      row.reached_outreach,
            "reached_call_scheduled": row.reached_call_scheduled,
            "closed_won":            won,
            "closed_lost":           lost,
            "outreach_to_call_rate": safe_rate(row.reached_call_scheduled, row.reached_outreach),
            "win_rate":              safe_rate(won, won + lost),
            "loss_rate":             safe_rate(lost, won + lost),
            "avg_deal_value":        float(row.avg_deal_value),
            "total_revenue":         float(row.total_revenue),
        })
    return rows
