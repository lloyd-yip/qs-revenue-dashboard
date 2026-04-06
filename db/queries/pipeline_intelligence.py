"""Pipeline Intelligence query — segment any metric by any dimension.

Group-by options:
  'rep'             → opportunity_owner_name
  'channel'         → canonical_channel
  'lead_quality'    → lead_quality
  'intent'          → intent_to_transform
  'indoctrination'  → pre_call_indoctrination
"""

from datetime import date

from sqlalchemy import and_, case, func, or_, select, text
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
from sync.ghl_client import DEAL_WON_STAGE_ID

# Maps group_by key → (column, display label)
GROUP_BY_MAP = {
    "rep": (Opportunity.opportunity_owner_name, "Rep"),
    "channel": (Opportunity.canonical_channel, "Channel"),
    "lead_quality": (Opportunity.lead_quality, "Lead Quality"),
    "intent": (Opportunity.intent_to_transform, "Intent to Transform"),
    "indoctrination": (Opportunity.pre_call_indoctrination, "Pre-Call Indoctrination"),
}


async def get_pipeline_intelligence(
    session: AsyncSession,
    group_by: str,
    start: date,
    end: date,
    date_by: str,
    rep_id: str | None = None,
) -> dict:
    """Return per-segment metrics for the Pipeline Intelligence page.

    Returns:
      {
        "dimension_label": str,  # human label for the group_by column
        "rows": [...]            # one dict per segment
      }
    """
    if group_by not in GROUP_BY_MAP:
        group_by = "rep"

    group_col, dimension_label = GROUP_BY_MAP[group_by]

    bf = base_filter(start, end, date_by, rep_id)
    is_1st = has_1st_call(start, end, date_by)
    is_2nd = has_2nd_call(start, end, date_by)
    showed_1st = showed_1st_call_expr()
    showed_2nd = showed_2nd_call_expr()

    is_won = Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID

    result = await session.execute(
        select(
            func.coalesce(group_col, "(Not Set)").label("segment"),
            # C1 metrics
            func.count(case((is_1st, 1))).label("calls_booked_1st"),
            func.count(case((and_(is_1st, showed_1st, ~Opportunity.outcome_unfilled), 1))).label("shows_1st"),
            func.count(case((and_(is_1st, ~Opportunity.outcome_unfilled), 1))).label("bookable_1st"),
            # Qualification
            func.count(
                case((and_(is_1st, showed_1st, Opportunity.lead_quality.in_(QUALIFIED_LEAD_QUALITY)), 1))
            ).label("qualified_shows"),
            # C2 metrics
            func.count(case((is_2nd, 1))).label("calls_booked_2nd"),
            func.count(case((and_(is_2nd, showed_2nd), 1))).label("shows_2nd"),
            # Close
            func.count(
                case((
                    or_(
                        and_(is_1st, showed_1st),
                        and_(is_2nd, showed_2nd),
                    ),
                    1,
                ))
            ).label("total_shows"),
            func.count(case((is_won, 1))).label("units_closed"),
            # Avg deal cycle (days from contact_created_at to close_date) — won opps only
            func.avg(
                case((
                    and_(
                        is_won,
                        Opportunity.close_date.isnot(None),
                        Opportunity.contact_created_at.isnot(None),
                    ),
                    func.extract(
                        "epoch",
                        Opportunity.close_date - Opportunity.contact_created_at,
                    ) / 86400.0,
                ))
            ).label("avg_cycle_days"),
        )
        .where(bf)
        .group_by(group_col)
        .order_by(func.count(case((is_1st, 1))).desc())
    )

    def safe_rate(n: int, d: int) -> float | None:
        return round(n / d, 4) if d else None

    rows = []
    for row in result.all():
        rows.append({
            "segment": row.segment,
            "calls_booked_1st": row.calls_booked_1st,
            "shows_1st": row.shows_1st,
            "show_rate_1st": safe_rate(row.shows_1st, row.bookable_1st),
            "qual_rate": safe_rate(row.qualified_shows, row.shows_1st),
            "calls_booked_2nd": row.calls_booked_2nd,
            "shows_2nd": row.shows_2nd,
            "show_rate_2nd": safe_rate(row.shows_2nd, row.calls_booked_2nd),
            "total_shows": row.total_shows,
            "units_closed": row.units_closed,
            "close_rate": safe_rate(row.units_closed, row.total_shows),
            "avg_cycle_days": round(float(row.avg_cycle_days), 1) if row.avg_cycle_days is not None else None,
            # Financial — null until CFO provides data
            "contract_value": None,
            "cash_collected": None,
        })

    return {"dimension_label": dimension_label, "rows": rows}
