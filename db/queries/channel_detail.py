"""Per-channel detail — manager view of one lead channel (funnel, quality, revenue, ROI).

Composes the existing channel breakdown (reused, not duplicated) with a per-rep split
scoped to the channel and the manual channel cost, and derives the funnel/ROI ratios a
marketing/sales manager needs to judge a channel's performance and value.
"""

from datetime import date

from sqlalchemy import and_, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import DealWhopMatch, Opportunity
from db.queries.channel_cost import get_channel_cost
from db.queries.common import (
    QUALIFIED_LEAD_QUALITY,
    base_filter,
    has_1st_call,
    showed_1st_call_expr,
    whop_projected_total_expr,
)
from db.queries.lead_source import get_lead_source_breakdown


def _safe_rate(num, den):
    return round(num / den, 4) if den else None


def _channel_filter(channel: str):
    if channel == "Unknown":
        return Opportunity.canonical_channel.is_(None)
    return Opportunity.canonical_channel == channel


async def _rep_breakdown(
    session: AsyncSession, channel: str, start: date, end: date, date_by: str
) -> list[dict]:
    """Per-rep funnel + cash for one channel — mirrors the channel breakdown, grouped by rep."""
    from sync.ghl_client import DEAL_WON_STAGE_ID

    bf = base_filter(start, end, date_by)
    is_1st = has_1st_call(start, end, date_by)
    showed_1st = showed_1st_call_expr()
    won = Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID

    result = await session.execute(
        select(
            func.coalesce(Opportunity.opportunity_owner_name, "Unassigned").label("rep"),
            func.count(Opportunity.id).label("total_ops"),
            func.count(case((and_(is_1st, showed_1st), 1))).label("shows"),
            func.count(case((and_(is_1st, showed_1st,
                                  Opportunity.lead_quality.in_(QUALIFIED_LEAD_QUALITY)), 1))).label("qualified"),
            func.count(case((and_(is_1st, won), 1))).label("units_closed"),
            func.coalesce(func.sum(case((and_(is_1st, won), DealWhopMatch.total_paid))), 0).label("cash_collected"),
            func.coalesce(func.sum(case((and_(is_1st, won), whop_projected_total_expr()))), 0).label("whop_projected_total"),
        )
        .select_from(Opportunity)
        .outerjoin(DealWhopMatch, Opportunity.ghl_opportunity_id == DealWhopMatch.ghl_opportunity_id)
        .where(and_(bf, _channel_filter(channel)))
        .group_by(Opportunity.opportunity_owner_name)
        .order_by(func.count(Opportunity.id).desc())
    )

    return [
        {
            "rep": row.rep,
            "total_ops": row.total_ops,
            "shows": row.shows,
            "qualified": row.qualified,
            "units_closed": row.units_closed,
            "cash_collected": float(row.cash_collected),
            "whop_projected_total": round(float(row.whop_projected_total), 2),
        }
        for row in result.all()
    ]


async def get_channel_detail(
    session: AsyncSession, channel: str, start: date, end: date, date_by: str
) -> dict:
    """Full manager view for one channel: headline funnel/revenue, ROI, per-rep split."""
    # Headline — reuse the exact aggregation the channel table uses, pick this channel's row.
    rows = await get_lead_source_breakdown(session, start, end, date_by, group_by="channel")
    row = next((r for r in rows if r["channel"] == channel), None)
    if row is None:
        row = {
            "channel": channel, "total_ops": 0, "shows": 0, "units_closed": 0,
            "contract_value": 0.0, "cash_collected": 0.0, "whop_projected_total": 0.0,
            "qual_rate": None, "dq_rate": None, "great_count": 0, "ok_count": 0,
            "barely_passable_count": 0, "bad_count": 0, "missing_data_count": 0,
            "projected_contract_value": 0.0,
        }

    shows = row["shows"]
    closed = row["units_closed"]
    qualified = row["great_count"] + row["ok_count"] + row["barely_passable_count"]
    cash = row["cash_collected"]
    projected = row["whop_projected_total"]

    headline = {
        **row,
        "qualified": qualified,
        "show_rate": _safe_rate(shows, row["total_ops"]),
        "close_rate": _safe_rate(closed, shows),
        "avg_deal_size": round(projected / closed, 2) if closed else None,
    }

    # ROI — only when a cost is set for this exact range.
    cost = await get_channel_cost(session, channel, start, end)
    roi = {
        "cost": cost,
        "is_set": cost is not None,
        "roi": round(cash / cost, 2) if cost else None,
        "contract_roi": round(projected / cost, 2) if cost else None,
        "cost_per_show": round(cost / shows, 2) if cost and shows else None,
        "cost_per_qualified": round(cost / qualified, 2) if cost and qualified else None,
        "cost_per_close": round(cost / closed, 2) if cost and closed else None,
    }

    reps = await _rep_breakdown(session, channel, start, end, date_by)

    return {"channel": channel, "headline": headline, "roi": roi, "reps": reps}
