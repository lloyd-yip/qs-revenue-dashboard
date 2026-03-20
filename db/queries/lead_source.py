"""Lead source / channel attribution queries."""

from datetime import date

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity
from db.queries.common import base_filter, has_1st_call, showed_1st_call_expr


async def get_lead_source_breakdown(
    session: AsyncSession,
    start: date,
    end: date,
    date_by: str,
    rep_id: str | None = None,
) -> list[dict]:
    """Attribution breakdown by canonical_channel.

    Returns counts for: total ops, shows, units closed, projected value.
    Sorted by total ops descending.
    """
    from sync.ghl_client import DEAL_WON_STAGE_ID

    bf = base_filter(start, end, date_by, rep_id)
    showed_1st = showed_1st_call_expr()

    result = await session.execute(
        select(
            func.coalesce(Opportunity.canonical_channel, "Unknown").label("channel"),
            func.count(Opportunity.id).label("total_ops"),
            func.count(case((showed_1st, 1))).label("shows"),
            func.count(
                case((Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID, 1))
            ).label("units_closed"),
            func.coalesce(
                func.sum(
                    case((
                        Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID,
                        Opportunity.monetary_value,
                    ))
                ),
                0,
            ).label("projected_contract_value"),
        )
        .where(bf)
        .group_by(Opportunity.canonical_channel)
        .order_by(func.count(Opportunity.id).desc())
    )

    return [
        {
            "channel": row.channel or "Unknown",
            "total_ops": row.total_ops,
            "shows": row.shows,
            "units_closed": row.units_closed,
            "projected_contract_value": float(row.projected_contract_value),
        }
        for row in result.all()
    ]


async def get_channel_quality_breakdown(
    session: AsyncSession,
    start: date,
    end: date,
    date_by: str,
    rep_id: str | None = None,
) -> list[dict]:
    """Lead quality distribution per canonical channel — for the grouped bar chart."""
    bf = base_filter(start, end, date_by, rep_id)

    result = await session.execute(
        select(
            func.coalesce(Opportunity.canonical_channel, "Unknown").label("channel"),
            func.count(case((Opportunity.lead_quality == "Great", 1))).label("great"),
            func.count(case((Opportunity.lead_quality == "Ok", 1))).label("ok"),
            func.count(case((Opportunity.lead_quality == "Barely Passable", 1))).label("barely_passable"),
            func.count(case((Opportunity.lead_quality == "Bad", 1))).label("bad"),
            func.count(case((Opportunity.lead_quality == "DQ", 1))).label("dq"),
            func.count(case((Opportunity.lead_quality.is_(None), 1))).label("not_set"),
            func.count(Opportunity.id).label("total"),
        )
        .where(bf)
        .group_by(Opportunity.canonical_channel)
        .order_by(func.count(Opportunity.id).desc())
    )

    return [
        {
            "channel": row.channel or "Unknown",
            "Great": row.great,
            "Ok": row.ok,
            "Barely Passable": row.barely_passable,
            "Bad": row.bad,
            "DQ": row.dq,
            "Not Set": row.not_set,
            "total": row.total,
        }
        for row in result.all()
    ]


async def get_qualification_breakdown(
    session: AsyncSession,
    start: date,
    end: date,
    date_by: str,
    rep_id: str | None = None,
) -> dict:
    """Breakdown of qualification custom fields across 1st call shows."""
    from sqlalchemy import and_

    bf = base_filter(start, end, date_by, rep_id)
    showed_1st = showed_1st_call_expr()
    is_1st = has_1st_call(start, end, date_by)
    showed_1st_filter = and_(bf, is_1st, showed_1st)

    async def field_breakdown(field_col):
        res = await session.execute(
            select(
                func.coalesce(field_col, "Not Set").label("value"),
                func.count(Opportunity.id).label("count"),
            )
            .where(showed_1st_filter)
            .group_by(field_col)
            .order_by(func.count(Opportunity.id).desc())
        )
        return [{"value": r.value or "Not Set", "count": r.count} for r in res.all()]

    return {
        "lead_quality": await field_breakdown(Opportunity.lead_quality),
        "financial_qual": await field_breakdown(Opportunity.financial_qual),
        "intent_to_transform": await field_breakdown(Opportunity.intent_to_transform),
        "pre_call_indoctrination": await field_breakdown(Opportunity.pre_call_indoctrination),
        "business_fit": await field_breakdown(Opportunity.business_fit),
        "pain_goal_oriented": await field_breakdown(Opportunity.pain_goal_oriented),
        "business_industry": await field_breakdown(Opportunity.business_industry),
        "current_revenue": await field_breakdown(Opportunity.current_revenue),
        "dq_reason": await field_breakdown(Opportunity.dq_reason),
        "deal_lost_reasons": await field_breakdown(Opportunity.deal_lost_reasons),
    }
