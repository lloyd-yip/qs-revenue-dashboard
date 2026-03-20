"""Lead source / channel attribution queries."""

from datetime import date

from datetime import datetime

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity
from db.queries.common import QUALIFIED_LEAD_QUALITY, base_filter, has_1st_call, showed_1st_call_expr


async def get_lead_source_breakdown(
    session: AsyncSession,
    start: date,
    end: date,
    date_by: str,
    rep_id: str | None = None,
) -> list[dict]:
    """Attribution breakdown by canonical_channel.

    Returns counts for: total ops, shows, units closed, projected value,
    qual_rate, dq_rate. Sorted by total ops descending.
    """
    from sync.ghl_client import DEAL_WON_STAGE_ID, DISQUALIFIED_STAGE_ID

    bf = base_filter(start, end, date_by, rep_id)
    is_1st = has_1st_call(start, end, date_by)
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
            # Qual/DQ per channel (1st call shows only)
            func.count(
                case((and_(is_1st, showed_1st, ~Opportunity.outcome_unfilled), 1))
            ).label("shows_1st"),
            func.count(
                case((
                    and_(is_1st, showed_1st, Opportunity.lead_quality.in_(QUALIFIED_LEAD_QUALITY)),
                    1,
                ))
            ).label("qualified_shows"),
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
            # Lead quality breakdown counts (1st call shows only)
            func.count(
                case((and_(is_1st, showed_1st, Opportunity.lead_quality == "Great"), 1))
            ).label("great_count"),
            func.count(
                case((and_(is_1st, showed_1st, Opportunity.lead_quality == "Ok"), 1))
            ).label("ok_count"),
            func.count(
                case((and_(is_1st, showed_1st, Opportunity.lead_quality == "Barely Passable"), 1))
            ).label("barely_passable_count"),
            func.count(
                case((and_(is_1st, showed_1st, Opportunity.lead_quality == "Bad"), 1))
            ).label("bad_count"),
            func.count(
                case((and_(is_1st, showed_1st, Opportunity.lead_quality.is_(None)), 1))
            ).label("missing_data_count"),
        )
        .where(bf)
        .group_by(Opportunity.canonical_channel)
        .order_by(func.count(Opportunity.id).desc())
    )

    def safe_rate(num: int, den: int) -> float | None:
        return round(num / den, 4) if den else None

    return [
        {
            "channel": row.channel or "Unknown",
            "total_ops": row.total_ops,
            "shows": row.shows,
            "units_closed": row.units_closed,
            "projected_contract_value": float(row.projected_contract_value),
            "qual_rate": safe_rate(row.qualified_shows, row.shows_1st),
            "dq_rate": safe_rate(row.dq_count, row.shows_1st),
            "great_count": row.great_count,
            "ok_count": row.ok_count,
            "barely_passable_count": row.barely_passable_count,
            "bad_count": row.bad_count,
            "missing_data_count": row.missing_data_count,
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
            "great": row.great,
            "ok": row.ok,
            "barely_passable": row.barely_passable,
            "bad": row.bad,
            "dq": row.dq,
            "not_set": row.not_set,
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


async def get_channel_closes(
    session: AsyncSession,
    channel: str,
    start: date,
    end: date,
    date_by: str,
) -> list[dict]:
    """Closed deals for a specific channel — for the drill-down popup.

    Returns: opportunity name, rep name, close date (updated_at_ghl), deal value.
    Ordered by close date descending.
    """
    from sync.ghl_client import DEAL_WON_STAGE_ID

    bf = base_filter(start, end, date_by)
    channel_filter = (
        Opportunity.canonical_channel == channel
        if channel != "Unknown"
        else Opportunity.canonical_channel.is_(None)
    )

    result = await session.execute(
        select(
            Opportunity.opportunity_name,
            Opportunity.opportunity_owner_name,
            Opportunity.updated_at_ghl,
            Opportunity.monetary_value,
        )
        .where(
            and_(
                bf,
                channel_filter,
                Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID,
            )
        )
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
