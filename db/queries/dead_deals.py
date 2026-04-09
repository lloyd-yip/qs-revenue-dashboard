"""Dead Deals queries — DQ'd and Lost opportunities.

Dead = lead_quality == 'DQ' (disqualified after call)
     OR pipeline_stage_id == DEAL_LOST_STAGE_ID (lost at close)
"""

from datetime import date

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity
from db.queries.common import base_filter, has_1st_call, has_2nd_call, showed_1st_call_expr, showed_2nd_call_expr
from sync.ghl_client import DEAL_LOST_STAGE_ID, DISQUALIFIED_STAGE_ID


def _is_dq():
    # Reps disqualify via pipeline stage — lead_quality='DQ' is never set in practice
    return or_(
        Opportunity.lead_quality == "DQ",
        Opportunity.pipeline_stage_id == DISQUALIFIED_STAGE_ID,
    )


def _is_lost():
    return Opportunity.pipeline_stage_id == DEAL_LOST_STAGE_ID


def _is_dead():
    return or_(_is_dq(), _is_lost())


def _safe_rate(n: int, d: int) -> float | None:
    return round(n / d, 4) if d else None


async def get_dead_deals_data(
    session: AsyncSession,
    start: date,
    end: date,
    date_by: str,
    rep_id: str | None = None,
) -> dict:
    """All data for the Dead Deals tab in one pass.

    Returns: summary KPIs, dq_reasons, lost_reasons, by_rep, by_channel, opp_list.
    """
    bf = base_filter(start, end, date_by, rep_id)
    is_1st = has_1st_call(start, end, date_by)
    is_2nd = has_2nd_call(start, end, date_by)
    showed_1st = showed_1st_call_expr()
    showed_2nd = showed_2nd_call_expr()

    # ── Summary KPIs ──────────────────────────────────────────────────────────
    summary_res = await session.execute(
        select(
            func.count(case((_is_dq(), 1))).label("dq_count"),
            func.count(case((_is_lost(), 1))).label("lost_count"),
            func.count(case((and_(is_1st, showed_1st), 1))).label("total_c1_shows"),
            func.count(case((and_(is_2nd, showed_2nd), 1))).label("total_c2_shows"),
        ).where(bf)
    )
    s = summary_res.one()

    summary = {
        "dq_count": s.dq_count,
        "lost_count": s.lost_count,
        "total_dead": s.dq_count + s.lost_count,
        "dq_rate": _safe_rate(s.dq_count, s.total_c1_shows),
    }

    # ── DQ Reasons ────────────────────────────────────────────────────────────
    dq_reasons_res = await session.execute(
        select(
            func.coalesce(Opportunity.dq_reason, "(Not Set)").label("reason"),
            func.count(Opportunity.id).label("count"),
        )
        .where(and_(bf, _is_dq()))
        .group_by(Opportunity.dq_reason)
        .order_by(func.count(Opportunity.id).desc())
    )
    total_dq = s.dq_count or 1
    dq_reasons = [
        {"reason": r.reason, "count": r.count, "pct": round(r.count / total_dq, 4)}
        for r in dq_reasons_res.all()
    ]

    # ── Lost Reasons ──────────────────────────────────────────────────────────
    lost_reasons_res = await session.execute(
        select(
            func.coalesce(Opportunity.deal_lost_reasons, "(Not Set)").label("reason"),
            func.count(Opportunity.id).label("count"),
        )
        .where(and_(bf, _is_lost()))
        .group_by(Opportunity.deal_lost_reasons)
        .order_by(func.count(Opportunity.id).desc())
    )
    total_lost = s.lost_count or 1
    lost_reasons = [
        {"reason": r.reason, "count": r.count, "pct": round(r.count / total_lost, 4)}
        for r in lost_reasons_res.all()
    ]

    # ── By Rep ────────────────────────────────────────────────────────────────
    by_rep_res = await session.execute(
        select(
            func.coalesce(Opportunity.opportunity_owner_name, "Unassigned").label("rep"),
            func.count(case((_is_dq(), 1))).label("dq_count"),
            func.count(case((_is_lost(), 1))).label("lost_count"),
        )
        .where(and_(bf, _is_dead()))
        .group_by(Opportunity.opportunity_owner_name)
        .order_by(func.count(Opportunity.id).desc())
    )
    by_rep = [
        {
            "rep": r.rep,
            "dq_count": r.dq_count,
            "lost_count": r.lost_count,
            "total_dead": r.dq_count + r.lost_count,
        }
        for r in by_rep_res.all()
    ]

    # ── By Channel ────────────────────────────────────────────────────────────
    by_channel_res = await session.execute(
        select(
            func.coalesce(Opportunity.canonical_channel, "Unknown").label("channel"),
            func.count(case((_is_dq(), 1))).label("dq_count"),
            func.count(case((_is_lost(), 1))).label("lost_count"),
        )
        .where(and_(bf, _is_dead()))
        .group_by(Opportunity.canonical_channel)
        .order_by(func.count(Opportunity.id).desc())
    )
    by_channel = [
        {
            "channel": r.channel,
            "dq_count": r.dq_count,
            "lost_count": r.lost_count,
            "total_dead": r.dq_count + r.lost_count,
        }
        for r in by_channel_res.all()
    ]

    # ── Individual Opp List ───────────────────────────────────────────────────
    list_res = await session.execute(
        select(
            Opportunity.opportunity_name,
            Opportunity.opportunity_owner_name,
            Opportunity.canonical_channel,
            Opportunity.lead_quality,
            Opportunity.dq_reason,
            Opportunity.deal_lost_reasons,
            Opportunity.business_industry,
            Opportunity.monetary_value,
            Opportunity.close_date,
            Opportunity.updated_at_ghl,
            Opportunity.ghl_opportunity_id,
        )
        .where(and_(bf, _is_dead()))
        .order_by(Opportunity.updated_at_ghl.desc())
        .limit(300)
    )

    def _opp_row(r) -> dict:
        is_dq = r.lead_quality == "DQ"
        dt = r.close_date or r.updated_at_ghl
        return {
            "name": r.opportunity_name or "—",
            "rep": r.opportunity_owner_name or "Unassigned",
            "channel": r.canonical_channel or "Unknown",
            "industry": r.business_industry or "—",
            "type": "DQ" if is_dq else "Lost",
            "reason": (r.dq_reason if is_dq else r.deal_lost_reasons) or "—",
            "value": float(r.monetary_value) if r.monetary_value else None,
            "date": dt.strftime("%b %d, %Y") if dt else "—",
            "ghl_id": r.ghl_opportunity_id,
        }

    opp_list = [_opp_row(r) for r in list_res.all()]

    return {
        "summary": summary,
        "dq_reasons": dq_reasons,
        "lost_reasons": lost_reasons,
        "by_rep": by_rep,
        "by_channel": by_channel,
        "opp_list": opp_list,
    }
