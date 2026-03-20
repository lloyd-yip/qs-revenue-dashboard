"""Compliance failure detail queries."""

from datetime import date

from sqlalchemy import and_, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity
from db.queries.common import NOTE_MIN_WORDS, rep_non_compliance_expr, showed_1st_call_expr


async def get_compliance_summary(
    session: AsyncSession,
    start: date,
    end: date,
    rep_id: str | None = None,
) -> dict:
    """Aggregate compliance counts for the summary cards.

    Returns:
        outcome_unfilled_count  — appointments where rep never logged outcome
        outcome_unfilled_rate   — as % of total 1st call booked opps in period
        non_compliance_count    — opps with any compliance violation (binary)
        non_compliance_rate     — as % of total opps in period
        note_missing_count      — showed opps with no qualifying post-call note
        qual_missing_count      — showed opps with lead_quality not filled
    """
    from db.queries.common import base_filter, has_1st_call

    bf = base_filter(start, end, "appointment", rep_id)
    is_1st = has_1st_call(start, end, "appointment")
    showed = showed_1st_call_expr()
    non_compliance = rep_non_compliance_expr()

    result = await session.execute(
        select(
            func.count(Opportunity.id).label("total_opps"),
            func.count(case((is_1st, 1))).label("total_booked"),
            func.count(case((Opportunity.outcome_unfilled.is_(True), 1))).label("outcome_unfilled_count"),
            func.count(case((non_compliance, 1))).label("non_compliance_count"),
            func.count(
                case((
                    and_(
                        showed,
                        Opportunity.post_call_note_word_count.isnot(None),
                        Opportunity.post_call_note_word_count < NOTE_MIN_WORDS,
                    ),
                    1,
                ))
            ).label("note_missing_count"),
            func.count(
                case((and_(showed, Opportunity.lead_quality.is_(None)), 1))
            ).label("qual_missing_count"),
        ).where(bf)
    )
    row = result.one()

    def safe_rate(num: int, den: int) -> float | None:
        return round(num / den, 4) if den else None

    return {
        "outcome_unfilled_count": row.outcome_unfilled_count,
        "outcome_unfilled_rate": safe_rate(row.outcome_unfilled_count, row.total_booked),
        "non_compliance_count": row.non_compliance_count,
        "non_compliance_rate": safe_rate(row.non_compliance_count, row.total_opps),
        "note_missing_count": row.note_missing_count,
        "qual_missing_count": row.qual_missing_count,
    }


async def get_compliance_by_rep(
    session: AsyncSession,
    start: date,
    end: date,
) -> list[dict]:
    """Per-rep compliance counts — for the bar chart, sorted worst first."""
    from db.queries.common import base_filter

    bf = base_filter(start, end, "appointment")
    non_compliance = rep_non_compliance_expr()

    result = await session.execute(
        select(
            Opportunity.opportunity_owner_name.label("rep_name"),
            func.count(case((Opportunity.outcome_unfilled.is_(True), 1))).label("outcome_unfilled"),
            func.count(case((non_compliance, 1))).label("non_compliance"),
        )
        .where(bf)
        .group_by(Opportunity.opportunity_owner_name)
        .order_by(func.count(case((non_compliance, 1))).desc())
    )

    return [
        {
            "rep_name": row.rep_name or "Unassigned",
            "outcome_unfilled": row.outcome_unfilled,
            "non_compliance": row.non_compliance,
        }
        for row in result.all()
    ]


async def get_compliance_failures(
    session: AsyncSession,
    start: date,
    end: date,
    rep_id: str | None = None,
) -> list[dict]:
    """Individual failure rows for the Tabulator detail table.

    Returns opps with any compliance violation, ordered by appointment date desc.
    Each row includes the GHL opportunity ID for the direct link.
    """
    from db.queries.common import base_filter

    bf = base_filter(start, end, "appointment", rep_id)
    non_compliance = rep_non_compliance_expr()
    showed = showed_1st_call_expr()

    result = await session.execute(
        select(
            Opportunity.ghl_opportunity_id,
            Opportunity.opportunity_owner_name,
            Opportunity.pipeline_stage_name,
            Opportunity.call1_appointment_date,
            Opportunity.call1_appointment_status,
            Opportunity.lead_quality,
            Opportunity.post_call_note_word_count,
            Opportunity.outcome_unfilled,
        )
        .where(and_(bf, non_compliance))
        .order_by(Opportunity.call1_appointment_date.desc())
    )

    rows = []
    for row in result.all():
        # Determine which violations this opp has for the detail table
        violations = []
        if row.outcome_unfilled:
            violations.append("Outcome not logged")
        if row.lead_quality is None:
            violations.append("Qual fields empty")
        if row.post_call_note_word_count is not None and row.post_call_note_word_count < NOTE_MIN_WORDS:
            wc = row.post_call_note_word_count
            violations.append(f"Note too short ({wc} words)" if wc > 0 else "No post-call note")

        rows.append({
            "ghl_opportunity_id": row.ghl_opportunity_id,
            "rep_name": row.opportunity_owner_name or "Unassigned",
            "stage_name": row.pipeline_stage_name or "Unknown",
            "call1_appointment_date": (
                row.call1_appointment_date.isoformat() if row.call1_appointment_date else None
            ),
            "call1_appointment_status": row.call1_appointment_status or "Not Set",
            "violations": ", ".join(violations),
        })

    return rows
