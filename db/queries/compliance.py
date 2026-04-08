"""Compliance failure detail queries."""

from datetime import date, timedelta

from sqlalchemy import and_, case, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity
from db.queries.common import NOTE_MIN_WORDS, rep_non_compliance_expr, sales_rep_filter, showed_1st_call_expr

GRACE_HOURS = 12  # hours after appointment before outcome_unfilled is flagged


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
                        Opportunity.post_call_note_word_count == 0,
                    ),
                    1,
                ))
            ).label("no_note_count"),
            func.count(
                case((
                    and_(
                        showed,
                        Opportunity.post_call_note_word_count.isnot(None),
                        Opportunity.post_call_note_word_count > 0,
                        Opportunity.post_call_note_word_count < NOTE_MIN_WORDS,
                    ),
                    1,
                ))
            ).label("low_fidelity_note_count"),
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
        "no_note_count": row.no_note_count,
        "low_fidelity_note_count": row.low_fidelity_note_count,
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
        .where(and_(bf, sales_rep_filter()))
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
            Opportunity.opportunity_name,
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
        # Qual and note violations only apply to opps that actually showed —
        # no-shows don't need qual fields filled or a post-call note
        actually_showed = row.call1_appointment_status == "Showed"
        if actually_showed and row.lead_quality is None:
            violations.append("Qual fields empty")
        if actually_showed and row.post_call_note_word_count is not None and row.post_call_note_word_count < NOTE_MIN_WORDS:
            wc = row.post_call_note_word_count
            violations.append(f"Note lacks fidelity ({wc} words)" if wc > 0 else "No qualifying note found")

        rows.append({
            "ghl_opportunity_id": row.ghl_opportunity_id,
            "opportunity_name": row.opportunity_name or None,
            "rep_name": row.opportunity_owner_name or "Unassigned",
            "stage_name": row.pipeline_stage_name or "Unknown",
            "call1_appointment_date": (
                row.call1_appointment_date.isoformat() if row.call1_appointment_date else None
            ),
            "call1_appointment_status": row.call1_appointment_status or "Not Set",
            "violations": ", ".join(violations),
        })

    return rows


async def get_rep_late_rates(
    session: AsyncSession,
) -> list[dict]:
    """Per-rep late-logging rate — how often reps take >12h to log call outcomes.

    Uses outcome_unfilled_first_flagged_at (ever flagged) and
    outcome_unfilled_resolved_at (when resolved) to compute:
      - total_flagged: opps that were ever flagged
      - resolved_late: opps resolved more than GRACE_HOURS after appointment
      - late_rate: resolved_late / total_flagged
      - avg_hours_late: average delay beyond the grace deadline
    """
    result = await session.execute(
        select(
            Opportunity.opportunity_owner_name.label("rep_name"),
            func.count(
                case((Opportunity.outcome_unfilled_first_flagged_at.isnot(None), 1))
            ).label("total_flagged"),
            func.count(
                case((
                    and_(
                        Opportunity.outcome_unfilled_resolved_at.isnot(None),
                        Opportunity.call1_appointment_date.isnot(None),
                        # resolved more than GRACE_HOURS after appointment (using epoch seconds)
                        func.extract(
                            "epoch",
                            Opportunity.outcome_unfilled_resolved_at - Opportunity.call1_appointment_date,
                        ) > GRACE_HOURS * 3600,
                    ),
                    1,
                ))
            ).label("resolved_late"),
            func.avg(
                case((
                    and_(
                        Opportunity.outcome_unfilled_resolved_at.isnot(None),
                        Opportunity.call1_appointment_date.isnot(None),
                    ),
                    func.extract(
                        "epoch",
                        Opportunity.outcome_unfilled_resolved_at
                        - Opportunity.call1_appointment_date
                    ) / 3600.0 - GRACE_HOURS,
                ))
            ).label("avg_hours_late"),
        )
        .where(and_(
            Opportunity.outcome_unfilled_first_flagged_at.isnot(None),
            sales_rep_filter(),
        ))
        .group_by(Opportunity.opportunity_owner_name)
        .order_by(func.count(
            case((Opportunity.outcome_unfilled_first_flagged_at.isnot(None), 1))
        ).desc())
    )

    rows = []
    for row in result.all():
        total = row.total_flagged or 0
        late = row.resolved_late or 0
        rows.append({
            "rep_name": row.rep_name or "Unassigned",
            "total_flagged": total,
            "resolved_late": late,
            "late_rate": round(late / total, 4) if total else None,
            "avg_hours_late": round(float(row.avg_hours_late), 1) if row.avg_hours_late else None,
        })
    return rows


async def get_rep_late_violations(
    session: AsyncSession,
    rep_name: str | None = None,
) -> list[dict]:
    """Individual opp rows for the late-violation drill-down modal."""
    conditions = [Opportunity.outcome_unfilled_first_flagged_at.isnot(None)]
    if rep_name:
        conditions.append(Opportunity.opportunity_owner_name == rep_name)

    result = await session.execute(
        select(
            Opportunity.opportunity_name,
            Opportunity.ghl_opportunity_id,
            Opportunity.opportunity_owner_name,
            Opportunity.call1_appointment_date,
            Opportunity.outcome_unfilled_first_flagged_at,
            Opportunity.outcome_unfilled_resolved_at,
        )
        .where(and_(*conditions))
        .order_by(Opportunity.outcome_unfilled_first_flagged_at.desc())
    )

    rows = []
    for row in result.all():
        hours_late = None
        if row.outcome_unfilled_resolved_at and row.call1_appointment_date:
            delta = (row.outcome_unfilled_resolved_at - row.call1_appointment_date).total_seconds() / 3600
            hours_late = round(max(0.0, delta - GRACE_HOURS), 1)
        rows.append({
            "opportunity_name": row.opportunity_name,
            "ghl_opportunity_id": row.ghl_opportunity_id,
            "rep_name": row.opportunity_owner_name or "Unassigned",
            "appt_date": row.call1_appointment_date.strftime("%b %d, %Y") if row.call1_appointment_date else "—",
            "first_flagged_at": row.outcome_unfilled_first_flagged_at.strftime("%b %d %H:%M UTC") if row.outcome_unfilled_first_flagged_at else "—",
            "resolved_at": row.outcome_unfilled_resolved_at.strftime("%b %d %H:%M UTC") if row.outcome_unfilled_resolved_at else "Still unresolved",
            "hours_late": hours_late,
        })
    return rows
