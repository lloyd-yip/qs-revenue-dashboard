"""Shared SQL fragments and constants used across metric queries."""

from datetime import date

from sqlalchemy import and_, case, func, or_

from db.models import Opportunity
from sync.ghl_client import (
    DEAL_WON_STAGE_ID,
    DISQUALIFIED_STAGE_ID,
    SHOWED_STAGE_IDS,
)

# Pipeline stage IDs where the prospect showed (used as secondary show signal)
SHOWED_STAGE_IDS_LIST = list(SHOWED_STAGE_IDS)

# Lead Quality values that count as "qualified"
QUALIFIED_LEAD_QUALITY = ("Great", "Ok", "Barely Passable")


def date_filter(start: date, end: date, date_by: str):
    """Return a SQLAlchemy WHERE clause for the chosen date dimension.

    date_by: 'created' → filter on created_at_ghl
             'appointment' → filter on call1_appointment_date OR call2_appointment_date
    """
    if date_by == "created":
        col = Opportunity.created_at_ghl
        return and_(func.date(col) >= start, func.date(col) <= end)
    else:
        c1 = Opportunity.call1_appointment_date
        c2 = Opportunity.call2_appointment_date
        return or_(
            and_(func.date(c1) >= start, func.date(c1) <= end),
            and_(func.date(c2) >= start, func.date(c2) <= end),
        )


def base_filter(start: date, end: date, date_by: str, rep_id: str | None = None):
    """Base filter: not excluded + date range + optional rep filter."""
    filters = [
        Opportunity.is_excluded.is_(False),
        date_filter(start, end, date_by),
    ]
    if rep_id:
        filters.append(Opportunity.opportunity_owner_id == rep_id)
    return and_(*filters)


# Minimum word count for a post-call note to be considered compliant
NOTE_MIN_WORDS = 50


def rep_non_compliance_expr():
    """SQLAlchemy boolean expression: opp has at least one compliance violation.

    TRUE if any of:
    1. outcome_unfilled — appointment passed 12h, status never updated
    2. Showed + lead_quality not filled
    3. Showed + post_call_note_word_count below threshold (or 0 = no note found)
    """
    showed = showed_1st_call_expr()
    return or_(
        Opportunity.outcome_unfilled.is_(True),
        and_(showed, Opportunity.lead_quality.is_(None)),
        and_(
            showed,
            Opportunity.post_call_note_word_count.isnot(None),  # notes were checked
            Opportunity.post_call_note_word_count < NOTE_MIN_WORDS,
        ),
    )


def has_1st_call(start: date, end: date, date_by: str):
    """Opportunity had a 1st call within the relevant scope.

    appointment mode → call1_appointment_date falls in date range
    created mode     → call1_appointment_date exists (opp was created in range via base_filter)
    """
    if date_by == "appointment":
        c1 = Opportunity.call1_appointment_date
        return and_(c1.isnot(None), func.date(c1) >= start, func.date(c1) <= end)
    return Opportunity.call1_appointment_date.isnot(None)


def has_2nd_call(start: date, end: date, date_by: str):
    """Opportunity had a 2nd call within the relevant scope."""
    if date_by == "appointment":
        c2 = Opportunity.call2_appointment_date
        return and_(c2.isnot(None), func.date(c2) >= start, func.date(c2) <= end)
    return Opportunity.call2_appointment_date.isnot(None)


def showed_1st_call_expr():
    """Boolean expression: opportunity showed on 1st call (either signal)."""
    return or_(
        Opportunity.call1_appointment_status == "Showed",
        Opportunity.pipeline_stage_id.in_(SHOWED_STAGE_IDS_LIST),
    )


def showed_2nd_call_expr():
    """Boolean expression: opportunity showed on 2nd call (either signal)."""
    return or_(
        Opportunity.call2_appointment_status == "Showed",
        Opportunity.pipeline_stage_id == "10e6b1ef-0685-4f73-b3c7-b5006b7bc311",  # 2nd Call Done
    )
