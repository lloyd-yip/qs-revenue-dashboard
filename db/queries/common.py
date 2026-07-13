"""Shared SQL fragments and constants used across metric queries."""

from datetime import date

from sqlalchemy import and_, case, func, or_

from config import _DB_OTHER_NAMES
from db.models import Opportunity
from sync.ghl_client import (
    DEAL_WON_STAGE_ID,
    DISQUALIFIED_STAGE_ID,
    SHOWED_STAGE_IDS,
)

def whop_projected_total_expr():
    """Payment-verified projected full contract value for one matched deal.

    Splitit/ClarityPay settle 100% upfront, so total_paid IS the full contract.
    Internal payment plans only record collected installments, so project
    avg installment × total_installments (the membership's authoritative
    split_pay_required_payments). Falls back to total_paid (pay-in-full, or
    plan length unknown). Assumes roughly equal installments.
    """
    from db.models import DealWhopMatch

    return case(
        (
            or_(DealWhopMatch.is_splitit.is_(True), DealWhopMatch.is_claritypay.is_(True)),
            DealWhopMatch.total_paid,
        ),
        (
            and_(
                DealWhopMatch.total_installments.isnot(None),
                DealWhopMatch.total_installments > 0,
                DealWhopMatch.payment_count.isnot(None),
                DealWhopMatch.payment_count > 0,
            ),
            DealWhopMatch.total_paid / DealWhopMatch.payment_count * DealWhopMatch.total_installments,
        ),
        else_=DealWhopMatch.total_paid,
    )


def prorated_expense_amount(start: date, end: date):
    """ExpenseLineItem.amount prorated by the fraction of days its period overlaps [start,end].

    A period fully inside the range contributes in full; a period that only partially
    overlaps (e.g. a whole calendar month when the selected range is 'Last 7d' or a
    mid-month custom range) contributes proportionally to the overlapping day count.
    Use inside func.sum(...). Postgres numeric ÷ int keeps precision (no truncation).
    """
    from db.models import ExpenseLineItem

    overlap_days = (
        func.least(ExpenseLineItem.period_end, end)
        - func.greatest(ExpenseLineItem.period_start, start)
        + 1
    )
    period_days = ExpenseLineItem.period_end - ExpenseLineItem.period_start + 1
    return ExpenseLineItem.amount * overlap_days / period_days


# Pipeline stage IDs where the prospect showed (used as secondary show signal)
SHOWED_STAGE_IDS_LIST = list(SHOWED_STAGE_IDS)

# Lead Quality values that count as "qualified"
QUALIFIED_LEAD_QUALITY = ("Great", "Ok", "Barely Passable")


def date_filter(start: date, end: date, date_by: str):
    """Return a SQLAlchemy WHERE clause for the chosen date dimension.

    date_by: 'appointment' → call1_appointment_date OR call2_appointment_date in range
             'booked'      → call1_booking_date (when the meeting was scheduled) in range
             'created'     → created_at_ghl in range
    """
    if date_by == "created":
        col = Opportunity.created_at_ghl
        return and_(func.date(col) >= start, func.date(col) <= end)
    elif date_by == "booked":
        col = Opportunity.call1_booking_date
        return and_(col.isnot(None), func.date(col) >= start, func.date(col) <= end)
    else:
        # appointment (default)
        c1 = Opportunity.call1_appointment_date
        c2 = Opportunity.call2_appointment_date
        return or_(
            and_(func.date(c1) >= start, func.date(c1) <= end),
            and_(func.date(c2) >= start, func.date(c2) <= end),
        )


def sales_rep_filter():
    """WHERE clause that restricts to known sales reps (active + inactive).

    Uses NOT IN (other names) approach so whitespace variants in DB names
    don't cause mismatches. Unknown reps default to included.
    """
    return Opportunity.opportunity_owner_name.notin_(_DB_OTHER_NAMES)


# Sentinel value for "show everything" (no rep filter at all)
ALL_TEAM_SENTINEL = "__all__"


def base_filter(start: date, end: date, date_by: str, rep_id: str | None = None):
    """Base filter: not excluded + date range + optional rep filter + sales rep restriction.

    rep_id behaviour:
      None / ''        → "All Sales Team": restricts to known sales reps
      '__all__'        → "All Team": no rep filter at all (old default)
      '<ghl_owner_id>' → specific rep selected from dropdown
    """
    filters = [
        Opportunity.is_excluded.is_(False),
        date_filter(start, end, date_by),
    ]
    if rep_id == ALL_TEAM_SENTINEL:
        pass  # no rep filter — show everything
    elif rep_id:
        filters.append(Opportunity.opportunity_owner_id == rep_id)
    else:
        filters.append(sales_rep_filter())
    return and_(*filters)


# Minimum word count for a post-call note to be considered compliant
NOTE_MIN_WORDS = 50


def rep_non_compliance_expr():
    """SQLAlchemy boolean expression: opp has at least one compliance violation.

    TRUE if any of:
    1. outcome_unfilled — appointment passed 12h, status never updated
    2. Showed + NOT disqualified + lead_quality not filled
    3. Showed + NOT disqualified + post_call_note_word_count below threshold
    4. Disqualified + dq_reason not filled (DQ without a reason is still a violation)

    DQ'd leads are exempt from lead_quality and note requirements — the only
    obligation after disqualifying someone is to record why.
    """
    showed = showed_1st_call_expr()
    is_dq = Opportunity.pipeline_stage_id == DISQUALIFIED_STAGE_ID
    not_dq = Opportunity.pipeline_stage_id != DISQUALIFIED_STAGE_ID
    return or_(
        Opportunity.outcome_unfilled.is_(True),
        and_(showed, not_dq, Opportunity.lead_quality.is_(None)),
        and_(
            showed,
            not_dq,
            Opportunity.post_call_note_word_count.isnot(None),  # notes were checked
            Opportunity.post_call_note_word_count < NOTE_MIN_WORDS,
        ),
        and_(is_dq, Opportunity.dq_reason.is_(None)),
    )


def cycle_start_expr():
    """DATE the prospect first SHOWED for a call — the sales-cycle clock starts when
    the client actually turned up, not when the call was merely booked.

    1st call if they showed on it; otherwise the 2nd call (a deal can be a no-show on
    call 1 then show on the reschedule); otherwise fall back to the 1st call date.
    """
    from db.models import Opportunity

    return func.date(
        case(
            (showed_1st_call_expr(), Opportunity.call1_appointment_date),
            (showed_2nd_call_expr(), Opportunity.call2_appointment_date),
            else_=Opportunity.call1_appointment_date,
        )
    )


def sales_cycle_days_expr(payment_date_col):
    """Whole days from the first showed call → first payment.

    End of the clock is the first reconciled Whop payment (payment_date_col); when a
    won deal has no matched payment we fall back to GHL close_date so it still counts.
    Returns an integer day count (DATE − DATE), or NULL when the start date is missing.
    payment_date_col must be a DATE column (DealWhopMatch.first_payment_date).
    """
    from db.models import Opportunity

    start = cycle_start_expr()
    end = func.coalesce(payment_date_col, func.date(Opportunity.close_date))
    return end - start


def has_1st_call(start: date, end: date, date_by: str):
    """Opportunity had a 1st call within the relevant scope.

    appointment mode → call1_appointment_date falls in date range
    booked mode      → call1_booking_date falls in date range (meeting booked in period;
                       appointment may be before or after the range)
    created mode     → call1_appointment_date exists (opp was created in range via base_filter)
    """
    if date_by == "appointment":
        c1 = Opportunity.call1_appointment_date
        return and_(c1.isnot(None), func.date(c1) >= start, func.date(c1) <= end)
    elif date_by == "booked":
        bd = Opportunity.call1_booking_date
        return and_(bd.isnot(None), func.date(bd) >= start, func.date(bd) <= end)
    return Opportunity.call1_appointment_date.isnot(None)


def has_2nd_call(start: date, end: date, date_by: str):
    """Opportunity had a 2nd call within the relevant scope.

    booked mode falls back to "2nd call exists" — we don't track 2nd call booking dates.
    """
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


def bookable_1st_call_expr():
    """Show rate DENOMINATOR for 1st call: every call with a determinate outcome —
    showed (by status OR stage) + no-show + cancelled.

    MUST be a superset of showed_1st_call_expr(): the numerator counts a stage-based
    show (e.g. rep advanced to "1st Call Done") even when the appointment status is
    still "Confirmed". If such a call is not also counted here, shows can exceed
    bookable and the show rate goes above 100%. Truly-upcoming calls (Confirmed AND
    stage not advanced) are still excluded, so the denominator isn't distorted.
    """
    return or_(
        showed_1st_call_expr(),
        Opportunity.call1_appointment_status.in_(["No Show", "Cancelled"]),
    )


def bookable_2nd_call_expr():
    """Show rate DENOMINATOR for 2nd call — superset of showed_2nd_call_expr()
    (see bookable_1st_call_expr for why). Prevents show rate > 100% when a rep
    advanced to "2nd Call Done" while the appointment status is still "Confirmed"."""
    return or_(
        showed_2nd_call_expr(),
        Opportunity.call2_appointment_status.in_(["No Show", "Cancelled"]),
    )
