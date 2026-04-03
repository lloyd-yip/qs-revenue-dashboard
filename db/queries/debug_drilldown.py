"""Debug drill-down queries — returns the individual opportunity rows behind each KPI.

Uses the exact same filter expressions as metrics_summary.py / common.py so
the row count always matches the number shown on the dashboard.

Also includes data-quality anomaly detection that highlights GHL records
where the data is inconsistent (stage/status mismatches, missing fields, etc.).
"""

from datetime import date

from sqlalchemy import and_, or_, select
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
from sync.ghl_client import (
    DEAL_WON_STAGE_ID,
    DISQUALIFIED_STAGE_ID,
    NO_SHOW_STAGE_ID,
    CANCELLED_STAGE_ID,
    SHOWED_STAGE_IDS,
)

# ── Pipeline stage IDs (1. Sales Pipeline) ───────────────────────────────────

FIRST_CALL_DONE_STAGE_ID = "45a0608f-7648-4509-8f3a-d93b21cc9d41"
SECOND_CALL_DONE_STAGE_ID = "10e6b1ef-0685-4f73-b3c7-b5006b7bc311"
FU_CALL_GHOST_STAGE_ID = "38aac258-cb3d-447a-828c-03b623ee5d05"
DEAL_LOST_STAGE_ID = "80cba97d-2f60-4485-8953-4b9569b1ddc1"
WARM_LIST_STAGE_ID = "d51c088d-1629-43f2-8ee8-c51bf74b8553"
HOT_LIST_STAGE_ID = "8b0e8559-7665-4033-b762-23d94bfce90b"
LONG_TERM_NURTURE_STAGE_ID = "dfb71208-834b-43e1-9777-5895d6dc8722"
HOT_LONG_TERM_NURTURE_STAGE_ID = "bd0a2b3d-abcc-414d-8935-5a7d781e9727"

# Stage ID → friendly name fallback (used when DB has NULL stage name).
# The /api/dashboard/lookups endpoint fetches these dynamically from GHL and
# overrides this map, so this is just a safety net.
STAGE_NAMES = {
    "c2315e44-4992-49e6-a2da-f177c884838e": "Duplicates",
    "8ffe9c93-0dc9-4a36-8241-f1252c6a425d": "Application, No Booking",
    "e82907fd-4d76-4c1a-a867-b82c1093a88d": "Upcoming 1st Call Booked",
    FIRST_CALL_DONE_STAGE_ID: "1st Call Done",
    SECOND_CALL_DONE_STAGE_ID: "2nd Call Done (In Prog)",
    WARM_LIST_STAGE_ID: "Warm List (1st or 2nd Call done)",
    HOT_LIST_STAGE_ID: "Hot List (Verbal Commit)",
    DEAL_WON_STAGE_ID: "Deal Won",
    LONG_TERM_NURTURE_STAGE_ID: "Long Term Nurture",
    HOT_LONG_TERM_NURTURE_STAGE_ID: "Hot Long Term Nurture",
    NO_SHOW_STAGE_ID: "1st Call - No-Show",
    CANCELLED_STAGE_ID: "1st Call - Cancelled",
    DISQUALIFIED_STAGE_ID: "Disqualified",
    DEAL_LOST_STAGE_ID: "Deal Lost",
    FU_CALL_GHOST_STAGE_ID: "FU Call Ghost",
    "c17b5e6f-c2a4-42b5-9ff3-dbf35d7083b9": "Temp Ryan/ Upcoming 1st Call Booked",
    "59c6d23d-1c46-4fc6-886c-a1045205aad6": "Temp Ryan/ Initial 1st Call Done",
    "fb6fe57f-8baf-4edd-b3a5-c0e8c3d29355": "Temp Ryan/ Hot List",
}

# Stages that imply the 1st call happened (showed)
STAGES_IMPLYING_SHOWED = {
    FIRST_CALL_DONE_STAGE_ID,
    SECOND_CALL_DONE_STAGE_ID,
    FU_CALL_GHOST_STAGE_ID,
    DEAL_WON_STAGE_ID,
    DEAL_LOST_STAGE_ID,
    WARM_LIST_STAGE_ID,        # "Warm List (1st or 2nd Call done)"
    HOT_LIST_STAGE_ID,         # "Hot List (Verbal Commit)"
    LONG_TERM_NURTURE_STAGE_ID,      # implies showed previously
    HOT_LONG_TERM_NURTURE_STAGE_ID,  # implies showed previously
}

# Stale statuses — appointment status was never updated
STALE_STATUSES = {None, "Confirmed", "confirmed"}

# Columns returned for every drilldown row
_DRILLDOWN_COLUMNS = [
    Opportunity.ghl_opportunity_id,
    Opportunity.ghl_contact_id,
    Opportunity.opportunity_name,
    Opportunity.opportunity_owner_name,
    Opportunity.opportunity_owner_id,
    Opportunity.pipeline_stage_name,
    Opportunity.pipeline_stage_id,
    Opportunity.call1_appointment_date,
    Opportunity.call1_appointment_status,
    Opportunity.call1_calendar_id,
    Opportunity.call2_appointment_date,
    Opportunity.call2_appointment_status,
    Opportunity.lead_quality,
    Opportunity.monetary_value,
    Opportunity.canonical_channel,
    Opportunity.created_at_ghl,
    Opportunity.outcome_unfilled,
    Opportunity.rep_compliance_failure,
    Opportunity.post_call_note_word_count,
    Opportunity.dq_reason,
    Opportunity.deal_lost_reasons,
]


def _build_metric_filter(metric: str, start: date, end: date, date_by: str):
    """Return an additional WHERE clause for the given metric key.

    This is layered on top of base_filter() so we get: not-excluded + date range + metric-specific.
    """
    is_1st = has_1st_call(start, end, date_by)
    is_2nd = has_2nd_call(start, end, date_by)
    showed_1st = showed_1st_call_expr()
    showed_2nd = showed_2nd_call_expr()

    match metric:
        case "calls_booked_1st":
            return is_1st

        case "shows_1st" | "show_rate_1st":
            return and_(is_1st, showed_1st, ~Opportunity.outcome_unfilled)

        case "no_show_1st" | "no_show_rate_1st":
            return and_(
                is_1st,
                ~Opportunity.outcome_unfilled,
                ~showed_1st,
            )

        case "bookable_1st":
            return and_(is_1st, ~Opportunity.outcome_unfilled)

        case "calls_booked_2nd":
            return is_2nd

        case "shows_2nd" | "show_rate_2nd":
            return and_(is_2nd, showed_2nd)

        case "qualified_shows" | "qualification_rate":
            return and_(
                is_1st,
                showed_1st,
                Opportunity.lead_quality.in_(QUALIFIED_LEAD_QUALITY),
            )

        case "dq_count" | "dq_rate":
            return and_(
                is_1st,
                showed_1st,
                or_(
                    Opportunity.lead_quality == "DQ",
                    Opportunity.pipeline_stage_id == DISQUALIFIED_STAGE_ID,
                ),
            )

        case "dq_after_call2":
            return and_(
                is_1st,
                showed_1st,
                or_(
                    Opportunity.lead_quality == "DQ",
                    Opportunity.pipeline_stage_id == DISQUALIFIED_STAGE_ID,
                ),
                Opportunity.call2_appointment_date.isnot(None),
            )

        case "units_closed" | "projected_contract_value" | "close_rate":
            return Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID

        case "compliance_failures":
            return Opportunity.rep_compliance_failure.is_(True)

        case "outcome_unfilled":
            return Opportunity.outcome_unfilled.is_(True)

        case "total_shows":
            return or_(
                and_(is_1st, showed_1st),
                and_(is_2nd, showed_2nd),
            )

        case "data_quality":
            # Return all opps with a 1st call — anomaly detection happens in Python
            return is_1st

        case _:
            # Fallback: return all opps in scope
            return is_1st


# ── Anomaly Detection ─────────────────────────────────────────────────────────

def _detect_anomalies(row: dict) -> list[dict]:
    """Run all anomaly checks on a single opportunity row.

    Returns a list of dicts: [{id, label, severity, action}]
    """
    anomalies = []
    stage_id = row.get("stage_id")
    status = row.get("call1_appointment_status")
    stage_name = row.get("stage_name") or STAGE_NAMES.get(stage_id, "")

    def add(anomaly_id: str, label: str, severity: str, action: str):
        anomalies.append({
            "id": anomaly_id,
            "label": label,
            "severity": severity,
            "action": action,
        })

    # ── 🔴 High: Stage/Status Contradictions ──────────────────────────────

    # 1. Stage implies showed but status ≠ Showed
    if stage_id in STAGES_IMPLYING_SHOWED and status in STALE_STATUSES:
        add(
            "stage_showed_status_stale",
            f"Stage = \"{stage_name}\" but status = \"{status or 'NULL'}\"",
            "high",
            "Update call 1 status to \"Showed\" in GHL",
        )

    # 2. Stage = 1st Call No-Show but status = Showed
    if stage_id == NO_SHOW_STAGE_ID and status == "Showed":
        add(
            "stage_noshow_status_showed",
            f"Stage = \"1st Call No-Show\" but status = \"Showed\"",
            "high",
            "Either update stage (call happened) or fix status to \"No Show\"",
        )

    # 3. Stage = Cancelled but status = Showed
    if stage_id == CANCELLED_STAGE_ID and status == "Showed":
        add(
            "stage_cancelled_status_showed",
            f"Stage = \"Cancelled\" but status = \"Showed\"",
            "high",
            "Either update stage (call happened) or fix status to \"Cancelled\"",
        )

    # 4. Post-call note exists but status still Confirmed/NULL
    note_wc = row.get("post_call_note_word_count")
    if note_wc and note_wc > 0 and status in STALE_STATUSES:
        # Only flag if not already caught by stage check
        if stage_id not in STAGES_IMPLYING_SHOWED:
            add(
                "note_exists_status_stale",
                f"Post-call note exists ({note_wc} words) but status = \"{status or 'NULL'}\"",
                "high",
                "Update call 1 status — note suggests the call happened",
            )

    # ── 🟡 Medium: Missing Data ──────────────────────────────────────────

    # 5. Past appointment, status never updated (outcome unfilled)
    if row.get("outcome_unfilled"):
        # Only flag if not already caught by a higher-severity check
        if not any(a["id"] in ("stage_showed_status_stale", "note_exists_status_stale") for a in anomalies):
            add(
                "outcome_unfilled",
                f"Appointment passed but status still \"{status or 'NULL'}\"",
                "medium",
                "Update call 1 status to Showed, No Show, or Cancelled",
            )

    # 6. Showed but Lead Quality not filled
    showed = status == "Showed" or stage_id in STAGES_IMPLYING_SHOWED
    if showed and row.get("lead_quality") is None:
        # Skip if stage is No-Show, Cancelled, or Disqualified (DQ might have happened without show)
        if stage_id not in (NO_SHOW_STAGE_ID, CANCELLED_STAGE_ID):
            add(
                "showed_no_lead_quality",
                "Showed but Lead Quality field is empty",
                "medium",
                "Fill Lead Quality dropdown (Great / Ok / Barely Passable / Bad / DQ)",
            )

    # 7. Showed but no post-call note
    if showed and (note_wc is not None and note_wc == 0):
        if stage_id not in (NO_SHOW_STAGE_ID, CANCELLED_STAGE_ID):
            add(
                "showed_no_note",
                "Showed but no post-call note found",
                "medium",
                "Add a post-call note describing the conversation",
            )

    # 8. Deal Won but no monetary value
    if stage_id == DEAL_WON_STAGE_ID and not row.get("monetary_value"):
        add(
            "deal_won_no_value",
            "Deal Won but no monetary value set",
            "medium",
            "Enter the deal value on the opportunity",
        )

    # 9. Has 2nd call date but 1st call ≠ Showed
    if row.get("call2_appointment_date") and status != "Showed":
        add(
            "call2_without_call1_showed",
            f"2nd call booked but 1st call status = \"{status or 'NULL'}\"",
            "medium",
            "Update 1st call status to \"Showed\" or remove the 2nd call booking",
        )

    # 10. Stage = No-Show but status is stale (never updated to No Show)
    if stage_id == NO_SHOW_STAGE_ID and status in STALE_STATUSES:
        add(
            "stage_noshow_status_stale",
            f"Stage = \"1st Call No-Show\" but status = \"{status or 'NULL'}\"",
            "medium",
            "Update call 1 status to \"No Show\"",
        )

    return anomalies


def _row_to_dict(r) -> dict:
    """Convert a SQLAlchemy row to a dict with serializable values."""
    stage_id = r.pipeline_stage_id
    # Use our known stage names if the DB has NULL
    stage_name = r.pipeline_stage_name or STAGE_NAMES.get(stage_id)

    return {
        "ghl_opportunity_id": r.ghl_opportunity_id,
        "ghl_contact_id": r.ghl_contact_id,
        "opportunity_name": r.opportunity_name,
        "rep_name": r.opportunity_owner_name,
        "rep_id": r.opportunity_owner_id,
        "stage_name": stage_name,
        "stage_id": stage_id,
        "call1_appointment_date": r.call1_appointment_date.isoformat() if r.call1_appointment_date else None,
        "call1_appointment_status": r.call1_appointment_status,
        "call1_calendar_id": r.call1_calendar_id,
        "call2_appointment_date": r.call2_appointment_date.isoformat() if r.call2_appointment_date else None,
        "call2_appointment_status": r.call2_appointment_status,
        "lead_quality": r.lead_quality,
        "monetary_value": float(r.monetary_value) if r.monetary_value else None,
        "canonical_channel": r.canonical_channel,
        "created_at_ghl": r.created_at_ghl.isoformat() if r.created_at_ghl else None,
        "outcome_unfilled": r.outcome_unfilled,
        "compliance_failure": r.rep_compliance_failure,
        "post_call_note_word_count": r.post_call_note_word_count,
        "dq_reason": r.dq_reason,
        "deal_lost_reasons": r.deal_lost_reasons,
    }


async def get_drilldown_opps(
    session: AsyncSession,
    metric: str,
    start: date,
    end: date,
    date_by: str,
    rep_id: str | None = None,
) -> list[dict]:
    """Return the individual opportunity rows behind a dashboard KPI."""

    bf = base_filter(start, end, date_by, rep_id)
    metric_filter = _build_metric_filter(metric, start, end, date_by)

    result = await session.execute(
        select(*_DRILLDOWN_COLUMNS)
        .where(and_(bf, metric_filter))
        .order_by(Opportunity.call1_appointment_date.desc().nulls_last())
    )

    is_data_quality = metric == "data_quality"
    rows = []

    for r in result.all():
        row = _row_to_dict(r)

        if is_data_quality:
            anomalies = _detect_anomalies(row)
            if anomalies:  # Only include rows with issues
                row["anomalies"] = anomalies
                rows.append(row)
        else:
            # For regular drilldown, still run anomaly detection for visual hints
            row["anomalies"] = _detect_anomalies(row)
            rows.append(row)

    return rows
