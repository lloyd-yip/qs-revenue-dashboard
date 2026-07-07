"""Sync engine — orchestrates full and incremental GHL → PostgreSQL syncs.

Guarantees:
- Idempotent: upsert on ghl_opportunity_id. Safe to run multiple times.
- Resumable: each opportunity is processed independently; failures are logged
  and skipped without halting the rest.
- Auditable: every sync creates a sync_runs record with full stats.
"""

import json
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from db.models import Appointment, Opportunity, SourceNormalization, SyncRun
from db.session import AsyncSessionLocal
from sync.ghl_client import (
    DEAL_WON_STAGE_ID,
    GHLClient,
    SHOWED_STAGE_IDS,
    classify_calendar,
    extract_attributions,
    extract_custom_fields,
    funnel_of_calendar,
)

# Pipeline IDs — used to loop over both pipelines in run_sync
SALES_PIPELINE_ID  = "zbI8YxmB9qhk1h4cInnq"
UPSELL_PIPELINE_ID = "NjidsHukHHUpYtTcQefX"
from sync.normalizer import (
    compute_compliance_failure,
    compute_outcome_unfilled,
    compute_post_call_note_word_count,
    is_excluded_stage,
    parse_ghl_datetime,
    resolve_canonical_channel,
)

logger = logging.getLogger(__name__)


async def _load_normalization_map(session: AsyncSession) -> dict[str, str]:
    """Load the full source_normalization table into memory as a dict."""
    result = await session.execute(select(SourceNormalization.raw_value, SourceNormalization.canonical_channel))
    return {row.raw_value: row.canonical_channel for row in result}


async def _get_last_successful_sync(session: AsyncSession) -> datetime | None:
    """Return the started_at timestamp of the last completed sync, or None."""
    result = await session.execute(
        select(SyncRun.started_at)
        .where(SyncRun.status == "completed")
        .order_by(SyncRun.started_at.desc())
        .limit(1)
    )
    row = result.scalar_one_or_none()
    return row


def _normalize_appt_status(raw: str) -> str | None:
    """Map GHL calendar appointment status values to our internal labels."""
    mapping = {
        "showed": "Showed",
        "noshow": "No Show",
        "confirmed": "Confirmed",
        "cancelled": "Cancelled",
        "new": "Confirmed",
    }
    return mapping.get(raw.lower().strip()) if raw else None


def _appointment_booking_date(appt: dict | None) -> datetime | None:
    """Return the booking timestamp for a matched calendar appointment when present."""
    if not appt:
        return None
    return parse_ghl_datetime(appt.get("createdAt") or appt.get("dateAdded"))


# Outcome priority for the outcome-aware 1st-call status (D1, Lloyd 2026-07-07).
_STATUS_PRIORITY = ["Showed", "No Show", "Cancelled", "Confirmed"]


def _derive_calls_from_appointments(
    appointments: list[dict],
    calendar_names: dict[str, str],
) -> dict:
    """Derive call1/call2 date+status from the contact's CALENDAR appointments.

    Approved per-opportunity positional model (F1 fix, Lloyd 2026-07-07 —
    see project-control/f1-fix-scope.md):
      - 1st call = the contact's earliest 'first' appointment (Business Evaluation /
        QuantumSCALE Demo / Referral). Status is OUTCOME-AWARE: Showed if any 1st-call
        attempt showed, else No Show, else Cancelled, else Confirmed. call1_date is the
        showed attempt's date when present, otherwise the earliest attempt's date.
        call1_booking_date is the earliest attempt's booking time (when first booked).
      - 2nd call = earliest 'followup' appointment (>= call1 date when available).

    Returns keys: call1_date, call1_status, call1_booking_date, call2_date,
    call2_status, followup_appt, first_call_attempts. first_call_attempts == 0 means
    the contact has no classifiable 1st-call appointment, so the caller can fall back
    to the legacy custom field.
    """
    firsts: list[tuple[datetime, dict]] = []
    followups: list[tuple[datetime, dict]] = []
    for appt in appointments:
        if appt.get("deleted"):
            continue
        start = parse_ghl_datetime(appt.get("startTime"))
        if start is None:
            continue
        role = classify_calendar(calendar_names.get(appt.get("calendarId")))
        if role == "first":
            firsts.append((start, appt))
        elif role == "followup":
            followups.append((start, appt))

    firsts.sort(key=lambda x: x[0])
    followups.sort(key=lambda x: x[0])

    result: dict = {
        "call1_date": None, "call1_status": None, "call1_booking_date": None,
        "call2_date": None, "call2_status": None,
        "followup_appt": None, "first_call_attempts": len(firsts),
        "first_call_funnel": None,
    }

    if firsts:
        statuses = [_normalize_appt_status(a.get("appointmentStatus") or "") for _, a in firsts]
        result["call1_status"] = next((s for s in _STATUS_PRIORITY if s in statuses), None)
        # Date: prefer the showed attempt (when the call actually happened), else earliest.
        showed = next(
            ((dt, a) for dt, a in firsts
             if _normalize_appt_status(a.get("appointmentStatus") or "") == "Showed"),
            None,
        )
        primary_dt, primary_appt = showed if showed else firsts[0]
        result["call1_date"] = primary_dt
        # Booking date = when they FIRST booked (earliest attempt) — drives date_by='booked'.
        result["call1_booking_date"] = _appointment_booking_date(firsts[0][1])
        # Reporting funnel (webinar / outreach / referral) from the 1st-call calendar.
        result["first_call_funnel"] = funnel_of_calendar(
            calendar_names.get(primary_appt.get("calendarId"))
        )

    if followups:
        chosen = None
        if result["call1_date"] is not None:
            chosen = next(((dt, a) for dt, a in followups if dt >= result["call1_date"]), None)
        if chosen is None:
            chosen = followups[0]
        fdt, fappt = chosen
        result["call2_date"] = fdt
        result["call2_status"] = _normalize_appt_status(fappt.get("appointmentStatus") or "")
        result["followup_appt"] = fappt

    return result


async def _build_opportunity_row(
    opp: dict,
    normalization_map: dict[str, str],
    ghl_client: GHLClient,
    user_map: dict[str, str] | None = None,
    pipeline_id: str | None = None,
    is_upsell: bool = False,
    calendar_names: dict[str, str] | None = None,
) -> dict:
    """Transform a raw GHL opportunity payload into a dict ready for DB upsert.

    is_upsell=True skips the expensive per-contact API calls (appointments, contact
    dateAdded, notes) since the upsell pipeline has no call1/call2 dates to resolve.
    """
    custom = extract_custom_fields(opp)
    attrs = extract_attributions(opp)
    calendar_names = calendar_names or {}

    # Opportunity name (lead/contact name from GHL)
    opportunity_name = opp.get("name")

    # Stage info
    stage = opp.get("pipelineStage") or {}
    stage_id = stage.get("id") or opp.get("pipelineStageId")
    stage_name = stage.get("name") or opp.get("pipelineStage")

    # Rep attribution — assignedTo is a plain user ID string in GHL v2
    assigned_to = opp.get("assignedTo")
    owner_id = assigned_to if isinstance(assigned_to, str) else None
    user_map = user_map or {}
    owner_name = user_map.get(owner_id) if owner_id else None

    # Legacy custom-field call1 — kept ONLY as a fallback for contacts with no
    # classifiable calendar appointment. The custom field is unreliable (~50-60%
    # populated; collapsed after the June-2026 calendar restructure), so the calendar
    # is now the source of truth. See project-control/f1-fix-scope.md (F1).
    cf_call1_date = parse_ghl_datetime(
        custom.get("call1_appointment_date") or custom.get("call1_initial_appointment_date")
    )
    cf_call1_status = custom.get("call1_appointment_status")

    # Attribution
    op_book_source = custom.get("op_book_campaign_source")
    op_book_medium = custom.get("op_book_campaign_medium")
    op_book_name = custom.get("op_book_campaign_name")
    raw_ghl_source = opp.get("source")

    canonical_channel = resolve_canonical_channel(
        normalization_map=normalization_map,
        attr_first_utm_source=attrs["attr_first_utm_source"],
        op_book_campaign_source=op_book_source,
        raw_ghl_source=raw_ghl_source,
    )

    # call1 + call2 date/status derive from the GHL CALENDAR (appointments) using the
    # approved per-opportunity positional model (earliest 1st-call = call1, outcome-aware
    # status; earliest follow-up = call2). Falls back to the legacy custom field only when
    # the contact has no classifiable 1st-call appointment.
    call1_date: datetime | None = None
    call1_status: str | None = None
    call1_booking_date: datetime | None = None
    call2_date: datetime | None = None
    call2_status: str | None = None
    first_call_funnel: str | None = None
    contact_id = opp.get("contactId")
    all_appointments: list[dict] = []
    followup_appt: dict | None = None

    if contact_id and not is_upsell:
        # Upsell pipeline has no 1st/2nd call appointments — skip these expensive calls.
        all_appointments = await ghl_client.get_contact_appointments(contact_id)
        derived = _derive_calls_from_appointments(all_appointments, calendar_names)
        followup_appt = derived["followup_appt"]
        call2_date = derived["call2_date"]
        call2_status = derived["call2_status"]
        first_call_funnel = derived["first_call_funnel"]
        if derived["first_call_attempts"] > 0:
            call1_date = derived["call1_date"]
            call1_status = derived["call1_status"]
            call1_booking_date = derived["call1_booking_date"]
        else:
            # No calendar 1st-call for this contact — fall back to the legacy custom field.
            call1_date = cf_call1_date
            call1_status = cf_call1_status
    else:
        # Upsell pipeline (no calls fetched) — nothing to derive from the calendar.
        call1_date = cf_call1_date
        call1_status = cf_call1_status

    # contact_created_at: fetch from GHL contact record (dateAdded).
    # Only fetched if not already stored — incremental syncs avoid re-fetching.
    contact_created_at: datetime | None = None
    if contact_id and not is_upsell:
        contact = await ghl_client.get_contact(contact_id)
        if contact:
            contact_created_at = parse_ghl_datetime(contact.get("dateAdded"))

    # close_date: automation-set custom field wonlostabandoned_date (vzU9IqXPuwAYkKrJ3I3F).
    # Written by GHL automation when deal status changes to won/lost/abandoned — stable and precise.
    # Fallback: if the custom field is missing but the deal IS at the won stage, use
    # lastStatusChangeAt (or updatedAt as a last resort).
    # Uses stage_id instead of opp["status"] because the GHL /opportunities/search endpoint
    # does not reliably return the "status" field — only the single-opp GET does.
    # A slightly imprecise date is far better than NULL — which would silently drop the
    # deal from all close-date queries and cost card calculations.
    close_date: datetime | None = parse_ghl_datetime(custom.get("wonlostabandoned_date"))
    if close_date is None and stage_id == DEAL_WON_STAGE_ID:
        close_date = (
            parse_ghl_datetime(opp.get("lastStatusChangeAt"))
            or parse_ghl_datetime(opp.get("updatedAt"))
        )

    # DEBUG: trace close_date resolution for specific opp (remove after fix verified)
    opp_id_raw = opp.get("id", "")
    if opp_id_raw == "hmY7ixWyJ8Cxf4WbLk9z":
        logger.warning(
            "DEBUG Judith: stage_id=%r, DEAL_WON=%r, match=%s, "
            "custom_wla=%r, lastStatusChangeAt=%r, updatedAt=%r, close_date=%r, "
            "status_field=%r, pipelineStage=%r, pipelineStageId=%r",
            stage_id, DEAL_WON_STAGE_ID, stage_id == DEAL_WON_STAGE_ID,
            custom.get("wonlostabandoned_date"),
            opp.get("lastStatusChangeAt"), opp.get("updatedAt"), close_date,
            opp.get("status"), opp.get("pipelineStage"), opp.get("pipelineStageId"),
        )

    # Legacy compliance flag (stage-specific — kept for backward compat)
    compliance_failure = compute_compliance_failure(
        pipeline_stage_id=stage_id,
        call1_appointment_date=call1_date,
        call1_appointment_status=call1_status,
    )

    # Outcome unfilled — broader signal (any stage, 12h grace)
    outcome_unfilled = compute_outcome_unfilled(
        call1_appointment_date=call1_date,
        call1_appointment_status=call1_status,
    )

    # Post-call note word count — only for showed opps with a past appointment.
    # Skipped for upsell pipeline (no call1 appointments to evaluate).
    showed_1st = (
        call1_status == "Showed"
        or (stage_id is not None and stage_id in SHOWED_STAGE_IDS)
    )
    post_call_note_word_count: int | None = None
    if not is_upsell and showed_1st and call1_date and opp.get("contactId"):
        now_utc = datetime.now(timezone.utc)
        if now_utc > call1_date + timedelta(hours=12):
            notes = await ghl_client.get_contact_notes(opp["contactId"])
            post_call_note_word_count = compute_post_call_note_word_count(
                notes=notes,
                owner_id=owner_id,
                call1_appointment_date=call1_date,
            )

    # Rep fallback: if the opportunity has no owner, attribute to the rep who ran the
    # Call-2 (decision) call — the follow-up appointment's assigned user. Recovers deals
    # the closer never got set as Opportunity Owner on (e.g. owner-less GHL opps).
    if owner_id is None and followup_appt:
        appt_user = followup_appt.get("assignedUserId")
        if isinstance(appt_user, str) and appt_user:
            owner_id = appt_user
            owner_name = user_map.get(appt_user)

    return {
        "ghl_opportunity_id": opp["id"],
        "ghl_contact_id": opp.get("contactId"),
        "opportunity_name": opportunity_name,
        "pipeline_id": pipeline_id,
        "pipeline_stage_id": stage_id,
        "pipeline_stage_name": stage_name,
        "is_excluded": is_excluded_stage(stage_id, stage_name),
        "opportunity_owner_id": owner_id,
        "opportunity_owner_name": owner_name,
        "monetary_value": opp.get("monetaryValue"),
        "cash_collected": float(custom["cash_collected"]) if custom.get("cash_collected") else None,
        "call1_appointment_status": call1_status,
        "call2_appointment_status": call2_status,
        "call1_appointment_date": call1_date,
        "call2_appointment_date": call2_date,
        "call1_booking_date": call1_booking_date,
        "first_call_funnel": first_call_funnel,
        "lead_quality": custom.get("lead_quality"),
        "financial_qual": custom.get("financial_qual"),
        "intent_to_transform": custom.get("intent_to_transform"),
        "pre_call_indoctrination": custom.get("pre_call_indoctrination"),
        "business_fit": custom.get("business_fit"),
        "pain_goal_oriented": custom.get("pain_goal_oriented"),
        "dq_reason": custom.get("dq_reason"),
        "deal_lost_reasons": custom.get("deal_lost_reasons"),
        "business_industry": custom.get("business_industry"),
        "current_revenue": custom.get("current_revenue"),
        **attrs,
        "op_book_campaign_source": op_book_source,
        "op_book_campaign_medium": op_book_medium,
        "op_book_campaign_name": op_book_name,
        "canonical_channel": canonical_channel,
        "rep_compliance_failure": compliance_failure,
        "outcome_unfilled": outcome_unfilled,
        "post_call_note_word_count": post_call_note_word_count,
        "contact_created_at": contact_created_at,
        "close_date": close_date,
        "created_at_ghl": parse_ghl_datetime(opp.get("createdAt")),
        "updated_at_ghl": parse_ghl_datetime(opp.get("updatedAt")),
        "synced_at": datetime.now(timezone.utc),
        "_all_appointments": all_appointments,  # passed through for appointments upsert; stripped before DB insert
    }


async def run_sync(sync_type: str = "incremental") -> dict:
    """Run a full or incremental sync. Returns a summary dict.

    sync_type: 'full' | 'incremental'
    """
    started_at = datetime.now(timezone.utc)
    logger.info("Starting %s sync at %s", sync_type, started_at.isoformat())

    async with AsyncSessionLocal() as session:
        # Create sync_run record
        sync_run = SyncRun(
            sync_type=sync_type,
            started_at=started_at,
            status="running",
        )
        session.add(sync_run)
        await session.commit()
        await session.refresh(sync_run)
        sync_run_id = sync_run.id

        normalization_map = await _load_normalization_map(session)

        # Determine incremental cutoff
        updated_after: datetime | None = None
        if sync_type == "incremental":
            last_sync = await _get_last_successful_sync(session)
            if last_sync:
                # 1-hour buffer to catch clock skew and late GHL updates
                updated_after = last_sync - timedelta(hours=1)
                logger.info("Incremental sync: fetching opportunities updated after %s", updated_after.isoformat())
            else:
                logger.info("No previous sync found — running as full sync")
                sync_type = "full"

    ghl_client = GHLClient()
    synced_count: int = 0
    error_count: int = 0
    error_details: list = []

    user_map = await ghl_client.get_users()
    logger.info("Loaded %d users for rep name resolution", len(user_map))

    # Calendar id→name map — drives name-based appointment classification (F1 fix).
    calendar_names = await ghl_client.get_calendars()

    # Loop over both pipelines — sales first, then upsell.
    pipeline_configs = [
        {"pipeline_id": SALES_PIPELINE_ID,  "is_upsell": False},
        {"pipeline_id": UPSELL_PIPELINE_ID, "is_upsell": True},
    ]

    async with AsyncSessionLocal() as session:
        for pipeline_cfg in pipeline_configs:
            pid       = pipeline_cfg["pipeline_id"]
            is_upsell = pipeline_cfg["is_upsell"]
            logger.info("Syncing pipeline %s (is_upsell=%s)", pid, is_upsell)

            async for raw_opp in ghl_client.stream_opportunities(updated_after=updated_after, pipeline_id=pid):
                opp_id = raw_opp.get("id", "unknown")
                try:
                    row = await _build_opportunity_row(
                        raw_opp, normalization_map, ghl_client, user_map,
                        pipeline_id=pid, is_upsell=is_upsell,
                        calendar_names=calendar_names,
                    )

                    # Pull appointments out before DB insert — not a DB column
                    all_appointments = row.pop("_all_appointments", [])
                    contact_id_for_appts = row.get("ghl_contact_id")

                    # PostgreSQL upsert — idempotent on ghl_opportunity_id.
                    # Exclude compliance history columns — they are managed by the
                    # conditional UPDATE below, not overwritten on every sync.
                    history_cols = {"outcome_unfilled_first_flagged_at", "outcome_unfilled_resolved_at"}
                    stmt = (
                        pg_insert(Opportunity)
                        .values(**row)
                        .on_conflict_do_update(
                            index_elements=["ghl_opportunity_id"],
                            set_={k: v for k, v in row.items() if k not in {"ghl_opportunity_id"} | history_cols},
                        )
                    )
                    await session.execute(stmt)

                    # Upsert all appointments for this contact into the appointments table
                    if contact_id_for_appts and all_appointments:
                        for appt in all_appointments:
                            appt_id = appt.get("id")
                            if not appt_id:
                                continue
                            cal_id = appt.get("calendarId")
                            # Name-based classification (F1 fix): 'first'→call_1,
                            # 'followup'→call_2, delivery/internal→'other' (excluded
                            # from call metrics). Robust to new per-rep calendars.
                            _role = classify_calendar(calendar_names.get(cal_id))
                            appt_type = {"first": "call_1", "followup": "call_2"}.get(_role, "other")
                            appt_row = {
                                "ghl_contact_id": contact_id_for_appts,
                                "ghl_appointment_id": appt_id,
                                "calendar_id": cal_id,
                                "appointment_type": appt_type,
                                "appointment_date": parse_ghl_datetime(appt.get("startTime")),
                                "appointment_status": _normalize_appt_status(appt.get("appointmentStatus") or ""),
                            }
                            appt_stmt = (
                                pg_insert(Appointment)
                                .values(**appt_row)
                                .on_conflict_do_update(
                                    index_elements=["ghl_appointment_id"],
                                    set_={k: v for k, v in appt_row.items() if k != "ghl_appointment_id"},
                                )
                            )
                            await session.execute(appt_stmt)

                    # Compliance history: track when outcome_unfilled was first set and when resolved.
                    # - first_flagged_at: set once when outcome_unfilled first becomes TRUE
                    # - resolved_at: set once when outcome_unfilled transitions TRUE → FALSE
                    # Both use CASE logic so they are never overwritten after being set.
                    await session.execute(
                        text("""
                            UPDATE opportunities SET
                                outcome_unfilled_first_flagged_at = CASE
                                    WHEN outcome_unfilled = TRUE AND outcome_unfilled_first_flagged_at IS NULL
                                    THEN :now
                                    ELSE outcome_unfilled_first_flagged_at
                                END,
                                outcome_unfilled_resolved_at = CASE
                                    WHEN outcome_unfilled = FALSE
                                         AND outcome_unfilled_resolved_at IS NULL
                                         AND outcome_unfilled_first_flagged_at IS NOT NULL
                                    THEN :now
                                    ELSE outcome_unfilled_resolved_at
                                END
                            WHERE ghl_opportunity_id = :ghl_id
                        """),
                        {"now": started_at, "ghl_id": row["ghl_opportunity_id"]},
                    )

                    synced_count += 1

                    # Commit in batches of 50 to balance memory and safety
                    if synced_count % 50 == 0:
                        await session.commit()
                        logger.info("Committed batch — %d opportunities synced so far", synced_count)

                except Exception as exc:
                    error_count += 1
                    error_detail = {"opportunity_id": opp_id, "error": str(exc)}
                    error_details.append(error_detail)
                    logger.error("Failed to sync opportunity %s: %s", opp_id, exc)
                    # Continue — never halt the sync for one bad record

            # Commit remaining records for this pipeline before moving to the next
            await session.commit()
            logger.info("Pipeline %s done — %d total synced so far", pid, synced_count)

        # Update sync_run record — runs once after all pipelines complete
        completed_at = datetime.now(timezone.utc)
        status = "completed" if error_count == 0 else "completed"  # still completed — errors are logged
        if synced_count == 0 and error_count > 0:
            status = "failed"

        await session.execute(
            text("""
                UPDATE sync_runs
                SET status = :status,
                    completed_at = :completed_at,
                    opportunities_synced = :synced,
                    errors_count = :errors,
                    error_details = CAST(:details AS jsonb)
                WHERE id = :id
            """),
            {
                "status": status,
                "completed_at": completed_at,
                "synced": synced_count,
                "errors": error_count,
                "details": json.dumps(error_details) if error_details else None,
                "id": str(sync_run_id),
            },
        )
        await session.commit()

    duration_s = (datetime.now(timezone.utc) - started_at).total_seconds()
    summary = {
        "sync_type": sync_type,
        "status": status,
        "opportunities_synced": synced_count,
        "errors_count": error_count,
        "duration_seconds": round(duration_s, 1),
    }
    logger.info("Sync complete: %s", summary)
    return summary
