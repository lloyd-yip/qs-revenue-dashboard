"""Sync engine — orchestrates full and incremental GHL → PostgreSQL syncs.

Guarantees:
- Idempotent: upsert on ghl_opportunity_id. Safe to run multiple times.
- Resumable: each opportunity is processed independently; failures are logged
  and skipped without halting the rest.
- Auditable: every sync creates a sync_runs record with full stats.
"""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity, SourceNormalization, SyncRun
from db.session import AsyncSessionLocal
from sync.ghl_client import (
    GHLClient,
    extract_attributions,
    extract_custom_fields,
)
from sync.ghl_client import SHOWED_STAGE_IDS
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


async def _build_opportunity_row(
    opp: dict,
    normalization_map: dict[str, str],
    ghl_client: GHLClient,
    user_map: dict[str, str] | None = None,
) -> dict:
    """Transform a raw GHL opportunity payload into a dict ready for DB upsert."""
    custom = extract_custom_fields(opp)
    attrs = extract_attributions(opp)

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

    # Appointment dates from custom fields
    # Use rescheduled date if present, otherwise initial date
    call1_date_raw = custom.get("call1_appointment_date") or custom.get("call1_initial_appointment_date")
    call2_date_raw = custom.get("call2_appointment_date")
    call1_date = parse_ghl_datetime(call1_date_raw)
    call2_date = parse_ghl_datetime(call2_date_raw)

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

    # Call statuses
    call1_status = custom.get("call1_appointment_status")
    call2_status = custom.get("call2_appointment_status")

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

    # Post-call note word count — only for showed opps with a past appointment
    showed_1st = (
        call1_status == "Showed"
        or (stage_id is not None and stage_id in SHOWED_STAGE_IDS)
    )
    post_call_note_word_count: int | None = None
    if showed_1st and call1_date and opp.get("contactId"):
        now_utc = datetime.now(timezone.utc)
        if now_utc > call1_date + timedelta(hours=12):
            notes = await ghl_client.get_contact_notes(opp["contactId"])
            post_call_note_word_count = compute_post_call_note_word_count(
                notes=notes,
                owner_id=owner_id,
                call1_appointment_date=call1_date,
            )

    return {
        "ghl_opportunity_id": opp["id"],
        "ghl_contact_id": opp.get("contactId"),
        "opportunity_name": opportunity_name,
        "pipeline_stage_id": stage_id,
        "pipeline_stage_name": stage_name,
        "is_excluded": is_excluded_stage(stage_id, stage_name),
        "opportunity_owner_id": owner_id,
        "opportunity_owner_name": owner_name,
        "monetary_value": opp.get("monetaryValue"),
        "call1_appointment_status": call1_status,
        "call2_appointment_status": call2_status,
        "call1_appointment_date": call1_date,
        "call2_appointment_date": call2_date,
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
        "created_at_ghl": parse_ghl_datetime(opp.get("createdAt")),
        "updated_at_ghl": parse_ghl_datetime(opp.get("updatedAt")),
        "synced_at": datetime.now(timezone.utc),
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

    async with AsyncSessionLocal() as session:
        async for raw_opp in ghl_client.stream_opportunities(updated_after=updated_after):
            opp_id = raw_opp.get("id", "unknown")
            try:
                row = await _build_opportunity_row(raw_opp, normalization_map, ghl_client, user_map)

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

        # Final commit for remaining records
        await session.commit()

        # Update sync_run record
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
                "details": str(error_details) if error_details else None,
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
