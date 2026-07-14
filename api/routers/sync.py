"""Sync status and manual trigger endpoints."""

import asyncio
import logging

from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas.responses import SyncStatusResponse, SyncTriggerResponse
from db.queries.sync_status import get_latest_sync_run
from db.session import get_db
from sync.appointment_resolver import resolve_appointments
from sync.sync_engine import run_sync

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sync", tags=["sync"])


@router.get("/status", response_model=SyncStatusResponse)
async def sync_status(db: AsyncSession = Depends(get_db)):
    """Return the most recent sync run's status and stats."""
    run = await get_latest_sync_run(db)

    if not run:
        return SyncStatusResponse(data=None, message="No sync runs found.")

    return SyncStatusResponse(
        data={
            "sync_type": run.sync_type,
            "status": run.status,
            "started_at": run.started_at,
            "completed_at": run.completed_at,
            "opportunities_synced": run.opportunities_synced,
            "errors_count": run.errors_count,
        },
        message="ok",
    )


@router.post("/trigger", response_model=SyncTriggerResponse)
async def trigger_sync(
    background_tasks: BackgroundTasks,
    sync_type: str = "full",
):
    """Manually trigger a sync. Runs in the background — returns immediately."""
    if sync_type not in ("full", "incremental"):
        sync_type = "full"

    background_tasks.add_task(_run_sync_background, sync_type)
    return SyncTriggerResponse(
        message=f"{sync_type.capitalize()} sync triggered. Check /api/sync/status for progress.",
        sync_type=sync_type,
    )


@router.post("/resolve-appointments")
async def trigger_resolver(
    background_tasks: BackgroundTasks,
    lookback_days: int = 3,
):
    """Manually trigger the Fireflies appointment resolver.

    Use lookback_days=30 for the initial retroactive sweep.
    Returns immediately — resolver runs in the background.
    """
    if lookback_days < 1 or lookback_days > 90:
        lookback_days = 3
    background_tasks.add_task(_run_resolver_background, lookback_days)
    return {
        "message": f"Appointment resolver triggered for {lookback_days}-day lookback. Check Railway logs for results.",
        "lookback_days": lookback_days,
    }


@router.post("/backfill-attribution")
async def backfill_attribution():
    """One-shot: re-resolve rep names for all deals (fixes 'Unassigned'). Fast + idempotent.

    Runs synchronously (one GHL user fetch + two set-based updates — seconds, not minutes).
    The fast alternative to a full re-sync when only rep attribution needs fixing.
    """
    from sync.attribution_backfill import backfill_rep_attribution

    try:
        stats = await backfill_rep_attribution()
        return {"ok": True, "stats": stats}
    except Exception as exc:
        logger.error("Attribution backfill failed: %s", exc)
        return {"ok": False, "error": str(exc)}


@router.post("/backfill-appointment-owners")
async def backfill_appointment_owners():
    """One-shot: recover rep for owner-less deals via the Call-2 appointment's assigned rep. Idempotent."""
    from sync.attribution_backfill import backfill_owner_from_appointments

    try:
        stats = await backfill_owner_from_appointments()
        return {"ok": True, "stats": stats}
    except Exception as exc:
        logger.error("Appointment-owner backfill failed: %s", exc)
        return {"ok": False, "error": str(exc)}


@router.post("/cancel/{run_id}")
async def cancel_sync(run_id: str, db: AsyncSession = Depends(get_db)):
    """Cancel a running sync run.

    Closes the sync_run row immediately (status='cancelled'); a live sync built
    from current code notices within one chunk (~seconds) and aborts its work.
    Also useful to clean up rows orphaned by zombie/killed processes on demand
    instead of waiting for the stale-run reaper.
    """
    import json as _json
    from sqlalchemy import text

    result = await db.execute(
        text("""
            UPDATE sync_runs
            SET status = 'cancelled',
                completed_at = now(),
                error_details = CAST(:details AS jsonb)
            WHERE id = CAST(:id AS uuid) AND status = 'running'
        """),
        {"id": run_id, "details": _json.dumps([{"error": "cancelled by user", "fatal": True}])},
    )
    await db.commit()
    cancelled = (result.rowcount or 0) > 0
    if cancelled:
        logger.info("Sync run %s cancelled by user", run_id)
    return {"ok": True, "cancelled": cancelled}


@router.get("/whop-inspect")
async def whop_inspect(email: str):
    """Diagnostic: raw Whop memberships + payments for one customer email.

    Bearer-protected (whole /api/sync router). Returns the customer's raw
    membership objects, per-membership payment objects, and a company-wide
    unfiltered payments probe matched to the customer — surfaces payments NOT
    attached to any membership (direct charges), which membership-scoped
    fetches can never see.
    """
    import httpx
    from sync.whop_payments import (
        WHOP_API_BASE,
        _extract_whop_identity,
        _fetch_membership_payments,
        _fetch_whop_memberships,
        _whop_headers,
    )

    email = email.lower().strip()
    out: dict = {"email": email}
    async with httpx.AsyncClient(timeout=30.0) as client:
        memberships = await _fetch_whop_memberships(client)
        mine = [m for m in memberships if _extract_whop_identity(m)[0] == email]
        out["membership_count"] = len(mine)
        out["memberships"] = mine
        out["payments_by_membership"] = {}
        my_ids = set()
        for m in mine:
            mid = m.get("id")
            if not mid:
                continue
            my_ids.add(mid)
            out["payments_by_membership"][mid] = await _fetch_membership_payments(client, mid)
        # Company-wide probe: unfiltered /payments, matched to this customer by
        # membership id OR user id — catches unattached/direct charges.
        user_ids = set()
        for m in mine:
            u = m.get("user")
            if isinstance(u, dict) and u.get("id"):
                user_ids.add(u["id"])
            elif isinstance(u, str):
                user_ids.add(u)
        probe: list = []
        page = 1
        status = None
        while page <= 40:
            resp = await client.get(
                f"{WHOP_API_BASE}/payments",
                headers=_whop_headers(),
                params={"per_page": 50, "page": page},
            )
            status = resp.status_code
            if status != 200:
                break
            data = resp.json()
            items = data.get("data", [])
            for p in items:
                p_m = p.get("membership")
                p_mid = p_m.get("id") if isinstance(p_m, dict) else p_m
                p_u = p.get("user")
                p_uid = p_u.get("id") if isinstance(p_u, dict) else (p_u or p.get("user_id"))
                if (p_mid and p_mid in my_ids) or (p_uid and p_uid in user_ids):
                    probe.append(p)
            pagination = data.get("pagination", {})
            if not items or pagination.get("current_page", page) >= pagination.get("total_page", 1):
                break
            page += 1
        out["unfiltered_probe_status"] = status
        out["probe_pages_scanned"] = page
        out["probe_payments"] = probe
    return out


async def _run_sync_background(sync_type: str) -> None:
    try:
        await run_sync(sync_type)
    except Exception as exc:
        logger.error("Background sync failed: %s", exc)


async def _run_resolver_background(lookback_days: int) -> None:
    try:
        summary = await resolve_appointments(lookback_days=lookback_days)
        logger.info("Manual resolver complete: %s", summary)
    except Exception as exc:
        logger.error("Manual resolver failed: %s", exc)
