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
