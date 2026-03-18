"""Sync status and manual trigger endpoints."""

import asyncio
import logging

from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas.responses import SyncStatusResponse, SyncTriggerResponse
from db.models import SyncRun
from db.session import get_db
from sync.sync_engine import run_sync

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sync", tags=["sync"])


@router.get("/status", response_model=SyncStatusResponse)
async def sync_status(db: AsyncSession = Depends(get_db)):
    """Return the most recent sync run's status and stats."""
    result = await db.execute(
        select(SyncRun).order_by(desc(SyncRun.started_at)).limit(1)
    )
    run = result.scalar_one_or_none()

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
    sync_type: str = "full",
    background_tasks: BackgroundTasks = BackgroundTasks(),
):
    """Manually trigger a sync. Runs in the background — returns immediately."""
    if sync_type not in ("full", "incremental"):
        sync_type = "full"

    background_tasks.add_task(_run_sync_background, sync_type)
    return SyncTriggerResponse(
        message=f"{sync_type.capitalize()} sync triggered. Check /api/sync/status for progress.",
        sync_type=sync_type,
    )


async def _run_sync_background(sync_type: str) -> None:
    try:
        await run_sync(sync_type)
    except Exception as exc:
        logger.error("Background sync failed: %s", exc)
