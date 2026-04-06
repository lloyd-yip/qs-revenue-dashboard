"""Sync run status queries."""

from datetime import datetime

from sqlalchemy import desc, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import SyncRun


async def get_latest_sync_run(session: AsyncSession) -> SyncRun | None:
    """Return the most recent sync run, or None if no runs exist."""
    result = await session.execute(
        select(SyncRun).order_by(desc(SyncRun.started_at)).limit(1)
    )
    return result.scalar_one_or_none()


async def check_db_health(session: AsyncSession) -> tuple[bool, datetime | None]:
    """Check DB connectivity and return (db_ok, last_sync_completed_at).

    Returns (False, None) if the DB is unreachable.
    """
    try:
        await session.execute(text("SELECT 1"))
        result = await session.execute(
            select(SyncRun.completed_at)
            .where(SyncRun.status == "completed")
            .order_by(SyncRun.completed_at.desc())
            .limit(1)
        )
        last_sync_at = result.scalar_one_or_none()
        return True, last_sync_at
    except Exception:
        return False, None


async def get_recent_sync_runs(session: AsyncSession, limit: int = 50) -> list[dict]:
    """Return the most recent sync runs as dicts for the sync history page."""
    result = await session.execute(
        select(SyncRun)
        .order_by(desc(SyncRun.started_at))
        .limit(limit)
    )
    runs = result.scalars().all()
    return [
        {
            "id": str(run.id),
            "sync_type": run.sync_type,
            "status": run.status,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "completed_at": run.completed_at.isoformat() if run.completed_at else None,
            "duration_seconds": round((run.completed_at - run.started_at).total_seconds(), 1)
                if run.completed_at and run.started_at else None,
            "opportunities_synced": run.opportunities_synced,
            "errors_count": run.errors_count,
            "error_details": run.error_details,
        }
        for run in runs
    ]

