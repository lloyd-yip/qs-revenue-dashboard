"""Read queries for whop_stats_snapshots — stored MRR/ARR for the CEO dashboard.

Snapshots are written by sync/whop_stats_sync.py. The monthly series takes the LATEST
snapshot within each month (MRR/ARR are point-in-time balances, not sums), so the chart
shows the recurring-revenue level at each month's most recent reading.
"""

from datetime import date

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import WhopStatsSnapshot


async def get_latest_whop_stats(session: AsyncSession) -> dict | None:
    """Most recent MRR/ARR snapshot, or None if none has been synced yet."""
    row = (await session.execute(
        select(WhopStatsSnapshot).order_by(WhopStatsSnapshot.snapshot_date.desc()).limit(1)
    )).scalar_one_or_none()
    if row is None:
        return None
    return {
        "snapshot_date": str(row.snapshot_date),
        "mrr": float(row.mrr or 0),
        "arr": float(row.arr or 0),
        "active_members": int(row.active_members or 0),
    }


async def get_whop_stats_by_month(session: AsyncSession, start: date, end: date) -> dict:
    """MRR/ARR per month (latest snapshot in each month) for [start, end].

    Returns {by_month: {"YYYY-MM": {mrr, arr, active_members}}}. Months without any
    snapshot are absent — the dashboard leaves those points empty (history only builds
    forward from when snapshotting began).
    """
    rows = (await session.execute(
        select(WhopStatsSnapshot)
        .where(WhopStatsSnapshot.snapshot_date >= start)
        .where(WhopStatsSnapshot.snapshot_date <= end)
        .order_by(WhopStatsSnapshot.snapshot_date.asc())
    )).scalars().all()

    by_month: dict[str, dict] = {}
    for r in rows:  # ascending → the last write for a month wins (latest reading)
        mk = f"{r.snapshot_date.year:04d}-{r.snapshot_date.month:02d}"
        by_month[mk] = {
            "mrr": float(r.mrr or 0),
            "arr": float(r.arr or 0),
            "active_members": int(r.active_members or 0),
        }
    return {"by_month": by_month}
