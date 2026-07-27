"""Manual per-channel cost inputs (for ROI on the channel detail page)."""

from datetime import date

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def get_channel_cost(
    session: AsyncSession, channel: str, start: date, end: date
) -> float | None:
    """Cost entered for this channel over the exact selected range, or None if unset."""
    result = await session.execute(
        text(
            "SELECT cost FROM channel_period_cost "
            "WHERE channel = :channel AND period_start = :start AND period_end = :end"
        ),
        {"channel": channel, "start": start, "end": end},
    )
    row = result.one_or_none()
    return float(row[0]) if row else None


async def set_channel_cost(
    session: AsyncSession, channel: str, start: date, end: date, cost: float
) -> None:
    """Upsert the cost for a channel + exact date range."""
    await session.execute(
        text(
            """
            INSERT INTO channel_period_cost (channel, period_start, period_end, cost, updated_at)
            VALUES (:channel, :start, :end, :cost, now())
            ON CONFLICT (channel, period_start, period_end) DO UPDATE
                SET cost = EXCLUDED.cost, updated_at = now()
            """
        ),
        {"channel": channel, "start": start, "end": end, "cost": cost},
    )
    await session.commit()


async def delete_channel_cost(
    session: AsyncSession, channel: str, start: date, end: date
) -> bool:
    """Clear the cost for a channel + exact date range. Returns True if a row was removed."""
    result = await session.execute(
        text(
            "DELETE FROM channel_period_cost "
            "WHERE channel = :channel AND period_start = :start AND period_end = :end"
        ),
        {"channel": channel, "start": start, "end": end},
    )
    await session.commit()
    return result.rowcount > 0
