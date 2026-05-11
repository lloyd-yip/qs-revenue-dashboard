"""App settings — persistent key/value store for tokens and config.

Used to store Xero OAuth refresh token so it survives Railway restarts.
"""

from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def get_setting(session: AsyncSession, key: str) -> str | None:
    """Return the value for a settings key, or None if not set."""
    result = await session.execute(
        text("SELECT value FROM app_settings WHERE key = :key"),
        {"key": key},
    )
    row = result.one_or_none()
    return row[0] if row else None


async def set_setting(session: AsyncSession, key: str, value: str) -> None:
    """Upsert a settings key/value pair."""
    await session.execute(
        text("""
            INSERT INTO app_settings (key, value, updated_at)
            VALUES (:key, :value, now())
            ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value,
                    updated_at = now()
        """),
        {"key": key, "value": value},
    )
    await session.commit()
