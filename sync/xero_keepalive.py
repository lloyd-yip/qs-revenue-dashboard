"""Weekly Xero refresh-token keep-alive.

Xero refresh tokens expire after 60 days WITHOUT USE. Syncs are manual/monthly,
so a skipped month can silently kill the connection. This job refreshes (and
therefore rotates + re-persists) the stored token weekly, keeping it alive
indefinitely. If Xero is not connected, it skips quietly.
"""

import logging

from api.utils.xero_utils import (
    XERO_SETTING_REFRESH_TOKEN,
    get_xero_config,
    xero_refresh_access_token,
)
from db.queries.settings import get_setting, set_setting
from db.session import AsyncSessionLocal

logger = logging.getLogger(__name__)


async def keepalive_xero_token() -> dict:
    """Rotate the stored Xero refresh token. Returns a small status dict."""
    async with AsyncSessionLocal() as session:
        refresh_token = await get_setting(session, XERO_SETTING_REFRESH_TOKEN)

    if not refresh_token:
        logger.info("Xero keep-alive: not connected (no refresh token stored) — skipping")
        return {"status": "skipped", "reason": "not_connected"}

    cfg = await get_xero_config()
    tokens = await xero_refresh_access_token(cfg, refresh_token)  # raises on failure
    new_refresh_token = tokens.get("refresh_token", refresh_token)

    async with AsyncSessionLocal() as session:
        await set_setting(session, XERO_SETTING_REFRESH_TOKEN, new_refresh_token)

    logger.info("Xero keep-alive: refresh token rotated and persisted")
    return {"status": "refreshed"}
