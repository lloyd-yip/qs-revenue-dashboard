"""Retell connector helpers — API-key resolution (app_settings → env) and a light
connection test. Mirrors the Xero connector pattern (api/utils/xero_utils.py) but for a
simple bearer API key rather than OAuth.

The full Retell API client (call listing, recordings, contact matching) lands in Part 2
as sync/retell_client.py and builds on get_retell_config() here.
"""

from dataclasses import dataclass

import httpx

from config import settings
from db.queries.settings import get_setting
from db.session import AsyncSessionLocal

# app_settings key for the in-app saved key.
RETELL_SETTING_API_KEY = "retell_api_key"

RETELL_API_BASE = "https://api.retellai.com"


@dataclass
class RetellConfig:
    api_key: str
    api_key_source: str  # "app" | "env" | "none"


async def get_retell_config() -> RetellConfig:
    """Resolve the Retell API key: in-app value first, then env fallback."""
    async with AsyncSessionLocal() as session:
        app_key = await get_setting(session, RETELL_SETTING_API_KEY)

    if app_key:
        return RetellConfig(api_key=app_key, api_key_source="app")
    if settings.retell_api_key:
        return RetellConfig(api_key=settings.retell_api_key, api_key_source="env")
    return RetellConfig(api_key="", api_key_source="none")


async def test_retell_key(api_key: str) -> tuple[bool, str]:
    """Validate a Retell API key with a cheap authenticated call.

    Returns (ok, detail). Uses list-agents — a lightweight GET that only needs the key.
    """
    if not api_key:
        return False, "No API key set."
    headers = {"Authorization": f"Bearer {api_key}"}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{RETELL_API_BASE}/list-agents", headers=headers)
    except httpx.HTTPError as exc:
        return False, f"Could not reach Retell: {exc}"

    if resp.status_code < 300:
        return True, "Connection OK."
    if resp.status_code in (401, 403):
        return False, "Retell rejected the API key (unauthorized)."
    return False, f"Retell returned HTTP {resp.status_code}."
