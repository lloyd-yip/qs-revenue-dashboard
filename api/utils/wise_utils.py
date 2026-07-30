"""Wise connector helpers — API key + RSA private key resolution (app_settings → env).

Moves the Wise credentials from environment variables into the in-app connector store so
they're editable in Settings → Connectors. get_wise_config() resolves app_settings first,
falling back to the legacy env vars; pre_migrate_wise_from_env() copies existing env values
into app_settings once so they're pre-saved. Mirrors retell_utils / appointwise_utils.
"""

from dataclasses import dataclass

from config import settings
from db.queries.settings import get_setting, set_setting
from db.session import AsyncSessionLocal

WISE_SETTING_API_KEY = "wise_api_key"
WISE_SETTING_PRIVATE_KEY = "wise_private_key"


@dataclass
class WiseConfig:
    api_key: str
    api_key_source: str          # "app" | "env" | "none"
    private_key: str
    private_key_source: str      # "app" | "env" | "none"


async def get_wise_config() -> WiseConfig:
    async with AsyncSessionLocal() as session:
        app_key = await get_setting(session, WISE_SETTING_API_KEY)
        app_pk = await get_setting(session, WISE_SETTING_PRIVATE_KEY)

    if app_key:
        key, key_src = app_key, "app"
    elif settings.wise_api_key:
        key, key_src = settings.wise_api_key, "env"
    else:
        key, key_src = "", "none"

    if app_pk:
        pk, pk_src = app_pk, "app"
    elif settings.wise_private_key:
        pk, pk_src = settings.wise_private_key, "env"
    else:
        pk, pk_src = "", "none"

    return WiseConfig(api_key=key, api_key_source=key_src, private_key=pk, private_key_source=pk_src)


async def pre_migrate_wise_from_env() -> None:
    """One-time: copy env Wise credentials into app_settings if not already stored, so the
    connector tab is pre-populated with the existing value (runs server-side where env lives)."""
    async with AsyncSessionLocal() as session:
        if settings.wise_api_key and not await get_setting(session, WISE_SETTING_API_KEY):
            await set_setting(session, WISE_SETTING_API_KEY, settings.wise_api_key)
        if settings.wise_private_key and not await get_setting(session, WISE_SETTING_PRIVATE_KEY):
            await set_setting(session, WISE_SETTING_PRIVATE_KEY, settings.wise_private_key)
