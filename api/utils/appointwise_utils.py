"""Appointwise connector helpers — API key + client ID resolution (app_settings → env).

Appointwise is a GHL-native SMS/WhatsApp appointment setter with no documented public
analytics API, so the SMS metrics themselves come from GHL Conversations. These credentials
are stored for source-checking / any future Appointwise API use. Mirrors retell_utils.
"""

from dataclasses import dataclass

from config import settings
from db.queries.settings import get_setting
from db.session import AsyncSessionLocal

APPOINTWISE_SETTING_API_KEY = "appointwise_api_key"
APPOINTWISE_SETTING_CLIENT_ID = "appointwise_client_id"


@dataclass
class AppointwiseConfig:
    api_key: str
    api_key_source: str      # "app" | "env" | "none"
    client_id: str
    client_id_source: str    # "app" | "env" | "none"


async def get_appointwise_config() -> AppointwiseConfig:
    async with AsyncSessionLocal() as session:
        app_key = await get_setting(session, APPOINTWISE_SETTING_API_KEY)
        app_cid = await get_setting(session, APPOINTWISE_SETTING_CLIENT_ID)

    if app_key:
        key, key_src = app_key, "app"
    elif settings.appointwise_api_key:
        key, key_src = settings.appointwise_api_key, "env"
    else:
        key, key_src = "", "none"

    if app_cid:
        cid, cid_src = app_cid, "app"
    elif settings.appointwise_client_id:
        cid, cid_src = settings.appointwise_client_id, "env"
    else:
        cid, cid_src = "", "none"

    return AppointwiseConfig(api_key=key, api_key_source=key_src, client_id=cid, client_id_source=cid_src)
