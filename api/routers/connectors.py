"""Settings → Connectors API — manage integration credentials stored in app_settings.

Currently covers the Xero connector: client ID/secret, tenant ID, redirect URI,
plus connection status (stored refresh token) and disconnect.

All endpoints require the bearer token (applied in api/main.py via verify_token).
The client secret is never returned in full — only a masked hint.
"""

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from api.utils.xero_utils import (
    XERO_SETTING_CLIENT_ID,
    XERO_SETTING_CLIENT_SECRET,
    XERO_SETTING_GRANTED_SCOPES,
    XERO_SETTING_REDIRECT_URI,
    XERO_SETTING_REFRESH_TOKEN,
    XERO_SETTING_SCOPES,
    XERO_SETTING_TENANT_ID,
    get_xero_config,
)
from db.queries.settings import delete_setting, get_setting, get_setting_meta, set_setting
from db.session import AsyncSessionLocal

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/settings/connectors", tags=["settings"])


class XeroConnectorStatus(BaseModel):
    client_id: str
    client_id_source: str          # "app" | "default"
    client_secret_set: bool
    client_secret_hint: str        # e.g. "••••7f2a" or ""
    client_secret_source: str      # "app" | "env" | "none"
    tenant_id: str
    tenant_id_source: str          # "app" | "default"
    redirect_uri: str
    redirect_uri_source: str       # "app" | "default"
    scopes: str
    scopes_source: str             # "app" | "default"
    connected: bool
    token_updated_at: str | None   # ISO timestamp of last refresh-token rotation
    missing_scopes: list[str]      # requested scopes the current token lacks
    reconnect_needed: bool         # connected but token predates a scope change


class XeroConnectorUpdate(BaseModel):
    """Fields to save. Omitted/null fields are left unchanged; empty string clears
    the app value (falling back to the legacy default/env value)."""
    client_id: str | None = None
    client_secret: str | None = None
    tenant_id: str | None = None
    redirect_uri: str | None = None
    scopes: str | None = None


def _mask(secret: str) -> str:
    return f"••••{secret[-4:]}" if len(secret) >= 8 else "••••"


async def _xero_status() -> XeroConnectorStatus:
    cfg = await get_xero_config()
    async with AsyncSessionLocal() as session:
        token_meta = await get_setting_meta(session, XERO_SETTING_REFRESH_TOKEN)
        granted    = await get_setting(session, XERO_SETTING_GRANTED_SCOPES) or ""

    connected = bool(token_meta and token_meta[0])
    granted_set = set(granted.split())
    # Only meaningful once we've recorded a grant; identity scopes echo back too.
    missing = (
        sorted(set(cfg.scopes.split()) - granted_set) if connected and granted_set else []
    )

    return XeroConnectorStatus(
        client_id=cfg.client_id,
        client_id_source=cfg.client_id_source,
        client_secret_set=bool(cfg.client_secret),
        client_secret_hint=_mask(cfg.client_secret) if cfg.client_secret else "",
        client_secret_source=cfg.client_secret_source,
        tenant_id=cfg.tenant_id,
        tenant_id_source=cfg.tenant_id_source,
        redirect_uri=cfg.redirect_uri,
        redirect_uri_source=cfg.redirect_uri_source,
        scopes=cfg.scopes,
        scopes_source=cfg.scopes_source,
        connected=connected,
        token_updated_at=token_meta[1].isoformat() if token_meta else None,
        missing_scopes=missing,
        reconnect_needed=bool(missing),
    )


@router.get("/xero", response_model=XeroConnectorStatus)
async def get_xero_connector() -> XeroConnectorStatus:
    """Current Xero connector config (secret masked) + connection status."""
    return await _xero_status()


@router.put("/xero", response_model=XeroConnectorStatus)
async def update_xero_connector(body: XeroConnectorUpdate) -> XeroConnectorStatus:
    """Save Xero connector credentials to app_settings.

    Null = unchanged; empty string = clear the app value (fall back to default).
    Changing client ID or secret invalidates any stored refresh token (tokens are
    per-app in Xero), so the connection is reset when either changes.
    """
    fields = {
        XERO_SETTING_CLIENT_ID:     body.client_id,
        XERO_SETTING_CLIENT_SECRET: body.client_secret,
        XERO_SETTING_TENANT_ID:     body.tenant_id,
        XERO_SETTING_REDIRECT_URI:  body.redirect_uri,
        XERO_SETTING_SCOPES:        body.scopes,
    }
    credentials_changed = False
    async with AsyncSessionLocal() as session:
        for key, value in fields.items():
            if value is None:
                continue
            value = value.strip()
            if value:
                await set_setting(session, key, value)
            else:
                await delete_setting(session, key)
            if key in (XERO_SETTING_CLIENT_ID, XERO_SETTING_CLIENT_SECRET):
                credentials_changed = True

        if credentials_changed:
            # Refresh tokens are bound to the Xero app — a new ID/secret makes the
            # old token useless. Drop it so the UI clearly shows "not connected".
            removed = await delete_setting(session, XERO_SETTING_REFRESH_TOKEN)
            if removed:
                logger.info("Xero credentials changed — stored refresh token cleared")

    logger.info("Xero connector settings updated (%s)",
                ", ".join(k for k, v in fields.items() if v is not None) or "no fields")
    return await _xero_status()


@router.post("/xero/disconnect", response_model=XeroConnectorStatus)
async def disconnect_xero() -> XeroConnectorStatus:
    """Remove the stored Xero refresh token (credentials are kept)."""
    async with AsyncSessionLocal() as session:
        removed = await delete_setting(session, XERO_SETTING_REFRESH_TOKEN)
    if not removed:
        raise HTTPException(status_code=404, detail="Xero is not connected.")
    logger.info("Xero disconnected — refresh token removed")
    return await _xero_status()
