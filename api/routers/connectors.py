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
from api.utils.appointwise_utils import (
    APPOINTWISE_SETTING_API_KEY,
    APPOINTWISE_SETTING_CLIENT_ID,
    get_appointwise_config,
    get_or_create_webhook_secret,
)
from api.utils.retell_utils import (
    RETELL_SETTING_API_KEY,
    get_retell_config,
    test_retell_key,
)
from api.utils.wise_utils import (
    WISE_SETTING_API_KEY,
    WISE_SETTING_PRIVATE_KEY,
    derive_public_key_pem,
    generate_wise_keypair,
    get_wise_config,
    pre_migrate_wise_from_env,
)
from config import settings as _env
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


# ── Retell (AI voice calls) ────────────────────────────────────────────────────

class RetellConnectorStatus(BaseModel):
    api_key_set: bool
    api_key_hint: str       # e.g. "••••7f2a" or ""
    api_key_source: str     # "app" | "env" | "none"
    connected: bool         # a usable key is present
    updated_at: str | None  # when the in-app key was last saved


class RetellConnectorUpdate(BaseModel):
    """api_key: null = leave unchanged; empty string = clear the in-app key
    (falling back to any env value)."""
    api_key: str | None = None


async def _retell_status() -> RetellConnectorStatus:
    cfg = await get_retell_config()
    async with AsyncSessionLocal() as session:
        meta = await get_setting_meta(session, RETELL_SETTING_API_KEY)
    return RetellConnectorStatus(
        api_key_set=bool(cfg.api_key),
        api_key_hint=_mask(cfg.api_key) if cfg.api_key else "",
        api_key_source=cfg.api_key_source,
        connected=bool(cfg.api_key),
        updated_at=meta[1].isoformat() if meta else None,
    )


@router.get("/retell", response_model=RetellConnectorStatus)
async def get_retell_connector() -> RetellConnectorStatus:
    """Current Retell connector status (API key masked)."""
    return await _retell_status()


@router.put("/retell", response_model=RetellConnectorStatus)
async def update_retell_connector(body: RetellConnectorUpdate) -> RetellConnectorStatus:
    """Save the Retell API key to app_settings. Null = unchanged; empty = clear."""
    if body.api_key is not None:
        value = body.api_key.strip()
        async with AsyncSessionLocal() as session:
            if value:
                await set_setting(session, RETELL_SETTING_API_KEY, value)
            else:
                await delete_setting(session, RETELL_SETTING_API_KEY)
        logger.info("Retell connector API key %s", "saved" if value else "cleared")
    return await _retell_status()


@router.post("/retell/test")
async def test_retell_connector() -> dict:
    """Validate the stored Retell API key against the Retell API."""
    cfg = await get_retell_config()
    ok, detail = await test_retell_key(cfg.api_key)
    return {"ok": ok, "detail": detail, "source": cfg.api_key_source}


@router.post("/retell/disconnect", response_model=RetellConnectorStatus)
async def disconnect_retell() -> RetellConnectorStatus:
    """Remove the stored Retell API key."""
    async with AsyncSessionLocal() as session:
        removed = await delete_setting(session, RETELL_SETTING_API_KEY)
    if not removed:
        raise HTTPException(status_code=404, detail="Retell is not connected.")
    logger.info("Retell disconnected — API key removed")
    return await _retell_status()


# ── Appointwise (AI SMS) ───────────────────────────────────────────────────────

class AppointwiseConnectorStatus(BaseModel):
    api_key_set: bool
    api_key_hint: str
    api_key_source: str
    client_id: str
    client_id_source: str
    connected: bool
    updated_at: str | None
    webhook_url: str        # paste into the Appointwise Webhook Node
    webhook_secret: str     # goes in the X-Appointwise-Secret header


class AppointwiseConnectorUpdate(BaseModel):
    api_key: str | None = None
    client_id: str | None = None


async def _appointwise_status() -> AppointwiseConnectorStatus:
    cfg = await get_appointwise_config()
    secret = await get_or_create_webhook_secret()
    async with AsyncSessionLocal() as session:
        meta = await get_setting_meta(session, APPOINTWISE_SETTING_API_KEY)
    return AppointwiseConnectorStatus(
        api_key_set=bool(cfg.api_key),
        api_key_hint=_mask(cfg.api_key) if cfg.api_key else "",
        api_key_source=cfg.api_key_source,
        client_id=cfg.client_id,
        client_id_source=cfg.client_id_source,
        connected=bool(cfg.api_key),
        updated_at=meta[1].isoformat() if meta else None,
        webhook_url=f"{_env.public_base_url.rstrip('/')}/api/webhooks/appointwise",
        webhook_secret=secret,
    )


@router.get("/appointwise", response_model=AppointwiseConnectorStatus)
async def get_appointwise_connector() -> AppointwiseConnectorStatus:
    return await _appointwise_status()


@router.put("/appointwise", response_model=AppointwiseConnectorStatus)
async def update_appointwise_connector(body: AppointwiseConnectorUpdate) -> AppointwiseConnectorStatus:
    """Save the Appointwise API key + client ID. Null = unchanged; empty = clear."""
    fields = {APPOINTWISE_SETTING_API_KEY: body.api_key, APPOINTWISE_SETTING_CLIENT_ID: body.client_id}
    async with AsyncSessionLocal() as session:
        for key, value in fields.items():
            if value is None:
                continue
            value = value.strip()
            if value:
                await set_setting(session, key, value)
            else:
                await delete_setting(session, key)
    logger.info("Appointwise connector updated")
    return await _appointwise_status()


@router.post("/appointwise/disconnect", response_model=AppointwiseConnectorStatus)
async def disconnect_appointwise() -> AppointwiseConnectorStatus:
    async with AsyncSessionLocal() as session:
        removed = await delete_setting(session, APPOINTWISE_SETTING_API_KEY)
        await delete_setting(session, APPOINTWISE_SETTING_CLIENT_ID)
    if not removed:
        raise HTTPException(status_code=404, detail="Appointwise is not connected.")
    logger.info("Appointwise disconnected")
    return await _appointwise_status()


# ── Wise (bank-transfer reconciliation) ────────────────────────────────────────

class WiseConnectorStatus(BaseModel):
    api_key_set: bool
    api_key_hint: str
    api_key_source: str
    private_key_set: bool
    private_key_source: str
    public_key_pem: str | None   # derived from the SAVED private key — the exact key to register in Wise
    connected: bool
    updated_at: str | None


class WiseConnectorUpdate(BaseModel):
    api_key: str | None = None
    private_key: str | None = None


async def _wise_status() -> WiseConnectorStatus:
    cfg = await get_wise_config()
    async with AsyncSessionLocal() as session:
        meta = await get_setting_meta(session, WISE_SETTING_API_KEY)
    public_key_pem = None
    if cfg.private_key:
        try:
            public_key_pem = derive_public_key_pem(cfg.private_key)
        except Exception:  # noqa: BLE001 — a malformed stored key shouldn't break status
            public_key_pem = None
    return WiseConnectorStatus(
        api_key_set=bool(cfg.api_key),
        api_key_hint=_mask(cfg.api_key) if cfg.api_key else "",
        api_key_source=cfg.api_key_source,
        private_key_set=bool(cfg.private_key),
        private_key_source=cfg.private_key_source,
        public_key_pem=public_key_pem,
        connected=bool(cfg.api_key and cfg.private_key),
        updated_at=meta[1].isoformat() if meta else None,
    )


@router.get("/wise", response_model=WiseConnectorStatus)
async def get_wise_connector() -> WiseConnectorStatus:
    """Current Wise connector status. Pre-migrates any env value into app_settings so the
    field is pre-saved on first open."""
    await pre_migrate_wise_from_env()
    return await _wise_status()


@router.put("/wise", response_model=WiseConnectorStatus)
async def update_wise_connector(body: WiseConnectorUpdate) -> WiseConnectorStatus:
    """Save the Wise API key + RSA private key to app_settings. Null = unchanged; empty = clear."""
    fields = {WISE_SETTING_API_KEY: body.api_key, WISE_SETTING_PRIVATE_KEY: body.private_key}
    async with AsyncSessionLocal() as session:
        for key, value in fields.items():
            if value is None:
                continue
            value = value.strip()
            if value:
                await set_setting(session, key, value)
            else:
                await delete_setting(session, key)
    logger.info("Wise connector updated")
    return await _wise_status()


@router.post("/wise/disconnect", response_model=WiseConnectorStatus)
async def disconnect_wise() -> WiseConnectorStatus:
    """Wipe every stored Wise credential from app_settings — API key and private key
    (the public key is derived, never stored). Clears both regardless of which was set so
    no stale value can linger."""
    async with AsyncSessionLocal() as session:
        removed_api = await delete_setting(session, WISE_SETTING_API_KEY)
        removed_pk = await delete_setting(session, WISE_SETTING_PRIVATE_KEY)
    if not (removed_api or removed_pk):
        raise HTTPException(status_code=404, detail="Wise is not configured in-app.")
    logger.info("Wise disconnected (removed api_key=%s private_key=%s)", removed_api, removed_pk)
    return await _wise_status()


class WiseKeypairResult(BaseModel):
    private_key_pem: str      # returned once so the user can copy/back it up
    public_key_pem: str       # register this in Wise → Developer → SCA
    status: WiseConnectorStatus


@router.post("/wise/generate-keypair", response_model=WiseKeypairResult)
async def generate_wise_keypair_endpoint() -> WiseKeypairResult:
    """Generate a fresh RSA keypair for Wise SCA, save the private key to app_settings, and
    return both PEMs. The public key still has to be registered manually in Wise (there is no
    API to upload SCA public keys)."""
    private_pem, public_pem = generate_wise_keypair()
    async with AsyncSessionLocal() as session:
        await set_setting(session, WISE_SETTING_PRIVATE_KEY, private_pem)
    logger.info("Wise RSA keypair generated and private key saved to app_settings")
    return WiseKeypairResult(
        private_key_pem=private_pem,
        public_key_pem=public_pem,
        status=await _wise_status(),
    )


class WiseTestSample(BaseModel):
    date: str | None
    amount: float
    currency: str
    sender_name: str


class WiseTestResult(BaseModel):
    ok: bool
    message: str
    usd_count: int = 0
    eur_count: int = 0
    sample: list[WiseTestSample] = []


@router.post("/wise/test", response_model=WiseTestResult)
async def test_wise_connection() -> WiseTestResult:
    """Live check that the saved Wise API key can pull incoming transfers.

    Uses the Activities feed (no SCA) — the balance-statement endpoints require Wise SCA,
    which is unavailable on this account (Wise rejects a valid signature). Only the API key
    is needed here; the private key/SCA path is retained in the client but unused."""
    cfg = await get_wise_config()
    if not cfg.api_key:
        return WiseTestResult(ok=False, message="Set the Wise API key first.")

    from sync.wise_client import fetch_wise_incoming_transfers

    try:
        txns = await fetch_wise_incoming_transfers(days=90)
    except Exception as exc:  # noqa: BLE001 — surface any client/network error to the UI
        logger.warning("Wise test connection failed: %s", exc)
        return WiseTestResult(ok=False, message=f"Wise request failed: {str(exc)[:200]}")

    usd = [t for t in txns if t.get("currency") == "USD"]
    eur = [t for t in txns if t.get("currency") == "EUR"]
    sample = [
        WiseTestSample(
            date=str(t.get("date")) if t.get("date") else None,
            amount=float(t.get("amount") or 0.0),
            currency=str(t.get("currency") or ""),
            sender_name=str(t.get("sender_name") or ""),
        )
        for t in txns[:5]
    ]
    if not txns:
        return WiseTestResult(
            ok=True,
            message="Connected — no incoming transfers found in the last 90 days.",
        )
    return WiseTestResult(
        ok=True,
        message=f"Connected ✓ Found {len(txns)} incoming transfers (last 90 days): "
                f"{len(usd)} USD + {len(eur)} EUR. Use Sync to match them to deals.",
        usd_count=len(usd),
        eur_count=len(eur),
        sample=sample,
    )


# Bank-transfer reconciliation (Wise + Payoneer) is done via the Xero bank sync
# (POST /xero/sync-wise-transfers) — the single source, so a transfer is never ingested
# twice. The old direct Wise Activities ingestion endpoint was removed for that reason;
# /wise/test still uses the Activities feed as a lightweight API-key check.


# ── Overview summary (drives the connector grid) ───────────────────────────────

@router.get("/summary")
async def connectors_summary() -> dict:
    """Status of every connector for the overview grid. Manageable ones have their own
    GET/PUT endpoints; env-configured ones are reported read-only."""
    xero = await _xero_status()
    retell = await _retell_status()
    aw = await _appointwise_status()
    wise = await _wise_status()
    items = [
        {"key": "xero", "name": "Xero", "group": "integration", "manageable": True,
         "connected": xero.connected,
         "desc": "Accounting — P&L revenue, invoices (contract value), Wise reconciliation."},
        {"key": "retell", "name": "Retell (VERA)", "group": "ai", "manageable": True,
         "connected": retell.connected,
         "desc": "AI voice call agent — call sync, recordings, contact matching."},
        {"key": "appointwise", "name": "Appointwise", "group": "ai", "manageable": True,
         "connected": aw.connected,
         "desc": "AI SMS appointment setter (GHL-native). Metrics read from GHL Conversations."},
        {"key": "ghl", "name": "GoHighLevel", "group": "integration", "manageable": False,
         "connected": bool(_env.ghl_api_key),
         "desc": "CRM — deals, contacts, appointments, conversations. Configured via env."},
        {"key": "stripe", "name": "Stripe", "group": "integration", "manageable": False,
         "connected": bool(_env.stripe_secret_key),
         "desc": "Read-only charge/customer lookups for deal payment enrichment."},
        {"key": "whop", "name": "Whop", "group": "integration", "manageable": False,
         "connected": bool(_env.whop_api_key),
         "desc": "Cash-collected payment matching for closed deals."},
        {"key": "wise", "name": "Wise", "group": "integration", "manageable": True,
         "connected": wise.connected,
         "desc": "Bank-transfer reconciliation against deals."},
        {"key": "fireflies", "name": "Fireflies", "group": "integration", "manageable": False,
         "connected": bool(_env.fireflies_api_key),
         "desc": "Meeting transcripts — appointment status resolver."},
    ]
    return {"data": items}
