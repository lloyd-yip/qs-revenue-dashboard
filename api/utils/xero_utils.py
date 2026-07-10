"""Shared Xero API constants and helpers used across xero_* routers and sync jobs.

Connector credentials (client ID/secret, tenant, redirect URI) are managed in-app
via Settings → Connectors and stored in the app_settings table. Legacy fallbacks
(the old hardcoded values / XERO_CLIENT_SECRET env var) keep existing deployments
working until values are saved in the UI.
"""

import base64
import json
import logging
import urllib.request
from dataclasses import dataclass

import httpx
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from config import settings
from db.queries.settings import get_setting
from db.session import AsyncSessionLocal

logger = logging.getLogger(__name__)

XERO_TENANT_ID = "3bead22e-28ff-4eb1-92cd-9b9d648e188a"

# ── Xero OAuth endpoints + connector settings keys ────────────────────────────

XERO_AUTH_URL  = "https://login.xero.com/identity/connect/authorize"
XERO_TOKEN_URL = "https://identity.xero.com/connect/token"

# Granular scopes — must each appear in the Xero app's own Authorisation scope
# list (developer portal → app → Configuration) or Xero returns invalid_scope.
# Post-March-2026 naming splits the old accounting.transactions into
# invoices / banktransactions / payments. Covers everything the dashboard
# reads: P&L report, ACCREC invoices, Wise bank transactions.
XERO_SCOPES = (
    "openid profile email offline_access "
    "accounting.reports.profitandloss.read "
    "accounting.invoices.read accounting.banktransactions.read "
    "accounting.settings.read"  # chart of accounts — maps account codes to names for expense detail
)

# app_settings keys — managed via Settings → Connectors (/settings)
XERO_SETTING_CLIENT_ID     = "xero_client_id"
XERO_SETTING_CLIENT_SECRET = "xero_client_secret"
XERO_SETTING_TENANT_ID     = "xero_tenant_id"
XERO_SETTING_REDIRECT_URI  = "xero_redirect_uri"
XERO_SETTING_SCOPES        = "xero_scopes"
XERO_SETTING_REFRESH_TOKEN = "xero_refresh_token"
XERO_SETTING_GRANTED_SCOPES = "xero_granted_scopes"  # what the current token actually has

# Legacy fallbacks — used only when nothing is saved in Settings → Connectors.
# "Automate accounting" — certified App Store app, pre-March 2026 (bypasses the
# per-org uncertified connection limit). Tenant: quantumSCALE Institute OÜ.
LEGACY_XERO_CLIENT_ID    = "EE84B9CECE064FDFA44A9989AD8356AA"
LEGACY_XERO_TENANT_ID    = XERO_TENANT_ID
LEGACY_XERO_REDIRECT_URI = "https://qs-revenue-dashboard-production.up.railway.app/xero/callback"


@dataclass
class XeroConfig:
    """Resolved Xero connector credentials with their provenance."""
    client_id: str
    client_secret: str
    tenant_id: str
    redirect_uri: str
    scopes: str
    client_id_source: str      # "app" | "default"
    client_secret_source: str  # "app" | "env" | "none"
    tenant_id_source: str      # "app" | "default"
    redirect_uri_source: str   # "app" | "default"
    scopes_source: str         # "app" | "default"


async def get_xero_config() -> XeroConfig:
    """Resolve Xero connector config: app_settings first, legacy fallbacks second."""
    async with AsyncSessionLocal() as session:
        client_id     = await get_setting(session, XERO_SETTING_CLIENT_ID)
        client_secret = await get_setting(session, XERO_SETTING_CLIENT_SECRET)
        tenant_id     = await get_setting(session, XERO_SETTING_TENANT_ID)
        redirect_uri  = await get_setting(session, XERO_SETTING_REDIRECT_URI)
        scopes        = await get_setting(session, XERO_SETTING_SCOPES)

    if client_secret:
        secret_source = "app"
    elif settings.xero_client_secret:
        client_secret, secret_source = settings.xero_client_secret, "env"
    else:
        client_secret, secret_source = "", "none"

    return XeroConfig(
        client_id=client_id or LEGACY_XERO_CLIENT_ID,
        client_secret=client_secret,
        tenant_id=tenant_id or LEGACY_XERO_TENANT_ID,
        redirect_uri=redirect_uri or LEGACY_XERO_REDIRECT_URI,
        scopes=scopes or XERO_SCOPES,
        client_id_source="app" if client_id else "default",
        client_secret_source=secret_source,
        tenant_id_source="app" if tenant_id else "default",
        redirect_uri_source="app" if redirect_uri else "default",
        scopes_source="app" if scopes else "default",
    )


def xero_basic_auth_header(cfg: XeroConfig) -> str:
    """Return the Authorization header value for Xero token endpoint calls.

    Raises 500 if no client secret is configured (Settings → Connectors, or the
    legacy XERO_CLIENT_SECRET env var).
    """
    if not cfg.client_secret:
        raise HTTPException(
            status_code=500,
            detail="Xero client secret is not configured. "
                   "Set it under Settings → Connectors → Xero.",
        )
    raw = f"{cfg.client_id}:{cfg.client_secret}"
    encoded = base64.b64encode(raw.encode()).decode()
    return f"Basic {encoded}"


async def xero_exchange_code(cfg: XeroConfig, code: str) -> dict:
    """Exchange an authorization code for access + refresh tokens."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            XERO_TOKEN_URL,
            headers={
                "Authorization": xero_basic_auth_header(cfg),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type":   "authorization_code",
                "code":         code,
                "redirect_uri": cfg.redirect_uri,
            },
            timeout=30,
        )
    if resp.status_code != 200:
        logger.error("Xero token exchange failed: %s %s", resp.status_code, resp.text)
        raise HTTPException(status_code=502, detail=f"Xero token exchange failed: {resp.text}")
    return resp.json()


async def xero_refresh_access_token(cfg: XeroConfig, refresh_token: str) -> dict:
    """Exchange a refresh token for a new access token (and new refresh token)."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            XERO_TOKEN_URL,
            headers={
                "Authorization": xero_basic_auth_header(cfg),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type":    "refresh_token",
                "refresh_token": refresh_token,
            },
            timeout=30,
        )
    if resp.status_code != 200:
        logger.error("Xero token refresh failed: %s %s", resp.status_code, resp.text)
        raise HTTPException(
            status_code=502,
            detail=f"Xero token refresh failed ({resp.status_code}). "
                   "The refresh token may have expired (60-day limit). "
                   "Reconnect under Settings → Connectors → Xero.",
        )
    return resp.json()


async def xero_access_token_from_stored_refresh() -> str:
    """Load the stored refresh token, exchange it, persist the rotated token.

    Returns a fresh 30-min access token. Raises 400 if no token is stored —
    the caller surfaces that to the user (connect under Settings → Connectors).
    """
    from db.queries.settings import set_setting  # local import avoids top-level churn

    async with AsyncSessionLocal() as session:
        refresh_token = await get_setting(session, XERO_SETTING_REFRESH_TOKEN)
    if not refresh_token:
        raise HTTPException(
            status_code=400,
            detail="Xero is not connected. Go to Settings → Connectors → Xero "
                   "and click 'Connect to Xero' (or pass xero_token=<token> directly).",
        )

    cfg = await get_xero_config()
    tokens = await xero_refresh_access_token(cfg, refresh_token)
    new_refresh_token = tokens.get("refresh_token", refresh_token)
    async with AsyncSessionLocal() as session:
        await set_setting(session, XERO_SETTING_REFRESH_TOKEN, new_refresh_token)
        if tokens.get("scope"):
            # Track what the token is actually granted — the Settings page compares
            # this against the requested scopes to show "Reconnect needed".
            await set_setting(session, XERO_SETTING_GRANTED_SCOPES, tokens["scope"])
    return tokens["access_token"]

# ECB monthly average EUR/USD rates — extend when syncing new months
EUR_USD_RATES: dict[str, float] = {
    "2025-10": 1.1630,
    "2025-11": 1.1560,
    "2025-12": 1.1709,
    "2026-01": 1.1738,
    "2026-02": 1.1824,
    "2026-03": 1.1558,
    "2026-04": 1.1706,
    "2026-05": 1.1729,
    "2026-06": 1.1518,
}

_bearer_scheme = HTTPBearer(auto_error=False)


async def verify_bearer(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer_scheme),
) -> None:
    """Raise 401 if the bearer token does not match settings.api_bearer_token."""
    if credentials is None or credentials.credentials != settings.api_bearer_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing bearer token.",
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_eur_usd_rate(year: int, month: int) -> float:
    """Return the ECB monthly average EUR/USD rate for the given month.

    Order: hardcoded table → ECB SDMX API → Frankfurter API → 1.10 fallback.
    """
    import calendar

    key = f"{year}-{month:02d}"
    if key in EUR_USD_RATES:
        return EUR_USD_RATES[key]

    # ECB SDMX monthly average (authoritative; same source as the table)
    ecb_url = (
        f"https://data-api.ecb.europa.eu/service/data/EXR/"
        f"M.USD.EUR.SP00.A?startPeriod={key}&endPeriod={key}"
        f"&detail=dataonly&format=jsondata"
    )
    try:
        with urllib.request.urlopen(ecb_url, timeout=10) as resp:
            data = json.loads(resp.read())
        obs = data["dataSets"][0]["series"]["0:0:0:0:0"]["observations"]
        rate = round(float(list(obs.values())[0][0]), 4)
        logger.info("ECB EUR/USD %s: %.4f", key, rate)
        return rate
    except Exception as exc:
        logger.warning("ECB rate fetch failed for %s: %s — trying Frankfurter", key, exc)

    last_day = calendar.monthrange(year, month)[1]
    start = f"{year}-{month:02d}-01"
    end   = f"{year}-{month:02d}-{last_day:02d}"
    url   = f"https://api.frankfurter.dev/{start}..{end}?from=EUR&to=USD"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
        usd_values = [v["USD"] for v in data.get("rates", {}).values() if "USD" in v]
        if usd_values:
            rate = round(sum(usd_values) / len(usd_values), 4)
            logger.info("Frankfurter EUR/USD %s: %.4f (avg %d days)", key, rate, len(usd_values))
            return rate
    except Exception as exc:
        logger.warning("Frankfurter rate fetch failed for %s: %s — using 1.10 fallback", key, exc)
    return 1.10
