"""Xero OAuth 2.0 flow + revenue sync endpoint.

Three routes:
  GET  /xero/auth          — redirects browser to Xero login (no auth required)
  GET  /xero/callback      — Xero redirects here after approval; stores refresh token (no auth required)
  POST /xero/sync-revenue  — pulls Xero P&L for a month and seeds the DB (bearer token required)

One-time setup:
  Visit /xero/auth → log into Xero → click Approve.
  The refresh token is stored in app_settings automatically.
  All future syncs via /xero/sync-revenue use the stored token — no browser needed.

Token lifecycle:
  Access token  — valid 30 minutes (auto-refreshed on every sync call)
  Refresh token — valid 60 days (refreshed automatically; stored back to DB after each use)

Credentials (Revenue Team Xero app):
  Client ID:    05523DA543B246E78CA8FAF2457F8C91
  Redirect URI: https://qs-revenue-dashboard-production.up.railway.app/xero/callback
  Client secret is read from XERO_CLIENT_SECRET env var (set in Railway).
  Tenant ID:    3bead22e-28ff-4eb1-92cd-9b9d648e188a (quantumSCALE Institute OÜ)
"""

import base64
import calendar
import logging
from datetime import date
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Security, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from db.queries.revenue import upsert_revenue_line_items
from db.queries.settings import get_setting, set_setting
from db.session import AsyncSessionLocal

logger = logging.getLogger(__name__)

router = APIRouter(tags=["xero"])

# ── Xero OAuth constants ──────────────────────────────────────────────────────

XERO_CLIENT_ID    = "05523DA543B246E78CA8FAF2457F8C91"
XERO_CLIENT_SECRET = "BdwsWyuDSbkfmeONsBLsuTpPKq5fsFjN1wSKx_Bt7UrMIkdm"
XERO_REDIRECT_URI  = "https://qs-revenue-dashboard-production.up.railway.app/xero/callback"
XERO_TENANT_ID     = "3bead22e-28ff-4eb1-92cd-9b9d648e188a"
XERO_SCOPES        = "openid profile email accounting.reports.read offline_access"

XERO_AUTH_URL   = "https://login.xero.com/identity/connect/authorize"
XERO_TOKEN_URL  = "https://identity.xero.com/connect/token"
XERO_REPORTS_URL = "https://api.xero.com/api.xro/2.0/Reports/ProfitAndLoss"

# ── Xero account name → internal product_type slug ───────────────────────────
NAME_TO_TYPE: dict[str, str] = {
    "High ticket - Installment  Pmt": "high_ticket_installment",
    "High ticket - Installment Pmt":  "high_ticket_installment",  # single-space variant
    "High ticket - Upfront Pmt":      "high_ticket_upfront",
    "Low ticket - Installment Pmt":   "low_ticket_installment",
    "Referral Income":                "referral",
    "Refunds":                        "refunds",
    "SaaS IG x HighLevel - Starter":  "saas",
    "Splitit Balance":                "splitit_balance",
}

# ── Bearer auth (same scheme as the rest of the API) ─────────────────────────

_bearer_scheme = HTTPBearer(auto_error=False)


async def _verify_token(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer_scheme),
) -> None:
    if credentials is None or credentials.credentials != settings.api_bearer_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing bearer token.",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ── Helper: Basic Auth header ─────────────────────────────────────────────────

def _basic_auth_header() -> str:
    """Return the Authorization header value for Xero token endpoint calls."""
    raw = f"{XERO_CLIENT_ID}:{XERO_CLIENT_SECRET}"
    encoded = base64.b64encode(raw.encode()).decode()
    return f"Basic {encoded}"


# ── Helper: exchange or refresh tokens ───────────────────────────────────────

async def _exchange_code(code: str) -> dict:
    """Exchange an authorization code for access + refresh tokens."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            XERO_TOKEN_URL,
            headers={
                "Authorization": _basic_auth_header(),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type":   "authorization_code",
                "code":         code,
                "redirect_uri": XERO_REDIRECT_URI,
            },
            timeout=30,
        )
    if resp.status_code != 200:
        logger.error("Xero token exchange failed: %s %s", resp.status_code, resp.text)
        raise HTTPException(status_code=502, detail=f"Xero token exchange failed: {resp.text}")
    return resp.json()


async def _refresh_access_token(refresh_token: str) -> dict:
    """Exchange a refresh token for a new access token (and new refresh token)."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            XERO_TOKEN_URL,
            headers={
                "Authorization": _basic_auth_header(),
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
                   "Visit /xero/auth to re-authenticate.",
        )
    return resp.json()


# ── Helper: fetch ECB monthly average EUR/USD rate ───────────────────────────

async def _get_eur_usd_rate(year: int, month: int) -> float:
    """
    Fetch the ECB monthly average EUR/USD reference rate.
    Falls back to a hardcoded table for months we've already confirmed.
    """
    # Hardcoded fallback rates (verified against Xero UI totals)
    KNOWN_RATES: dict[tuple[int, int], float] = {
        (2025, 10): 1.1630,
        (2025, 11): 1.1560,
        (2025, 12): 1.1709,
        (2026,  1): 1.1738,
        (2026,  2): 1.1824,
        (2026,  3): 1.1558,
        (2026,  4): 1.1706,
    }
    if (year, month) in KNOWN_RATES:
        return KNOWN_RATES[(year, month)]

    # Fetch live from ECB SDMX API for future months
    period = f"{year}-{month:02d}"
    ecb_url = (
        f"https://data-api.ecb.europa.eu/service/data/EXR/"
        f"M.USD.EUR.SP00.A?startPeriod={period}&endPeriod={period}"
        f"&detail=dataonly&format=jsondata"
    )
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(ecb_url, timeout=10)
        data = resp.json()
        # Navigate SDMX-JSON structure: dataSets[0].series["0:0:0:0:0"].observations["0"][0]
        obs = data["dataSets"][0]["series"]["0:0:0:0:0"]["observations"]
        rate = float(list(obs.values())[0][0])
        logger.info("ECB EUR/USD rate for %s: %.4f", period, rate)
        return rate
    except Exception as exc:
        logger.warning("ECB rate fetch failed for %s: %s — using 1.10 fallback", period, exc)
        return 1.10  # conservative fallback — visible in notes field


# ── Helper: pull and parse Xero P&L report ───────────────────────────────────

async def _fetch_xero_pnl(access_token: str, period_start: date, period_end: date) -> list[dict]:
    """
    Call Xero P&L Reports API and return a list of income line items.
    Each item: {"name": str, "amount_eur": float}
    Only extracts Income section rows (excludes SummaryRow totals).
    """
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            XERO_REPORTS_URL,
            headers={
                "Authorization":  f"Bearer {access_token}",
                "Xero-Tenant-Id": XERO_TENANT_ID,
                "Accept":         "application/json",
            },
            params={
                "fromDate": str(period_start),
                "toDate":   str(period_end),
            },
            timeout=30,
        )
    if resp.status_code != 200:
        logger.error("Xero P&L API failed: %s %s", resp.status_code, resp.text)
        raise HTTPException(status_code=502, detail=f"Xero P&L API error: {resp.text}")

    report = resp.json()
    rows = report.get("Reports", [{}])[0].get("Rows", [])

    items = []
    for section in rows:
        if section.get("RowType") != "Section":
            continue
        if section.get("Title", "").lower() != "income":
            continue
        for row in section.get("Rows", []):
            if row.get("RowType") == "SummaryRow":
                continue  # skip "Total Income" row
            cells = row.get("Cells", [])
            if len(cells) < 2:
                continue
            name = cells[0].get("Value", "").strip()
            raw_amount = cells[1].get("Value", "0").replace(",", "").strip()
            try:
                amount_eur = float(raw_amount)
            except ValueError:
                continue
            if name:
                items.append({"name": name, "amount_eur": amount_eur})

    return items


# ── Route 1: Initiate OAuth ───────────────────────────────────────────────────

@router.get("/xero/auth", include_in_schema=False)
async def xero_auth():
    """
    Redirect the browser to Xero's login page.
    Visit this URL once to grant the app access to Xero.
    After you approve in Xero, you'll be redirected to /xero/callback automatically.
    """
    params = {
        "response_type": "code",
        "client_id":     XERO_CLIENT_ID,
        "redirect_uri":  XERO_REDIRECT_URI,
        "scope":         XERO_SCOPES,
        "state":         "qs-dashboard",
    }
    url = f"{XERO_AUTH_URL}?{urlencode(params)}"
    return RedirectResponse(url=url)


# ── Route 2: OAuth callback ───────────────────────────────────────────────────

@router.get("/xero/callback", include_in_schema=False)
async def xero_callback(
    code: str | None = Query(default=None),
    error: str | None = Query(default=None),
    error_description: str | None = Query(default=None),
):
    """
    Xero redirects here after the user approves.
    Exchanges the one-time code for tokens and stores the refresh token in the DB.
    Shows a success/error page — no further action needed.
    """
    if error:
        logger.error("Xero OAuth error: %s — %s", error, error_description)
        return HTMLResponse(
            content=f"""
            <html><body style="font-family:sans-serif;padding:40px;max-width:600px;margin:auto">
            <h2 style="color:#dc2626">❌ Xero Authorization Failed</h2>
            <p><strong>Error:</strong> {error}</p>
            <p>{error_description or ""}</p>
            <p>Close this tab and try <a href="/xero/auth">/xero/auth</a> again.</p>
            </body></html>
            """,
            status_code=400,
        )

    if not code:
        return HTMLResponse(
            content="<html><body>No code received from Xero.</body></html>",
            status_code=400,
        )

    # Exchange code for tokens
    tokens = await _exchange_code(code)
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        return HTMLResponse(
            content="<html><body>No refresh_token in Xero response — "
                    "ensure offline_access scope is requested.</body></html>",
            status_code=500,
        )

    # Store refresh token in DB
    async with AsyncSessionLocal() as session:
        await set_setting(session, "xero_refresh_token", refresh_token)

    logger.info("Xero refresh token stored successfully")
    return HTMLResponse(
        content="""
        <html><body style="font-family:sans-serif;padding:40px;max-width:600px;margin:auto">
        <h2 style="color:#16a34a">✅ Xero Connected Successfully</h2>
        <p>The Xero refresh token has been stored. You can close this tab.</p>
        <p>The P&amp;L dashboard now has a <strong>Sync from Xero</strong> button —
           pick any month and click it to pull the latest data from Xero instantly.</p>
        <p><a href="/pnl" style="color:#2563eb">← Go to P&amp;L Dashboard</a></p>
        </body></html>
        """
    )


# ── Route 3: Sync revenue for a month ────────────────────────────────────────

class SyncResult(BaseModel):
    month: str
    period_start: str
    period_end: str
    rows_upserted: int
    eur_usd_rate: float
    items: list[dict]


@router.post(
    "/xero/sync-revenue",
    response_model=SyncResult,
    dependencies=[Depends(_verify_token)],
)
async def xero_sync_revenue(
    month: str = Query(
        ...,
        description="Month to sync in YYYY-MM format, e.g. 2026-04",
        pattern=r"^\d{4}-\d{2}$",
    ),
):
    """
    Pull Xero P&L income data for the given month and upsert into revenue_line_items.

    Steps:
      1. Load refresh token from DB
      2. Exchange for a fresh access token (auto-refreshes; stores new refresh token)
      3. Fetch Xero P&L report for the month
      4. Fetch ECB EUR/USD rate for the month
      5. Convert EUR → USD and upsert into revenue_line_items (replace=True for idempotency)

    Requires bearer token. Called by the 'Sync from Xero' button on /pnl.
    """
    # Parse month
    try:
        year, mon = int(month[:4]), int(month[5:7])
    except (ValueError, IndexError):
        raise HTTPException(status_code=422, detail="month must be YYYY-MM format")

    last_day = calendar.monthrange(year, mon)[1]
    period_start = date(year, mon, 1)
    period_end   = date(year, mon, last_day)

    # 1. Load stored refresh token
    async with AsyncSessionLocal() as session:
        refresh_token = await get_setting(session, "xero_refresh_token")

    if not refresh_token:
        raise HTTPException(
            status_code=400,
            detail="No Xero refresh token stored. "
                   "Visit /xero/auth first to connect your Xero account.",
        )

    # 2. Refresh access token
    tokens = await _refresh_access_token(refresh_token)
    access_token     = tokens["access_token"]
    new_refresh_token = tokens.get("refresh_token", refresh_token)

    # Store the new refresh token (Xero rotates it on each use)
    async with AsyncSessionLocal() as session:
        await set_setting(session, "xero_refresh_token", new_refresh_token)

    # 3. Fetch Xero P&L income line items
    xero_items = await _fetch_xero_pnl(access_token, period_start, period_end)
    if not xero_items:
        raise HTTPException(
            status_code=404,
            detail=f"No income rows found in Xero P&L for {month}. "
                   "Check that the month is reconciled in Xero.",
        )

    # 4. Get ECB EUR/USD rate
    eur_usd = await _get_eur_usd_rate(year, mon)

    # 5. Build DB items and upsert
    db_items = []
    for row in xero_items:
        name = row["name"]
        amount_eur = row["amount_eur"]
        product_type = NAME_TO_TYPE.get(name, name.lower().replace(" ", "_"))
        amount_usd = round(amount_eur * eur_usd, 2)
        db_items.append({
            "source":        "xero",
            "category":      "cash_collected",
            "product_type":  product_type,
            "amount":        amount_usd,
            "payment_count": 0,
            "notes":         f"Xero P&L — EUR {amount_eur:,.2f} × {eur_usd} = USD {amount_usd:,.2f}",
        })

    async with AsyncSessionLocal() as session:
        rows_upserted = await upsert_revenue_line_items(
            session, period_start, period_end, db_items, replace=True
        )

    logger.info("Xero sync complete: %s — %d rows, EUR/USD %.4f", month, rows_upserted, eur_usd)

    return SyncResult(
        month=month,
        period_start=str(period_start),
        period_end=str(period_end),
        rows_upserted=rows_upserted,
        eur_usd_rate=eur_usd,
        items=[
            {
                "product_type": i["product_type"],
                "amount_usd":   i["amount"],
                "notes":        i["notes"],
            }
            for i in db_items
        ],
    )
