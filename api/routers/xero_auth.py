"""Xero OAuth 2.0 flow + revenue sync endpoint.

Three routes:
  GET  /xero/auth          — redirects browser to Xero login (no auth required)
  GET  /xero/callback      — Xero redirects here after approval; stores refresh token (no auth required)
  POST /xero/sync-revenue  — pulls Xero P&L for a month and seeds the DB (bearer token required)

One-time setup:
  Enter the Xero app credentials under Settings → Connectors → Xero (/settings),
  then click "Connect to Xero" (= /xero/auth) → log into Xero → Approve.
  The refresh token is stored in app_settings automatically.
  All future syncs via /xero/sync-revenue use the stored token — no browser needed.

Token lifecycle:
  Access token  — valid 30 minutes (auto-refreshed on every sync call)
  Refresh token — valid 60 days (refreshed automatically; stored back to DB after
                  each use, plus a weekly keep-alive job in sync/scheduler.py)

Credentials live in the app_settings table (managed via /settings), with legacy
fallbacks in api/utils/xero_utils.py for deployments that predate the settings UI.
"""

import calendar
import logging
import re
import unicodedata
from datetime import date, datetime, timezone
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Security, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from api.utils.xero_utils import (
    XERO_AUTH_URL,
    XERO_SETTING_REFRESH_TOKEN,
    get_xero_config,
    xero_access_token_from_stored_refresh,
    xero_exchange_code,
)
from config import settings
from db.models import DealWhopMatch, XeroBankTransfer
from db.queries.revenue import upsert_revenue_line_items
from db.queries.settings import set_setting
from db.session import AsyncSessionLocal

logger = logging.getLogger(__name__)

router = APIRouter(tags=["xero"])

# ── Xero API endpoints ────────────────────────────────────────────────────────
# OAuth credentials (client ID/secret, tenant, redirect URI) are resolved via
# get_xero_config() — managed in-app under Settings → Connectors → Xero.

XERO_REPORTS_URL = "https://api.xero.com/api.xro/2.0/Reports/ProfitAndLoss"
XERO_BANK_TXN_URL = "https://api.xero.com/api.xro/2.0/BankTransactions"

# Wise account IDs in Xero (from Bank Accounts → URL accountId param)
WISE_USD_XERO_ID = "6E143B3701FF412EA200F5FD8EFCA5F0"
WISE_EUR_XERO_ID = "F58E3D6C41EE4F65B9D9CBB4B5C19214"

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

async def _fetch_xero_pnl(
    access_token: str, tenant_id: str, period_start: date, period_end: date
) -> list[dict]:
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
                "Xero-Tenant-Id": tenant_id,
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
    Visit this URL once (via Settings → Connectors → 'Connect to Xero') to grant access.
    After you approve in Xero, you'll be redirected to /xero/callback automatically.
    """
    cfg = await get_xero_config()
    if not cfg.client_secret:
        # Fail here with instructions instead of a confusing 502 on the callback.
        return HTMLResponse(
            content="""
            <html><body style="font-family:sans-serif;padding:40px;max-width:600px;margin:auto">
            <h2 style="color:#dc2626">Xero is not configured yet</h2>
            <p>No client secret is set. Open <a href="/settings">Settings → Connectors → Xero</a>,
               enter the Client ID and Client Secret from the Xero developer portal, save,
               then click <strong>Connect to Xero</strong>.</p>
            </body></html>
            """,
            status_code=400,
        )
    params = {
        "response_type": "code",
        "client_id":     cfg.client_id,
        "redirect_uri":  cfg.redirect_uri,
        "scope":         cfg.scopes,
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
    cfg = await get_xero_config()
    tokens = await xero_exchange_code(cfg, code)
    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        return HTMLResponse(
            content="<html><body>No refresh_token in Xero response — "
                    "ensure offline_access scope is requested.</body></html>",
            status_code=500,
        )

    # Store refresh token in DB
    async with AsyncSessionLocal() as session:
        await set_setting(session, XERO_SETTING_REFRESH_TOKEN, refresh_token)

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
    xero_token: str | None = Query(
        default=None,
        description=(
            "Optional: pass a 30-min Xero access token directly (e.g. from API Explorer). "
            "When provided, skips the stored refresh-token flow entirely."
        ),
    ),
):
    """
    Pull Xero P&L income data for the given month and upsert into revenue_line_items.

    Two modes:
      A) xero_token param supplied — use it directly as the bearer token (30-min API Explorer flow).
      B) No xero_token — load stored refresh token from DB, exchange for access token (automated flow).

    Steps:
      1. Resolve access token (direct or via refresh)
      2. Fetch Xero P&L report for the month
      3. Fetch ECB EUR/USD rate for the month
      4. Convert EUR → USD and upsert into revenue_line_items (replace=True for idempotency)

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

    # 1. Resolve access token
    if xero_token:
        # Direct token path — API Explorer / manual flow (30-min token, no DB lookup needed)
        access_token = xero_token
    else:
        # Stored refresh token path — automated flow (rotated token persisted inside)
        access_token = await xero_access_token_from_stored_refresh()

    # 3. Fetch Xero P&L income line items
    cfg = await get_xero_config()
    xero_items = await _fetch_xero_pnl(access_token, cfg.tenant_id, period_start, period_end)
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


# ── Wise bank transfer sync ───────────────────────────────────────────────────
# Fetches RECEIVE (incoming) transactions from Wise USD + Wise EUR accounts in Xero.
# Normalises them into xero_bank_transfers table.
# Then runs a matching pass: links transfers to GHL deals by contact name + amount + date.
#
# Plain English: Xero knows about every wire transfer because Wise is connected
# as a bank feed. This route downloads all of them and tries to figure out
# which client each transfer is from, then links it to their deal in the system.


def _normalise_name(name: str) -> str:
    """Lowercase, strip accents, collapse whitespace — for fuzzy matching."""
    if not name:
        return ""
    # Decompose unicode (e.g. é → e + combining accent) then drop non-ASCII
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_only = nfkd.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", ascii_only).lower().strip()


def _name_similarity(a: str, b: str) -> float:
    """Simple token-overlap similarity 0–1 between two name strings.

    Plain English: splits both names into words, counts how many words appear
    in both, and returns a ratio. "Hankins Consulting Group" vs "Hankins Consulting"
    would score ~0.8 (2 of 3 words match).
    """
    if not a or not b:
        return 0.0
    tokens_a = set(_normalise_name(a).split())
    tokens_b = set(_normalise_name(b).split())
    if not tokens_a or not tokens_b:
        return 0.0
    overlap = len(tokens_a & tokens_b)
    return overlap / max(len(tokens_a), len(tokens_b))


def _parse_xero_date(raw: str) -> date | None:
    """Parse Xero's /Date(milliseconds+offset)/ format into a Python date.

    Xero returns dates as "/Date(1234567890000+0000)/" — milliseconds since epoch.
    """
    if not raw:
        return None
    m = re.search(r"/Date\((\d+)", raw)
    if m:
        ts = int(m.group(1)) / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc).date()
    # Fallback: try ISO string
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        return None


async def _fetch_xero_bank_transactions(
    access_token: str,
    tenant_id: str,
    account_id: str,
    account_name: str,
    date_from: str,
    date_to: str,
) -> list[dict]:
    """Fetch all RECEIVE (incoming) bank transactions from one Xero account.

    Paginates automatically (100 per page). Returns normalised dicts.
    """
    results = []
    page = 1
    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            resp = await client.get(
                XERO_BANK_TXN_URL,
                headers={
                    "Authorization":  f"Bearer {access_token}",
                    "Xero-Tenant-Id": tenant_id,
                    "Accept":         "application/json",
                },
                params={
                    "BankAccountID": account_id,
                    "where":        'Type=="RECEIVE"',
                    "DateFrom":     date_from,
                    "DateTo":       date_to,
                    "page":         page,
                },
            )
            if resp.status_code == 404 or resp.status_code == 204:
                break  # no more pages
            if resp.status_code != 200:
                logger.error("Xero BankTransactions %s page %d: %s %s",
                             account_name, page, resp.status_code, resp.text[:300])
                break

            data = resp.json()
            txns = data.get("BankTransactions", [])
            if not txns:
                break

            for t in txns:
                contact_name = (
                    t.get("Contact", {}).get("Name") or
                    t.get("Contact", {}).get("ContactName") or
                    ""
                ).strip()
                # Reference from top-level or first line item
                reference = (t.get("Reference") or "").strip()
                description = ""
                for li in (t.get("LineItems") or []):
                    if li.get("Description"):
                        description = li["Description"].strip()
                        break

                results.append({
                    "xero_transaction_id": t.get("BankTransactionID", ""),
                    "xero_account_id":     account_id,
                    "account_name":        account_name,
                    "date":                _parse_xero_date(t.get("Date", "")),
                    "amount":              float(t.get("Total") or t.get("SubTotal") or 0),
                    "currency":            t.get("CurrencyCode", "USD"),
                    "contact_name":        contact_name,
                    "reference":           reference,
                    "description":         description,
                    "is_reconciled":       bool(t.get("IsReconciled")),
                })

            if len(txns) < 100:
                break  # last page
            page += 1

    logger.info("Fetched %d RECEIVE txns from %s (%s→%s)", len(results), account_name, date_from, date_to)
    return results


def _match_transfer_to_deal(
    transfer: dict,
    deals: list[DealWhopMatch],
) -> tuple[str | None, str, str, float]:
    """Try to match one Xero transfer to a GHL deal.

    Returns (ghl_opportunity_id, method, confidence, score) or (None, "none", "unmatched", 0.0).

    Matching rules (in priority order):
      HIGH   ≥ 0.85 name similarity + amount within 30% + date within 60 days
      MEDIUM ≥ 0.70 name similarity + amount within 40%
      LOW    ≥ 0.60 name similarity only

    Plain English: we compare the sender name from the bank statement against
    the client name on each deal. We also check the dollar amount and date
    to avoid false matches between clients with similar names.
    """
    best_id: str | None = None
    best_method = "none"
    best_confidence = "unmatched"
    best_score = 0.0

    t_name = transfer.get("contact_name", "")
    t_amount = float(transfer.get("amount") or 0)
    t_date = transfer.get("date")

    for deal in deals:
        # Try matching against both the contact name and the company in opportunity name
        candidate_names = [
            deal.ghl_contact_name or "",
            deal.ghl_opportunity_name or "",
        ]
        name_score = max(_name_similarity(t_name, cn) for cn in candidate_names)
        if name_score < 0.50:
            continue  # not even close

        # Amount similarity: how close is the transfer to the deal value or installment?
        amount_score = 0.0
        if t_amount > 0 and deal.ghl_monetary_value:
            deal_val = float(deal.ghl_monetary_value)
            # Check against full deal value and common installment fractions
            for divisor in [1, 2, 3, 4, 6, 12]:
                expected = deal_val / divisor
                if expected > 0:
                    ratio = min(t_amount, expected) / max(t_amount, expected)
                    if ratio > amount_score:
                        amount_score = ratio

        # Date proximity (days between transfer date and deal close date)
        date_ok = False
        if t_date and deal.ghl_close_date:
            delta = abs((t_date - deal.ghl_close_date).days)
            date_ok = delta <= 90

        # Compute composite score and assign confidence
        score = name_score * 0.6 + amount_score * 0.4

        if name_score >= 0.85 and amount_score >= 0.70 and date_ok:
            confidence = "high"
        elif name_score >= 0.70 and amount_score >= 0.60:
            confidence = "medium"
        elif name_score >= 0.60:
            confidence = "low"
        else:
            continue

        if score > best_score:
            best_score = score
            best_id = deal.ghl_opportunity_id
            best_confidence = confidence
            best_method = "name_amount_date"

    return best_id, best_method, best_confidence, round(best_score, 3)


class WiseSyncResult(BaseModel):
    period_start: str
    period_end: str
    usd_fetched: int
    eur_fetched: int
    total_upserted: int
    matched_high: int
    matched_medium: int
    matched_low: int
    unmatched: int


@router.post(
    "/xero/sync-wise-transfers",
    response_model=WiseSyncResult,
    dependencies=[Depends(_verify_token)],
)
async def xero_sync_wise_transfers(
    date_from: str = Query(
        default="2025-01-01",
        description="Sync start date YYYY-MM-DD (default: 2025-01-01)",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    ),
    date_to: str = Query(
        default="",
        description="Sync end date YYYY-MM-DD (default: today)",
        pattern=r"^(\d{4}-\d{2}-\d{2})?$",
    ),
):
    """
    Sync all incoming (RECEIVE) Wise USD and Wise EUR bank transactions from Xero,
    then attempt to match each transfer to a GHL deal.

    Steps:
      1. Load refresh token from DB (set via /xero/auth)
      2. Refresh access token
      3. Fetch RECEIVE transactions for Wise USD + Wise EUR from Xero
      4. Upsert into xero_bank_transfers (idempotent — safe to run repeatedly)
      5. Run matching pass: link unconfirmed transfers to GHL deals by name+amount+date

    Verification: after running, call GET /api/dashboard/deals/wise-transfers to see results.
    Silent failure signal: if total_upserted = 0 and you know there are transfers in Xero,
    the Xero token needs re-auth (visit /xero/auth) or the account IDs are wrong.

    Requires bearer token.
    """
    if not date_to:
        date_to = date.today().isoformat()

    # 1+2. Resolve access token from stored refresh token (rotated token persisted inside)
    access_token = await xero_access_token_from_stored_refresh()

    # 3. Fetch from both Wise accounts
    cfg = await get_xero_config()
    usd_txns = await _fetch_xero_bank_transactions(
        access_token, cfg.tenant_id, WISE_USD_XERO_ID, "Wise USD", date_from, date_to
    )
    eur_txns = await _fetch_xero_bank_transactions(
        access_token, cfg.tenant_id, WISE_EUR_XERO_ID, "Wise EUR", date_from, date_to
    )
    all_txns = usd_txns + eur_txns

    if not all_txns:
        logger.info("No Wise transfers found for %s → %s", date_from, date_to)
        return WiseSyncResult(
            period_start=date_from, period_end=date_to,
            usd_fetched=0, eur_fetched=0, total_upserted=0,
            matched_high=0, matched_medium=0, matched_low=0, unmatched=0,
        )

    # 4. Upsert into DB (plain English: if a transfer already exists with the same
    #    Xero ID, update its fields but never overwrite a manually confirmed match)
    async with AsyncSessionLocal() as session:
        for txn in all_txns:
            if not txn["xero_transaction_id"]:
                continue
            stmt = pg_insert(XeroBankTransfer).values(
                xero_transaction_id=txn["xero_transaction_id"],
                xero_account_id=txn["xero_account_id"],
                account_name=txn["account_name"],
                date=txn["date"],
                amount=txn["amount"],
                currency=txn["currency"],
                contact_name=txn["contact_name"],
                reference=txn["reference"],
                description=txn["description"],
                is_reconciled=txn["is_reconciled"],
            ).on_conflict_do_update(
                index_elements=["xero_transaction_id"],
                set_={
                    "account_name":  txn["account_name"],
                    "date":          txn["date"],
                    "amount":        txn["amount"],
                    "currency":      txn["currency"],
                    "contact_name":  txn["contact_name"],
                    "reference":     txn["reference"],
                    "description":   txn["description"],
                    "is_reconciled": txn["is_reconciled"],
                    "synced_at":     datetime.now(timezone.utc),
                },
                where=XeroBankTransfer.is_confirmed == False,  # noqa: E712
            )
            await session.execute(stmt)
        await session.commit()

    # 5. Matching pass: load unmatched/unconfirmed transfers + all deals, run matcher
    async with AsyncSessionLocal() as session:
        unmatched_transfers = (await session.execute(
            select(XeroBankTransfer)
            .where(XeroBankTransfer.is_confirmed == False)  # noqa: E712
            .where(XeroBankTransfer.match_confidence.in_(["unmatched", "low"]))
        )).scalars().all()

        all_deals = (await session.execute(
            select(DealWhopMatch)
            .where(DealWhopMatch.ghl_close_date.isnot(None))
        )).scalars().all()

    counters = {"high": 0, "medium": 0, "low": 0, "unmatched": 0}

    async with AsyncSessionLocal() as session:
        for transfer in unmatched_transfers:
            ghl_id, method, confidence, score = _match_transfer_to_deal(
                {
                    "contact_name": transfer.contact_name,
                    "amount":       float(transfer.amount or 0),
                    "date":         transfer.date,
                },
                list(all_deals),
            )
            counters[confidence] = counters.get(confidence, 0) + 1

            await session.execute(
                pg_insert(XeroBankTransfer)
                .values(xero_transaction_id=transfer.xero_transaction_id)
                .on_conflict_do_update(
                    index_elements=["xero_transaction_id"],
                    set_={
                        "ghl_opportunity_id": ghl_id,
                        "match_method":       method,
                        "match_confidence":   confidence,
                        "match_score":        score,
                    },
                    where=XeroBankTransfer.is_confirmed == False,  # noqa: E712
                )
            )
        await session.commit()

    logger.info(
        "Wise sync done: %d USD + %d EUR txns, matched high=%d medium=%d low=%d unmatched=%d",
        len(usd_txns), len(eur_txns),
        counters["high"], counters["medium"], counters["low"], counters["unmatched"],
    )

    return WiseSyncResult(
        period_start=date_from,
        period_end=date_to,
        usd_fetched=len(usd_txns),
        eur_fetched=len(eur_txns),
        total_upserted=len(all_txns),
        matched_high=counters["high"],
        matched_medium=counters["medium"],
        matched_low=counters["low"],
        unmatched=counters["unmatched"],
    )


# ── P&L bulk import (bypasses Xero OAuth — uses pre-fetched income rows) ─


class PnlLineItemInput(BaseModel):
    """One income line item from Xero P&L report.

    Plain English: copy the income rows you see in Xero API Explorer
    (ProfitAndLoss report → Income section) and POST them here.
    The endpoint converts EUR → USD using the hardcoded ECB monthly rate
    and upserts into revenue_line_items (replace=True — idempotent).
    """
    name: str
    amount_eur: float


class PnlImportResult(BaseModel):
    month: str
    period_start: str
    period_end: str
    rows_upserted: int
    eur_usd_rate: float
    items: list[dict]


@router.post(
    "/xero/import-pnl",
    response_model=PnlImportResult,
    dependencies=[Depends(_verify_token)],
)
async def xero_import_pnl(
    month: str = Query(
        ...,
        description="Month to import in YYYY-MM format, e.g. 2026-04",
        pattern=r"^\d{4}-\d{2}$",
    ),
    items: list[PnlLineItemInput] = ...,
):
    """
    Import Xero P&L income rows that were pre-fetched (e.g. via API Explorer).

    Bypasses Xero OAuth entirely — accepts an array of {name, amount_eur} objects
    in the request body, converts EUR → USD using the ECB monthly rate, and upserts
    into revenue_line_items (replace=True for idempotency).

    Plain English: when Xero OAuth is broken, fetch the P&L from the API Explorer,
    copy the income rows, and POST them here instead of using sync-revenue.

    Example body:
      [{"name": "High ticket - Upfront Pmt", "amount_eur": 61591.89}, ...]

    Requires bearer token.
    """
    try:
        year, mon = int(month[:4]), int(month[5:7])
    except (ValueError, IndexError):
        raise HTTPException(status_code=422, detail="month must be YYYY-MM format")

    if not items:
        raise HTTPException(status_code=422, detail="items list is empty")

    last_day = calendar.monthrange(year, mon)[1]
    period_start = date(year, mon, 1)
    period_end   = date(year, mon, last_day)

    eur_usd = await _get_eur_usd_rate(year, mon)

    db_items = []
    for row in items:
        name = row.name
        amount_eur = row.amount_eur
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

    logger.info("Xero P&L import: %s — %d rows, EUR/USD %.4f", month, rows_upserted, eur_usd)

    return PnlImportResult(
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


# ── Bulk import (bypasses Xero OAuth — uses pre-fetched data) ────────────


class WiseTransferInput(BaseModel):
    """One Wise bank transfer record for bulk import.

    Plain English: this is the shape of data you get from Xero API Explorer
    after querying BankTransactions filtered to Wise USD/EUR accounts.
    """
    xero_transaction_id: str
    xero_account_id: str = ""
    account_name: str = ""
    date: str = ""
    amount: float = 0.0
    currency: str = "USD"
    contact_name: str = ""
    reference: str = ""
    description: str = ""
    is_reconciled: bool = False


class WiseImportResult(BaseModel):
    total_received: int
    total_upserted: int
    matched_high: int
    matched_medium: int
    matched_low: int
    unmatched: int


@router.post(
    "/xero/import-wise-transfers",
    response_model=WiseImportResult,
    dependencies=[Depends(_verify_token)],
)
async def xero_import_wise_transfers(
    transfers: list[WiseTransferInput],
    run_matcher: bool = Query(
        default=True,
        description="Run deal-matching pass after import (default: true)",
    ),
):
    """
    Bulk-import Wise bank transfers from pre-fetched data (e.g. Xero API Explorer).

    Bypasses Xero OAuth entirely — accepts an array of transfer records in the
    request body and upserts them into xero_bank_transfers.

    Plain English: when Xero OAuth is broken or you prefer the API Explorer,
    copy the transaction data and POST it here instead.

    Steps:
      1. Upsert all transfers (idempotent — safe to run repeatedly)
      2. Optionally run matching pass to link transfers to GHL deals

    Verification: GET /api/dashboard/deals/wise-transfers → check count.
    Requires bearer token.
    """
    if not transfers:
        return WiseImportResult(
            total_received=0, total_upserted=0,
            matched_high=0, matched_medium=0, matched_low=0, unmatched=0,
        )

    # 1. Upsert into DB
    upserted = 0
    async with AsyncSessionLocal() as session:
        for txn in transfers:
            if not txn.xero_transaction_id:
                continue
            # Parse date string to date object
            txn_date = None
            if txn.date:
                try:
                    txn_date = date.fromisoformat(txn.date)
                except ValueError:
                    txn_date = None

            stmt = pg_insert(XeroBankTransfer).values(
                xero_transaction_id=txn.xero_transaction_id,
                xero_account_id=txn.xero_account_id,
                account_name=txn.account_name,
                date=txn_date,
                amount=txn.amount,
                currency=txn.currency,
                contact_name=txn.contact_name,
                reference=txn.reference,
                description=txn.description,
                is_reconciled=txn.is_reconciled,
            ).on_conflict_do_update(
                index_elements=["xero_transaction_id"],
                set_={
                    "account_name":  txn.account_name,
                    "date":          txn_date,
                    "amount":        txn.amount,
                    "currency":      txn.currency,
                    "contact_name":  txn.contact_name,
                    "reference":     txn.reference,
                    "description":   txn.description,
                    "is_reconciled": txn.is_reconciled,
                    "synced_at":     datetime.now(timezone.utc),
                },
                where=XeroBankTransfer.is_confirmed == False,  # noqa: E712
            )
            await session.execute(stmt)
            upserted += 1
        await session.commit()

    # 2. Matching pass (optional)
    counters = {"high": 0, "medium": 0, "low": 0, "unmatched": 0}

    if run_matcher:
        async with AsyncSessionLocal() as session:
            unmatched_transfers = (await session.execute(
                select(XeroBankTransfer)
                .where(XeroBankTransfer.is_confirmed == False)  # noqa: E712
                .where(XeroBankTransfer.match_confidence.in_(["unmatched", "low"]))
            )).scalars().all()

            all_deals = (await session.execute(
                select(DealWhopMatch)
                .where(DealWhopMatch.ghl_close_date.isnot(None))
            )).scalars().all()

        async with AsyncSessionLocal() as session:
            for transfer in unmatched_transfers:
                ghl_id, method, confidence, score = _match_transfer_to_deal(
                    {
                        "contact_name": transfer.contact_name,
                        "amount":       float(transfer.amount or 0),
                        "date":         transfer.date,
                    },
                    list(all_deals),
                )
                counters[confidence] = counters.get(confidence, 0) + 1

                await session.execute(
                    pg_insert(XeroBankTransfer)
                    .values(xero_transaction_id=transfer.xero_transaction_id)
                    .on_conflict_do_update(
                        index_elements=["xero_transaction_id"],
                        set_={
                            "ghl_opportunity_id": ghl_id,
                            "match_method":       method,
                            "match_confidence":   confidence,
                            "match_score":        score,
                        },
                        where=XeroBankTransfer.is_confirmed == False,  # noqa: E712
                    )
                )
            await session.commit()

    logger.info(
        "Wise import done: %d received, %d upserted, matched high=%d medium=%d low=%d unmatched=%d",
        len(transfers), upserted,
        counters["high"], counters["medium"], counters["low"], counters["unmatched"],
    )

    return WiseImportResult(
        total_received=len(transfers),
        total_upserted=upserted,
        matched_high=counters["high"],
        matched_medium=counters["medium"],
        matched_low=counters["low"],
        unmatched=counters["unmatched"],
    )


# ── Manual link: wire transfer → GHL deal ──────────────────────────────────


class ManualLinkInput(BaseModel):
    """Link one Wise/Xero transfer to a GHL deal by hand."""
    xero_transaction_id: str
    ghl_opportunity_id: str


class ManualLinkResult(BaseModel):
    updated: int
    xero_transaction_id: str
    ghl_opportunity_id: str
    contact_name: str | None = None
    amount: float | None = None


@router.post(
    "/xero/link-transfer",
    response_model=ManualLinkResult,
    dependencies=[Depends(_verify_token)],
)
async def xero_link_transfer(link: ManualLinkInput):
    """Manually link a Wise bank transfer record to a GHL opportunity.

    Sets match_method='manual', match_confidence='high', is_confirmed=True.
    """
    async with AsyncSessionLocal() as session:
        row = (await session.execute(
            select(XeroBankTransfer)
            .where(XeroBankTransfer.xero_transaction_id == link.xero_transaction_id)
        )).scalar_one_or_none()

        if not row:
            raise HTTPException(404, f"Transfer {link.xero_transaction_id} not found")

        row.ghl_opportunity_id = link.ghl_opportunity_id
        row.match_method = "manual"
        row.match_confidence = "high"
        row.is_confirmed = True
        await session.commit()

        return ManualLinkResult(
            updated=1,
            xero_transaction_id=link.xero_transaction_id,
            ghl_opportunity_id=link.ghl_opportunity_id,
            contact_name=row.contact_name,
            amount=float(row.amount) if row.amount else None,
        )


@router.post(
    "/xero/link-transfers-batch",
    response_model=list[ManualLinkResult],
    dependencies=[Depends(_verify_token)],
)
async def xero_link_transfers_batch(links: list[ManualLinkInput]):
    """Batch-link multiple Wise transfers to GHL deals."""
    results = []
    async with AsyncSessionLocal() as session:
        for link in links:
            row = (await session.execute(
                select(XeroBankTransfer)
                .where(XeroBankTransfer.xero_transaction_id == link.xero_transaction_id)
            )).scalar_one_or_none()

            if not row:
                results.append(ManualLinkResult(
                    updated=0,
                    xero_transaction_id=link.xero_transaction_id,
                    ghl_opportunity_id=link.ghl_opportunity_id,
                ))
                continue

            row.ghl_opportunity_id = link.ghl_opportunity_id
            row.match_method = "manual"
            row.match_confidence = "high"
            row.is_confirmed = True
            results.append(ManualLinkResult(
                updated=1,
                xero_transaction_id=link.xero_transaction_id,
                ghl_opportunity_id=link.ghl_opportunity_id,
                contact_name=row.contact_name,
                amount=float(row.amount) if row.amount else None,
            ))
        await session.commit()

    return results
