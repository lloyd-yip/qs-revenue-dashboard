"""Xero P&L expense sync — per-payee detail from bank transactions.

Replicates the historical "actual SPEND (cash basis)" load pattern: for each
P&L expense account, expenses are broken down per payee (bank-transaction
Contact) so the dashboard shows people/vendors (Ryan Matsumori, Facebook,
OPENAI…) rather than account totals. Accounts with no bank-transaction detail
fall back to one account-level row — same as historical months.

Uses the stored Xero connection from Settings → Connectors. Payee aliases
(e.g. Armando's LLC) are canonicalised via db/queries/vendor_classification.py.
"""

import calendar
import logging
from collections import defaultdict
from datetime import date

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from api.utils.xero_utils import (
    get_eur_usd_rate,
    get_xero_config,
    verify_bearer,
    xero_access_token_from_stored_refresh,
)
from db.queries.expenses import upsert_expense_line_items
from db.queries.vendor_classification import canonicalise_vendor, classify_vendor
from db.session import AsyncSessionLocal

logger = logging.getLogger(__name__)
router = APIRouter(tags=["xero"])

XERO_PNL_URL      = "https://api.xero.com/api.xro/2.0/Reports/ProfitAndLoss"
XERO_ACCOUNTS_URL = "https://api.xero.com/api.xro/2.0/Accounts"
XERO_BANK_TXN_URL = "https://api.xero.com/api.xro/2.0/BankTransactions"


class _ScopeMissing(Exception):
    """Raised when the stored token lacks a scope (user must reconnect)."""


class ExpenseSyncResult(BaseModel):
    month: str
    period_start: str
    period_end: str
    rows_upserted: int
    total_usd: float
    eur_usd_rate: float
    skipped_unclassified: list[dict]  # [{vendor, section, amount_usd}] — add to vendor_classification.py
    skipped_non_revenue: int
    account_level_accounts: list[str]  # accounts with no bank-transaction detail
    warnings: list[str]


def _xero_headers(access_token: str, tenant_id: str) -> dict:
    return {
        "Authorization":  f"Bearer {access_token}",
        "Xero-Tenant-Id": tenant_id,
        "Accept":         "application/json",
    }


async def _fetch_pnl_expense_rows(
    access_token: str, tenant_id: str, period_start: date, period_end: date
) -> list[dict]:
    """Return all non-Income P&L line items: {"section", "name", "amount_eur"}."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            XERO_PNL_URL,
            headers=_xero_headers(access_token, tenant_id),
            params={"fromDate": str(period_start), "toDate": str(period_end)},
            timeout=30,
        )
    if resp.status_code != 200:
        logger.error("Xero P&L API failed: %s %s", resp.status_code, resp.text)
        raise HTTPException(status_code=502, detail=f"Xero P&L API error: {resp.text[:300]}")

    rows = resp.json().get("Reports", [{}])[0].get("Rows", [])
    items = []
    for section in rows:
        if section.get("RowType") != "Section":
            continue
        title = section.get("Title", "").strip()
        if not title:
            continue  # untitled sections wrap Gross Profit / Net Profit summary lines
        if title.lower() == "income":
            continue  # revenue side handled by /xero/sync-revenue
        for row in section.get("Rows", []):
            if row.get("RowType") in ("SummaryRow", "Header"):
                continue
            cells = row.get("Cells", [])
            if len(cells) < 2:
                continue
            name = cells[0].get("Value", "").strip()
            raw  = cells[1].get("Value", "0").replace(",", "").strip()
            if not name or not raw:
                continue
            try:
                amount = float(raw)
            except ValueError:
                continue
            if amount == 0:
                continue
            items.append({"section": title, "name": name, "amount_eur": amount})
    return items


async def _fetch_expense_account_names(access_token: str, tenant_id: str) -> dict[str, str]:
    """Return {account_code: account_name} for EXPENSE-class accounts.

    Requires accounting.settings.read — raises _ScopeMissing on 403 so the
    caller can degrade to account-level rows.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            XERO_ACCOUNTS_URL,
            headers=_xero_headers(access_token, tenant_id),
            params={"where": 'Class=="EXPENSE"'},
            timeout=30,
        )
    if resp.status_code in (401, 403):
        # The access token just worked for the P&L call, so 401/403 here means
        # this token predates the accounting.settings.read scope grant.
        raise _ScopeMissing("accounting.settings.read not granted")
    if resp.status_code != 200:
        logger.error("Xero Accounts API failed: %s %s", resp.status_code, resp.text)
        raise HTTPException(status_code=502, detail=f"Xero Accounts API error: {resp.text[:300]}")
    return {
        a["Code"]: a["Name"].strip()
        for a in resp.json().get("Accounts", [])
        if a.get("Code") and a.get("Name")
    }


async def _fetch_spend_detail(
    access_token: str,
    tenant_id: str,
    account_names: dict[str, str],
    year: int,
    mon: int,
    eur_usd: float,
) -> dict[str, dict[str, float]]:
    """Return {account_name: {payee: usd_amount}} from SPEND bank transactions.

    Line-item level so split transactions land on the right accounts. USD
    transactions keep their native amount; EUR converts at the monthly rate;
    other currencies go via the transaction's base-currency rate.
    """
    next_y, next_m = (year + 1, 1) if mon == 12 else (year, mon + 1)
    where = (
        f'Type=="SPEND" AND Status=="AUTHORISED" '
        f"AND Date >= DateTime({year},{mon:02d},01) "
        f"AND Date < DateTime({next_y},{next_m:02d},01)"
    )

    detail: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    page = 1
    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            resp = await client.get(
                XERO_BANK_TXN_URL,
                headers=_xero_headers(access_token, tenant_id),
                params={"where": where, "page": page},
            )
            if resp.status_code in (204, 404):
                break
            if resp.status_code != 200:
                logger.error("Xero BankTransactions API failed: %s %s", resp.status_code, resp.text)
                raise HTTPException(
                    status_code=502, detail=f"Xero BankTransactions API error: {resp.text[:300]}"
                )
            txns = resp.json().get("BankTransactions", [])
            if not txns:
                break

            for txn in txns:
                payee    = (txn.get("Contact") or {}).get("Name", "").strip() or "Unknown payee"
                currency = txn.get("CurrencyCode", "EUR")
                rate     = float(txn.get("CurrencyRate") or 1) or 1  # foreign units per EUR
                for line in txn.get("LineItems", []):
                    code   = str(line.get("AccountCode") or "")
                    amount = float(line.get("LineAmount") or 0)
                    if not code or code not in account_names or amount == 0:
                        continue
                    if currency == "USD":
                        usd = amount
                    elif currency == "EUR":
                        usd = amount * eur_usd
                    else:
                        usd = (amount / rate) * eur_usd
                    detail[account_names[code]][payee] += usd

            if len(txns) < 100:
                break
            page += 1
    return detail


@router.post(
    "/xero/sync-expenses",
    response_model=ExpenseSyncResult,
    dependencies=[Depends(verify_bearer)],
)
async def xero_sync_expenses(
    month: str = Query(..., pattern=r"^\d{4}-\d{2}$",
                       description="Month in YYYY-MM format, e.g. 2026-06"),
    xero_token: str | None = Query(
        default=None,
        description="Optional: Xero access token from API Explorer. "
                    "Omit to use the stored Xero connection (Settings → Connectors).",
    ),
) -> ExpenseSyncResult:
    """Pull Xero expenses for a month into expense_line_items (replace per period).

    Per-payee rows from SPEND bank transactions where detail exists ("actual
    SPEND (cash basis)" — matches historical months); account-level fallback
    otherwise. non_revenue vendors hidden by design; unclassified reported.
    """
    try:
        year, mon = int(month[:4]), int(month[5:7])
        if not (1 <= mon <= 12):
            raise ValueError
    except (ValueError, IndexError):
        raise HTTPException(status_code=422, detail="month must be YYYY-MM format")

    last_day     = calendar.monthrange(year, mon)[1]
    period_start = date(year, mon, 1)
    period_end   = date(year, mon, last_day)

    access_token = xero_token or await xero_access_token_from_stored_refresh()
    cfg = await get_xero_config()

    pnl_rows = await _fetch_pnl_expense_rows(access_token, cfg.tenant_id, period_start, period_end)
    if not pnl_rows:
        raise HTTPException(
            status_code=404,
            detail=f"No expense rows found in Xero P&L for {month}. "
                   "Check that the month has data (and is reconciled) in Xero.",
        )

    eur_usd = get_eur_usd_rate(year, mon)
    warnings: list[str] = []

    # Per-payee detail from bank transactions; degrade gracefully if the stored
    # token predates the accounting.settings.read scope (needs one reconnect).
    detail: dict[str, dict[str, float]] = {}
    try:
        account_names = await _fetch_expense_account_names(access_token, cfg.tenant_id)
        detail = await _fetch_spend_detail(
            access_token, cfg.tenant_id, account_names, year, mon, eur_usd
        )
    except _ScopeMissing:
        warnings.append(
            "Per-vendor detail unavailable: the Xero connection predates the "
            "chart-of-accounts scope. Go to Settings → Connect to Xero once more, "
            "then re-sync this month."
        )

    items: list[dict] = []
    unclassified: list[dict] = []
    account_level: list[str] = []
    non_revenue_count = 0

    for row in pnl_rows:
        account = row["name"]
        account_bucket = classify_vendor(account)
        if account_bucket == "non_revenue":
            non_revenue_count += 1
            continue  # delivery-team / bank costs — hidden from the dashboard by design

        payees = detail.get(account)
        if payees:
            for payee, usd_raw in sorted(payees.items(), key=lambda kv: -kv[1]):
                vendor = canonicalise_vendor(payee)
                usd    = round(abs(usd_raw), 2)
                if usd == 0:
                    continue
                # Unknown payees inherit the account's bucket (May pattern);
                # unclassified only when the account itself is unknown too.
                bucket = classify_vendor(vendor, account_bucket)
                if bucket == "non_revenue":
                    non_revenue_count += 1
                    continue
                if bucket == "unclassified":
                    unclassified.append({"vendor": vendor, "section": account, "amount_usd": usd})
                    continue
                items.append({
                    "bucket": bucket,
                    "vendor": vendor,
                    "amount": usd,
                    "notes":  f"{account} — actual SPEND (cash basis)",
                })
        else:
            usd = round(abs(row["amount_eur"]) * eur_usd, 2)
            if account_bucket == "unclassified":
                unclassified.append({"vendor": account, "section": row["section"], "amount_usd": usd})
                continue
            account_level.append(account)
            items.append({
                "bucket": account_bucket,
                "vendor": account,
                "amount": usd,
                "notes":  f"{account} — account-level (no bank-transaction detail)",
            })

    async with AsyncSessionLocal() as session:
        rows_upserted = await upsert_expense_line_items(
            session, period_start, period_end, items, replace=True
        )

    total_usd = round(sum(i["amount"] for i in items), 2)
    logger.info(
        "xero_sync_expenses: month=%s rows=%d total_usd=%.2f detail_accounts=%d "
        "account_level=%d unclassified=%d non_revenue=%d",
        month, rows_upserted, total_usd, len(detail), len(account_level),
        len(unclassified), non_revenue_count,
    )

    return ExpenseSyncResult(
        month=month,
        period_start=str(period_start),
        period_end=str(period_end),
        rows_upserted=rows_upserted,
        total_usd=total_usd,
        eur_usd_rate=eur_usd,
        skipped_unclassified=unclassified,
        skipped_non_revenue=non_revenue_count,
        account_level_accounts=account_level,
        warnings=warnings,
    )
