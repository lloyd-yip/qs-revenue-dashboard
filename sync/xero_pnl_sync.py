"""Faithful Xero P&L sync — mirrors the whole Profit & Loss report per month.

Stores EVERY income and expense account (nothing dropped) into xero_pnl_lines so the
Company dashboard's Revenue / Expenses / Net Profit match Xero exactly. This is
separate from the marketing/sales expense view (expense_line_items), which stays as-is.

Runs against the stored Xero refresh token (unattended-capable), so the monthly cron
can pull the previous, now-reconciled month with no human token paste.

Verification: after POST /xero/sync-pnl?month=YYYY-MM, GET the Company dashboard —
net for that month should equal Xero's Net Profit for the same month.
"""

import calendar
import logging
from datetime import date, datetime, timezone

import httpx
from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert as pg_insert

from api.utils.xero_utils import (
    get_eur_usd_rate,
    get_xero_config,
    xero_access_token_from_stored_refresh,
)
from db.models import XeroPnlLine, XeroPnlLineItem
from db.session import AsyncSessionLocal

logger = logging.getLogger(__name__)

XERO_PNL_URL = "https://api.xero.com/api.xro/2.0/Reports/ProfitAndLoss"


def _month_bounds(month: str) -> tuple[date, date]:
    year, mon = (int(x) for x in month.split("-"))
    return date(year, mon, 1), date(year, mon, calendar.monthrange(year, mon)[1])


async def _fetch_full_pnl(access_token: str, tenant_id: str, ps: date, pe: date) -> list[dict]:
    """Parse the ENTIRE P&L report → [{section, account, is_income, amount_eur}].

    is_income = the section title contains 'income' (covers 'Income' / 'Trading Income'
    / 'Other Income'); every other titled section is an expense section (Cost of Sales,
    Operating Expenses, …). Untitled sections wrap the Gross/Net Profit summary lines
    and are skipped. Amounts are the org currency (EUR); the caller converts to USD.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            XERO_PNL_URL,
            headers={"Authorization": f"Bearer {access_token}", "Xero-Tenant-Id": tenant_id,
                     "Accept": "application/json"},
            params={"fromDate": str(ps), "toDate": str(pe)},
            timeout=30,
        )
    if resp.status_code != 200:
        logger.error("Xero P&L API failed: %s %s", resp.status_code, resp.text[:300])
        raise RuntimeError(f"Xero P&L API error {resp.status_code}: {resp.text[:200]}")

    rows = resp.json().get("Reports", [{}])[0].get("Rows", [])
    out: list[dict] = []
    for section in rows:
        if section.get("RowType") != "Section":
            continue
        title = (section.get("Title") or "").strip()
        if not title:
            continue  # summary wrappers (Gross Profit / Net Profit)
        is_income = "income" in title.lower()
        for row in section.get("Rows", []):
            if row.get("RowType") in ("SummaryRow", "Header"):
                continue
            cells = row.get("Cells", [])
            if len(cells) < 2:
                continue
            name = (cells[0].get("Value") or "").strip()
            raw = (cells[1].get("Value") or "0").replace(",", "").strip()
            if not name or not raw:
                continue
            try:
                amount = float(raw)
            except ValueError:
                continue
            if amount == 0:
                continue
            out.append({"section": title, "account": name, "is_income": is_income, "amount_eur": amount})
    return out


async def sync_xero_pnl(month: str, xero_token: str | None = None) -> dict:
    """Pull one month's full Xero P&L and upsert into xero_pnl_lines (idempotent).

    Category-scoped delete for the month first, so a re-sync reflects voided/edited
    lines exactly. Returns totals so the caller can verify against Xero.
    """
    ps, pe = _month_bounds(month)
    access_token = xero_token or await xero_access_token_from_stored_refresh()
    cfg = await get_xero_config()
    eur_usd = get_eur_usd_rate(ps.year, ps.month)

    lines = await _fetch_full_pnl(access_token, cfg.tenant_id, ps, pe)

    income_usd = expense_usd = 0.0
    async with AsyncSessionLocal() as session:
        await session.execute(
            delete(XeroPnlLine).where(XeroPnlLine.period_start == ps, XeroPnlLine.period_end == pe)
        )
        for ln in lines:
            usd = round(ln["amount_eur"] * eur_usd, 2)
            if ln["is_income"]:
                income_usd += usd
            else:
                expense_usd += usd
            await session.execute(pg_insert(XeroPnlLine).values(
                period_start=ps, period_end=pe, section=ln["section"], account=ln["account"],
                is_income=ln["is_income"], amount_usd=usd, amount_eur=round(ln["amount_eur"], 2),
                eur_usd=round(eur_usd, 4),
            ).on_conflict_do_update(
                index_elements=["period_start", "period_end", "section", "account"],
                set_={"is_income": ln["is_income"], "amount_usd": usd,
                      "amount_eur": round(ln["amount_eur"], 2), "eur_usd": round(eur_usd, 4),
                      "synced_at": datetime.now(tz=timezone.utc)},
            ))
        await session.commit()

    # Per-payee detail for each expense account (so the P&L can drill into an account).
    # Best-effort: needs accounting.settings.read + bank SPEND data; degrades to no detail.
    detail_rows = 0
    try:
        from api.routers.xero_expenses import (
            _ScopeMissing, _fetch_expense_account_names, _fetch_spend_detail,
        )
        account_names = await _fetch_expense_account_names(access_token, cfg.tenant_id)
        detail = await _fetch_spend_detail(access_token, cfg.tenant_id, account_names, ps.year, ps.month, eur_usd)
        async with AsyncSessionLocal() as session:
            await session.execute(
                delete(XeroPnlLineItem).where(
                    XeroPnlLineItem.period_start == ps, XeroPnlLineItem.period_end == pe)
            )
            for account, payees in detail.items():
                for payee, usd in payees.items():
                    if not payee or round(usd, 2) == 0:
                        continue
                    await session.execute(pg_insert(XeroPnlLineItem).values(
                        period_start=ps, period_end=pe, account=account, payee=payee[:300],
                        amount_usd=round(usd, 2),
                    ).on_conflict_do_update(
                        index_elements=["period_start", "period_end", "account", "payee"],
                        set_={"amount_usd": round(usd, 2), "synced_at": datetime.now(tz=timezone.utc)},
                    ))
                    detail_rows += 1
            await session.commit()
    except _ScopeMissing:
        logger.info("Xero P&L %s: payee detail skipped (accounting.settings.read not granted)", month)
    except Exception as exc:
        logger.warning("Xero P&L %s: payee detail failed (%s) — account totals still stored", month, exc)

    income_usd, expense_usd = round(income_usd, 2), round(expense_usd, 2)
    result = {"ok": True, "month": month, "lines": len(lines), "detail_rows": detail_rows,
              "eur_usd": round(eur_usd, 4), "income_usd": income_usd, "expenses_usd": expense_usd,
              "net_usd": round(income_usd - expense_usd, 2)}
    logger.info("Xero P&L sync %s: %s", month, result)
    return result


async def backfill_xero_pnl(months: int = 12) -> dict:
    """Sync the trailing `months` complete months (excludes the current, in-progress
    month). Used by the manual backfill endpoint and the first population run."""
    today = date.today()
    y, m = today.year, today.month
    results, errors = [], []
    for _ in range(months):
        m -= 1
        if m == 0:
            m = 12
            y -= 1
        mk = f"{y:04d}-{m:02d}"
        try:
            results.append(await sync_xero_pnl(mk))
        except Exception as exc:  # one bad month must not abort the rest
            logger.error("Xero P&L backfill failed for %s: %s", mk, exc)
            errors.append({"month": mk, "error": str(exc)})
    return {"ok": not errors, "synced": results, "errors": errors}
