"""Xero P&L expense sync — pulls expense rows for a month, classifies vendors,
converts EUR→USD, and upserts into expense_line_items.

Server-side version of the old sync/xero_live_sync.py CLI flow (which required a
manually pasted token). Uses the stored Xero connection from Settings → Connectors.
Unknown vendors are skipped from the upsert and returned in the response so they
can be added to db/queries/vendor_classification.py.
"""

import calendar
import logging
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
from db.queries.vendor_classification import classify_vendor
from db.session import AsyncSessionLocal

logger = logging.getLogger(__name__)
router = APIRouter(tags=["xero"])

XERO_PNL_URL = "https://api.xero.com/api.xro/2.0/Reports/ProfitAndLoss"


class ExpenseSyncResult(BaseModel):
    month: str
    period_start: str
    period_end: str
    rows_upserted: int
    total_usd: float
    eur_usd_rate: float
    skipped_unclassified: list[dict]  # [{vendor, section, amount_usd}] — add to vendor_classification.py
    skipped_non_revenue: int


async def _fetch_pnl_expense_rows(
    access_token: str, tenant_id: str, period_start: date, period_end: date
) -> list[dict]:
    """Fetch the Xero P&L and return all non-Income line items with their section.

    Each item: {"section": str, "name": str, "amount_eur": float}. Skips summary
    and header rows and zero amounts (mirrors sync/xero_live_sync.py parsing).
    """
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            XERO_PNL_URL,
            headers={
                "Authorization":  f"Bearer {access_token}",
                "Xero-Tenant-Id": tenant_id,
                "Accept":         "application/json",
            },
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
    """Pull Xero P&L expense rows for a month and upsert into expense_line_items.

    replace=True per period — safe monthly refresh, same as the CLI flow.
    Vendors classified via VENDOR_BUCKET_MAP; non_revenue vendors are hidden by
    design; unclassified vendors are skipped and reported for review.
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

    items: list[dict] = []
    unclassified: list[dict] = []
    non_revenue_count = 0
    for row in pnl_rows:
        vendor  = row["name"]
        usd_amt = round(abs(row["amount_eur"]) * eur_usd, 2)  # Xero shows expenses positive
        bucket  = classify_vendor(vendor)

        if bucket == "non_revenue":
            non_revenue_count += 1
            continue  # delivery-team / bank costs — hidden from the dashboard by design
        if bucket == "unclassified":
            unclassified.append({"vendor": vendor, "section": row["section"], "amount_usd": usd_amt})
            continue  # skipped — add to vendor_classification.py, then re-sync

        items.append({
            "bucket": bucket,
            "vendor": vendor,
            "amount": usd_amt,
            "notes":  f"Xero P&L — {row['section']} — EUR {row['amount_eur']:,.2f} × {eur_usd} = USD {usd_amt:,.2f}",
        })

    async with AsyncSessionLocal() as session:
        rows_upserted = await upsert_expense_line_items(
            session, period_start, period_end, items, replace=True
        )

    total_usd = round(sum(i["amount"] for i in items), 2)
    logger.info(
        "xero_sync_expenses: month=%s rows=%d total_usd=%.2f unclassified=%d non_revenue=%d",
        month, rows_upserted, total_usd, len(unclassified), non_revenue_count,
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
    )
