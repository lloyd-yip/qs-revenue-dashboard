"""Xero ACCREC invoice sync — pulls monthly invoices and stores contract value."""

import calendar
import logging
import time
from datetime import date

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from api.utils.xero_utils import XERO_TENANT_ID, get_eur_usd_rate, verify_bearer
from db.models import RevenueLineItem
from db.queries.revenue import upsert_revenue_line_items
from db.session import AsyncSessionLocal

logger = logging.getLogger(__name__)
router = APIRouter(tags=["xero"])

XERO_INVOICES_URL  = "https://api.xero.com/api.xro/2.0/Invoices"
_INVOICE_SOURCE    = "xero"
_INVOICE_CATEGORY  = "contract_value"
_INVOICE_PRODUCT   = "invoiced_total"
_BILLABLE_STATUSES = {"AUTHORISED", "PAID"}


class XeroApiError(Exception):
    """Raised when the Xero Invoices API returns a non-200 response."""


class InvoiceSyncResult(BaseModel):
    month: str
    period_start: str
    period_end: str
    invoice_count: int
    total_eur: float
    total_usd: float
    eur_usd_rate: float
    rows_upserted: int


async def _fetch_xero_invoices(
    access_token: str,
    period_start: date,
    period_end: date,
) -> list[dict]:
    """Fetch all ACCREC invoices from Xero whose DateString falls within the period.

    Paginates until an empty page is returned. Filters AUTHORISED and PAID statuses.
    Drops invoices whose DateString falls outside the period (defensive date guard).
    """
    results: list[dict] = []
    page = 1
    t0 = time.monotonic()

    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            resp = await client.get(
                XERO_INVOICES_URL,
                headers={
                    "Authorization":  f"Bearer {access_token}",
                    "Xero-Tenant-Id": XERO_TENANT_ID,
                    "Accept":         "application/json",
                },
                params={
                    "Type":      "ACCREC",
                    "fromDate":  str(period_start),
                    "toDate":    str(period_end),
                    "page":      page,
                },
            )
            if resp.status_code == 401:
                raise XeroApiError(f"Xero 401 — token expired or invalid: {resp.text[:200]}")
            if resp.status_code not in (200, 204):
                raise XeroApiError(
                    f"Xero Invoices API error {resp.status_code}: {resp.text[:300]}"
                )

            invoices = resp.json().get("Invoices", [])
            if not invoices:
                break

            for inv in invoices:
                if inv.get("Status") not in _BILLABLE_STATUSES:
                    continue
                # Defensive date guard — drop out-of-range invoices
                inv_date_str = inv.get("DateString", "")[:10]
                try:
                    inv_date = date.fromisoformat(inv_date_str)
                except ValueError:
                    logger.warning("Skipping invoice %s — unparseable DateString %r",
                                   inv.get("InvoiceID"), inv_date_str)
                    continue
                if not (period_start <= inv_date <= period_end):
                    logger.info("Skipping invoice %s — DateString %s outside %s..%s",
                                inv.get("InvoiceID"), inv_date_str, period_start, period_end)
                    continue
                results.append(inv)

            if len(invoices) < 100:
                break  # last page
            page += 1

    duration_ms = round((time.monotonic() - t0) * 1000)
    logger.info("_fetch_xero_invoices: month=%s..%s invoice_count=%d duration_ms=%d status=ok",
                period_start, period_end, len(results), duration_ms)
    return results


def _parse_invoice_totals(invoices: list[dict]) -> tuple[float, int]:
    """Sum Total amounts and count invoices across a list of raw Xero invoice dicts."""
    total_eur = 0.0
    count = 0
    for inv in invoices:
        if inv.get("Status") not in _BILLABLE_STATUSES:
            continue
        total_eur += float(inv.get("Total") or 0)
        count += 1
    return round(total_eur, 2), count


def _to_revenue_item(total_eur: float, eur_usd: float, invoice_count: int) -> dict:
    """Build a revenue_line_items-compatible dict from an invoice total and FX rate."""
    amount_usd = round(total_eur * eur_usd, 2)
    return {
        "source":        _INVOICE_SOURCE,
        "category":      _INVOICE_CATEGORY,
        "product_type":  _INVOICE_PRODUCT,
        "amount":        amount_usd,
        "payment_count": invoice_count,
        "notes":         f"Xero ACCREC invoices — EUR {total_eur:,.2f} × {eur_usd} = USD {amount_usd:,.2f}",
    }


@router.post(
    "/xero/sync-invoices",
    response_model=InvoiceSyncResult,
    dependencies=[Depends(verify_bearer)],
)
async def xero_sync_invoices(
    month: str = Query(..., pattern=r"^\d{4}-\d{2}$",
                       description="Month in YYYY-MM format, e.g. 2026-05"),
    xero_token: str = Query(..., description="Xero access token from API Explorer or OAuth flow"),
) -> InvoiceSyncResult:
    """Fetch Xero ACCREC invoices for a month, convert EUR→USD, upsert as contract_value.

    Performs a category-scoped DELETE before upsert so re-runs are idempotent
    without touching cash_collected or other categories for the same period.
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

    eur_usd = get_eur_usd_rate(year, mon)

    try:
        invoices = await _fetch_xero_invoices(xero_token, period_start, period_end)
    except XeroApiError as exc:
        raise HTTPException(status_code=502, detail=str(exc))

    total_eur, invoice_count = _parse_invoice_totals(invoices)
    item = _to_revenue_item(total_eur, eur_usd, invoice_count)

    async with AsyncSessionLocal() as session:
        # Category-scoped delete — leaves cash_collected and other categories intact
        await session.execute(
            delete(RevenueLineItem).where(
                RevenueLineItem.period_start == period_start,
                RevenueLineItem.period_end   == period_end,
                RevenueLineItem.category     == _INVOICE_CATEGORY,
            )
        )
        rows_upserted = await upsert_revenue_line_items(
            session, period_start, period_end, [item], replace=False
        )

    logger.info(
        "xero_sync_invoices: month=%s invoice_count=%d total_eur=%.2f eur_usd_rate=%.4f rows_upserted=%d",
        month, invoice_count, total_eur, eur_usd, rows_upserted,
    )

    return InvoiceSyncResult(
        month=month,
        period_start=str(period_start),
        period_end=str(period_end),
        invoice_count=invoice_count,
        total_eur=total_eur,
        total_usd=round(total_eur * eur_usd, 2),
        eur_usd_rate=eur_usd,
        rows_upserted=rows_upserted,
    )
