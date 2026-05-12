"""Read queries for xero_bank_transfers — Wise wire reconciliation.

Plain English: this file contains the database queries that fetch Wise
bank transfer data. The sync logic lives in xero_auth.py (the POST endpoint).
These are read-only — the dashboard UI and API call these to display transfer data.

Verification: after running POST /xero/sync-wise-transfers, call:
  GET /api/dashboard/deals/wise-transfers
  → should return {"transfers": [...], "count": N} where N > 0

Silent failure signal: if count is 0 and you know there are transfers in Xero,
the sync hasn't run or failed silently — check Railway logs for "Wise sync done".
"""

from datetime import date
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import XeroBankTransfer


async def get_wise_transfers_for_deal(
    session: AsyncSession,
    ghl_opportunity_id: str,
) -> list[dict]:
    """Return all Wise transfers linked to one GHL deal (most recent first).

    Used by the Deals page to show how much cash has actually arrived via wire
    for a specific deal — complements the Whop payment data.
    """
    rows = (await session.execute(
        select(XeroBankTransfer)
        .where(XeroBankTransfer.ghl_opportunity_id == ghl_opportunity_id)
        .order_by(XeroBankTransfer.date.desc().nullslast())
    )).scalars().all()

    return [_row_to_dict(r) for r in rows]


async def get_all_wise_transfers(
    session: AsyncSession,
    limit: int = 500,
    match_confidence: Optional[str] = None,
) -> list[dict]:
    """Return all Wise transfers, most recent first.

    Used by GET /api/dashboard/deals/wise-transfers (no deal filter).
    Capped at 500 rows to keep response times reasonable.
    """
    query = (
        select(XeroBankTransfer)
        .order_by(XeroBankTransfer.date.desc().nullslast())
        .limit(limit)
    )
    if match_confidence:
        query = query.where(XeroBankTransfer.match_confidence == match_confidence)

    rows = (await session.execute(query)).scalars().all()
    return [_row_to_dict(r) for r in rows]


def _row_to_dict(r: XeroBankTransfer) -> dict:
    """Serialize one XeroBankTransfer row to a JSON-safe dict."""
    return {
        "xero_transaction_id": r.xero_transaction_id,
        "account_name":        r.account_name,
        "date":                str(r.date) if r.date else None,
        "amount":              float(r.amount) if r.amount else None,
        "currency":            r.currency,
        "contact_name":        r.contact_name,
        "reference":           r.reference,
        "description":         r.description,
        "is_reconciled":       r.is_reconciled,
        "ghl_opportunity_id":  r.ghl_opportunity_id,
        "match_confidence":    r.match_confidence,
        "match_method":        r.match_method,
        "match_score":         float(r.match_score) if r.match_score else 0.0,
        "is_confirmed":        r.is_confirmed,
        "synced_at":           r.synced_at.isoformat() if r.synced_at else None,
    }
