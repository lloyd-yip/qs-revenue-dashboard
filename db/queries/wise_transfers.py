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

from api.utils.xero_utils import get_eur_usd_rate
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


def bank_source_label(account_name: str | None) -> str:
    """Map an xero_bank_transfers account_name to a payment-source badge label.

    account_name is e.g. "Wise USD", "Payoneer EUR" — collapse the currency suffix to a
    single bank label. Unknown names fall back to the raw name (or "Bank").
    """
    n = (account_name or "").strip().lower()
    if n.startswith("wise"):
        return "Wise"
    if n.startswith("payoneer"):
        return "Payoneer"
    if n.startswith("stripe"):
        return "Stripe"
    if n.startswith("whop"):
        return "Whop"
    return (account_name or "Bank").strip()


async def get_matched_bank_sources_by_opp(session: AsyncSession) -> dict[str, set[str]]:
    """Map each deal (ghl_opportunity_id) → set of bank-transfer source labels it has.

    e.g. {"opp123": {"Wise"}, "opp456": {"Payoneer", "Wise"}}. Derived from matched
    xero_bank_transfers rows (any bank Xero/Wise syncs into that table); excludes unmatched.
    Used to badge deals with their bank payment rail(s).
    """
    rows = (await session.execute(
        select(XeroBankTransfer.ghl_opportunity_id, XeroBankTransfer.account_name)
        .where(XeroBankTransfer.ghl_opportunity_id.isnot(None))
        .where(XeroBankTransfer.match_confidence != "unmatched")
    )).all()
    out: dict[str, set[str]] = {}
    for opp_id, account_name in rows:
        if opp_id:
            out.setdefault(opp_id, set()).add(bank_source_label(account_name))
    return out


async def get_bank_inflow_for_range(
    session: AsyncSession, start: date, end: date
) -> dict:
    """Sum incoming bank-transfer cash (Wise + Payoneer, via the Xero bank feed) for
    the window [start, end], converted to USD and broken down by bank + month.

    These are the RECEIVE transactions synced into xero_bank_transfers — a payment
    rail SEPARATE from Whop (direct wires / Payoneer), so they represent cash that
    Whop live revenue does not already capture. EUR rows are converted with the same
    ECB monthly rate the P&L uses (get_eur_usd_rate), cached per month here so a
    busy window doesn't refetch the rate for every row.

    Returns:
        {
          total_usd,
          count,
          by_source: {"Wise": x, "Payoneer": y, ...},   # USD
          by_month:  [{"month": "YYYY-MM", "amount": z, "count": n}],  # USD, ascending
        }

    NOTE (Stage 1 caveat): includes ALL incoming transfers, matched to a deal or not.
    A wire that also settles a deal tracked elsewhere can overlap with collections —
    the CEO page surfaces bank inflow as its own channel line rather than blindly
    folding it into a single grand total. Direct-Payoneer and Stripe rails arrive in
    later stages.
    """
    rows = (await session.execute(
        select(XeroBankTransfer.amount, XeroBankTransfer.currency,
               XeroBankTransfer.account_name, XeroBankTransfer.date)
        .where(XeroBankTransfer.date.isnot(None))
        .where(XeroBankTransfer.date >= start)
        .where(XeroBankTransfer.date <= end)
    )).all()

    rate_cache: dict[tuple[int, int], float] = {}

    def _to_usd(amount: float, currency: str | None, d: date) -> float:
        cur = (currency or "USD").strip().upper()
        if cur == "USD":
            return amount
        if cur == "EUR":
            key = (d.year, d.month)
            if key not in rate_cache:
                rate_cache[key] = get_eur_usd_rate(d.year, d.month)
            return amount * rate_cache[key]
        # Unknown currency: pass through unconverted rather than drop the cash.
        return amount

    by_source: dict[str, float] = {}
    by_month: dict[str, dict] = {}
    total = 0.0
    count = 0
    for amount, currency, account_name, d in rows:
        if amount is None or d is None:
            continue
        usd = _to_usd(float(amount), currency, d)
        label = bank_source_label(account_name)
        mk = f"{d.year:04d}-{d.month:02d}"
        by_source[label] = round(by_source.get(label, 0.0) + usd, 2)
        m = by_month.setdefault(mk, {"month": mk, "amount": 0.0, "count": 0})
        m["amount"] = round(m["amount"] + usd, 2)
        m["count"] += 1
        total += usd
        count += 1

    return {
        "total_usd": round(total, 2),
        "count": count,
        "by_source": by_source,
        "by_month": sorted(by_month.values(), key=lambda x: x["month"]),
    }


def _is_direct_wire(contact_name: str | None) -> bool:
    """True if a bank receipt is a DIRECT client wire — i.e. NOT a Whop/Stripe payout
    (the same cash already counted at the processor) and NOT an internal account move.

    This is the key to a non-double-counted channel mix: Whop and Stripe pay their
    collected cash out to the bank, so those bank rows must be excluded and the cash
    counted once at the Whop/Stripe gross layer instead.
    """
    n = (contact_name or "").strip().lower()
    if not n:
        return True  # unnamed receipt → treat as a direct wire (best effort)
    return not any(k in n for k in ("whop", "stripe", "wise", "payoneer"))


async def get_direct_wire_by_month(session: AsyncSession, start: date, end: date) -> dict:
    """Direct client wires (excluding Whop/Stripe payouts + internal moves) by month,
    split by landing bank (Wise / Payoneer), USD.

    Feeds the de-duplicated cash-in channel mix on the Company dashboard. Returns:
        {by_bank: {"Wise (direct)": {month: usd}, "Payoneer (direct)": {month: usd}},
         totals: {bank_label: usd}}
    """
    rows = (await session.execute(
        select(XeroBankTransfer.amount, XeroBankTransfer.currency,
               XeroBankTransfer.account_name, XeroBankTransfer.contact_name, XeroBankTransfer.date)
        .where(XeroBankTransfer.date.isnot(None))
        .where(XeroBankTransfer.date >= start)
        .where(XeroBankTransfer.date <= end)
    )).all()

    rate_cache: dict[tuple[int, int], float] = {}

    def _usd(amount: float, currency: str | None, d: date) -> float:
        cur = (currency or "USD").strip().upper()
        if cur == "EUR":
            key = (d.year, d.month)
            if key not in rate_cache:
                rate_cache[key] = get_eur_usd_rate(d.year, d.month)
            return amount * rate_cache[key]
        return amount

    by_bank: dict[str, dict[str, float]] = {}
    totals: dict[str, float] = {}
    for amount, currency, account_name, contact_name, d in rows:
        if amount is None or d is None or not _is_direct_wire(contact_name):
            continue
        label = bank_source_label(account_name)  # "Wise" | "Payoneer" | …
        label = f"{label} (direct)"
        usd = _usd(float(amount), currency, d)
        mk = f"{d.year:04d}-{d.month:02d}"
        by_bank.setdefault(label, {})[mk] = round(by_bank.setdefault(label, {}).get(mk, 0.0) + usd, 2)
        totals[label] = round(totals.get(label, 0.0) + usd, 2)

    return {"by_bank": by_bank, "totals": totals}


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
