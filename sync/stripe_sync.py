"""Stripe cash-inflow sync — pulls EVERY succeeded charge into stripe_charges.

Distinct from the deal-matching Stripe pass in match_deals_whop.py, which floors at
$100 to skip the GHL SaaS subscription. Here there is NO floor: GHL sub-account
subscriptions and commissions are exactly the "other" cash the CEO wants counted.

The Company dashboard reads these per month as the live "Stripe" inflow channel.
Idempotent: upsert on stripe_charge_id, overwriting mutable fields so pending→refunded
transitions and amount changes land on re-sync.

Verification: after POST /api/dashboard/company/stripe-refresh, the returned stats show
{fetched, upserted}; then GET the Company overview and the Stripe channel should be > 0
for a month with card revenue.
"""

import logging
from datetime import datetime, timezone

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import settings
from db.models import StripeCharge
from db.session import AsyncSessionLocal

logger = logging.getLogger(__name__)

STRIPE_API_BASE = "https://api.stripe.com/v1"
# Match the whop-refresh lookback so multi-month windows stay populated without a
# full historical backfill on every run.
DEFAULT_LOOKBACK_DAYS = 190
_SUCCEEDED_STATUSES = {"succeeded"}


def _charge_to_row(ch: dict) -> dict | None:
    """Map a raw Stripe charge object → a stripe_charges row dict, or None if unusable.

    Amounts are cents in Stripe; store major units. Net cash is derived at read time
    as amount − refunded_amount, so both are persisted.
    """
    cid = ch.get("id")
    if not cid:
        return None
    if ch.get("status") not in _SUCCEEDED_STATUSES:
        return None
    ts = ch.get("created")
    created = datetime.fromtimestamp(ts, tz=timezone.utc).date() if ts else None
    bill = ch.get("billing_details") or {}
    cust = ch.get("customer")
    return {
        "stripe_charge_id": cid,
        "amount": round((ch.get("amount") or 0) / 100.0, 2),
        "refunded_amount": round((ch.get("amount_refunded") or 0) / 100.0, 2),
        "currency": (ch.get("currency") or "usd").lower(),
        "created": created,
        "status": ch.get("status"),
        "customer_id": cust if isinstance(cust, str) else (cust or {}).get("id") if isinstance(cust, dict) else None,
        "customer_email": ch.get("receipt_email") or bill.get("email"),
        "description": ch.get("description"),
        "payment_intent": ch.get("payment_intent") if isinstance(ch.get("payment_intent"), str) else None,
    }


async def _fetch_all_succeeded_charges(client: httpx.AsyncClient, since_ts: int) -> list[dict]:
    """All succeeded charges created after `since_ts` (Unix), via the Search API.

    NO amount floor — this is the whole point of the Stripe inflow channel. Paginates
    on next_page until exhausted.
    """
    charges: list[dict] = []
    query = f"status:'succeeded' AND created>{since_ts}"
    next_page = None
    while True:
        params: dict = {"query": query, "limit": 100}
        if next_page:
            params["page"] = next_page
        resp = await client.get(
            f"{STRIPE_API_BASE}/charges/search",
            params=params,
            auth=(settings.stripe_secret_key, ""),
        )
        resp.raise_for_status()
        data = resp.json()
        charges.extend(data.get("data", []))
        if data.get("has_more") and data.get("next_page"):
            next_page = data["next_page"]
        else:
            break
    return charges


async def sync_stripe_charges(lookback_days: int = DEFAULT_LOOKBACK_DAYS) -> dict:
    """Fetch succeeded Stripe charges in the lookback window and upsert them.

    Returns {ok, fetched, upserted, skipped, error?}. Degrades gracefully: if no
    Stripe key is configured, returns ok=False with a clear reason rather than raising.
    """
    if not settings.stripe_secret_key:
        logger.info("STRIPE_SECRET_KEY not set — skipping Stripe inflow sync")
        return {"ok": False, "reason": "no_stripe_key", "fetched": 0, "upserted": 0, "skipped": 0}

    now_ts = int(datetime.now(tz=timezone.utc).timestamp())
    since_ts = now_ts - lookback_days * 86400

    async with httpx.AsyncClient(timeout=30.0) as client:
        raw = await _fetch_all_succeeded_charges(client, since_ts)

    rows, skipped = [], 0
    for ch in raw:
        row = _charge_to_row(ch)
        if row is None:
            skipped += 1
        else:
            rows.append(row)

    upserted = 0
    if rows:
        async with AsyncSessionLocal() as session:
            for row in rows:
                stmt = pg_insert(StripeCharge).values(**row)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["stripe_charge_id"],
                    set_={
                        "amount": stmt.excluded.amount,
                        "refunded_amount": stmt.excluded.refunded_amount,
                        "currency": stmt.excluded.currency,
                        "created": stmt.excluded.created,
                        "status": stmt.excluded.status,
                        "customer_id": stmt.excluded.customer_id,
                        "customer_email": stmt.excluded.customer_email,
                        "description": stmt.excluded.description,
                        "payment_intent": stmt.excluded.payment_intent,
                        "synced_at": datetime.now(tz=timezone.utc),
                    },
                )
                await session.execute(stmt)
                upserted += 1
            await session.commit()

    logger.info("Stripe inflow sync: fetched=%d upserted=%d skipped=%d", len(raw), upserted, skipped)
    return {"ok": True, "fetched": len(raw), "upserted": upserted, "skipped": skipped}
