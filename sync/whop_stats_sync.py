"""Whop MRR/ARR sync — store a dated snapshot of recurring-revenue stats.

MRR/ARR are not in our DB (Whop computes them). Rather than live-call Whop on every
Company-page load, we snapshot MRR/ARR here on a periodic sync and store one row per
date; the dashboard charts the stored series.

MRR is computed from ACTIVE RECURRING memberships: each active subscription's renewal
price normalised to a monthly figure, summed. This is a best-effort computation from
the Whop memberships API (Whop has no public MRR endpoint) — verify the number against
the Whop dashboard after the first prod sync and adjust the field mapping / unit if it
diverges (prices may be dollars or cents depending on the plan).

Reuses the membership fetch + recurring signal from sync/whop_payments.py.
"""

import logging
from datetime import date, datetime, timezone

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import settings
from db.models import WhopStatsSnapshot
from db.session import AsyncSessionLocal
from sync.whop_payments import (
    WHOP_API_BASE,
    _fetch_whop_memberships,
    _whop_headers,
    membership_is_recurring,
)

logger = logging.getLogger(__name__)

_AVG_MONTH_SECONDS = 2_629_800  # seconds in an average month (365.25/12 days)
_ACTIVE_STATUSES = {"active", "trialing", "completed", "valid", "past_due"}


def _is_active(m: dict) -> bool:
    """Best-effort active check across the fields Whop uses for membership state."""
    if m.get("valid") is True:
        return True
    status = (m.get("status") or "").lower()
    return status in _ACTIVE_STATUSES


def _period_months(m: dict) -> float:
    """Months in the membership's renewal period, from the renewal window; default 1."""
    start, end = m.get("renewal_period_start"), m.get("renewal_period_end")
    if start and end and end > start:
        return max((end - start) / _AVG_MONTH_SECONDS, 0.5)
    return 1.0


async def _fetch_plan(client: httpx.AsyncClient, plan_id: str, cache: dict) -> dict:
    """Fetch a Whop plan by id (price lives on the plan, not the membership). Cached."""
    if plan_id in cache:
        return cache[plan_id]
    try:
        resp = await client.get(f"{WHOP_API_BASE}/plans/{plan_id}", headers=_whop_headers())
        plan = resp.json() if resp.status_code == 200 else {}
    except Exception:
        plan = {}
    cache[plan_id] = plan
    return plan


def _plan_price(plan: dict) -> float:
    """Renewal price (major units) from a Whop plan object, read defensively."""
    for key in ("renewal_price", "initial_price", "price", "final_price"):
        v = plan.get(key)
        if v is None:
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if f > 0:
            return f
    return 0.0


async def sync_whop_stats() -> dict:
    """Fetch memberships, compute MRR/ARR, and upsert today's snapshot.

    Returns {ok, mrr, arr, active_members, snapshot_date} — degrades to ok=False with
    a reason when the Whop key is unset (local) rather than raising.
    """
    if not settings.whop_api_key:
        logger.info("WHOP_API_KEY not set — skipping Whop stats sync")
        return {"ok": False, "reason": "no_whop_key", "mrr": 0.0, "arr": 0.0, "active_members": 0}

    plan_cache: dict = {}
    mrr = 0.0
    active = 0        # active recurring SUBSCRIPTION memberships (the MRR base)
    priced = 0        # of those, how many had a resolvable plan price

    async with httpx.AsyncClient(timeout=60.0) as client:
        memberships = await _fetch_whop_memberships(client)
        for m in memberships:
            if not _is_active(m) or not membership_is_recurring(m):
                continue
            # Exclude Splitit/ClarityPay financing: split_pay_required_payments marks a
            # FINITE installment plan for a high-ticket deal (e.g. $9,000 × N), not a
            # durable subscription. This is "true MRR" (subscriptions only). NOTE: Whop's
            # own dashboard is higher because it counts in-progress installment plans as
            # recurring until they complete — flip this exclusion to mirror that number.
            if m.get("split_pay_required_payments"):
                continue
            active += 1
            plan_id = m.get("plan")
            plan = await _fetch_plan(client, plan_id, plan_cache) if isinstance(plan_id, str) else {}
            price = _plan_price(plan)
            if price > 0:
                mrr += price / _period_months(m)
                priced += 1

    mrr = round(mrr, 2)
    arr = round(mrr * 12, 2)
    today = datetime.now(tz=timezone.utc).date()

    sample = {"membership_count": len(memberships), "active_recurring": active, "priced": priced}

    async with AsyncSessionLocal() as session:
        stmt = pg_insert(WhopStatsSnapshot).values(
            snapshot_date=today, mrr=mrr, arr=arr, active_members=active,
            currency="usd", source="whop_memberships",
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["snapshot_date"],
            set_={
                "mrr": stmt.excluded.mrr,
                "arr": stmt.excluded.arr,
                "active_members": stmt.excluded.active_members,
                "synced_at": datetime.now(tz=timezone.utc),
            },
        )
        await session.execute(stmt)
        await session.commit()

    logger.info("Whop stats sync: mrr=%.2f arr=%.2f active=%d (from %d memberships)",
                mrr, arr, active, len(memberships))
    return {"ok": True, "mrr": mrr, "arr": arr, "active_members": active,
            "snapshot_date": str(today), "sample": sample}
