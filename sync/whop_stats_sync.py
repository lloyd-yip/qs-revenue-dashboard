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
from sync.whop_payments import _fetch_whop_memberships, membership_is_recurring

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


def _renewal_price(m: dict) -> float:
    """Best-effort renewal price (major units) for a recurring membership.

    Reads the common Whop fields defensively. Values that look like cents (integers
    ≥ 1000 with no decimal) are left as-is here — unit correction, if needed, is a
    one-line change verified against the Whop dashboard on the first prod sync.
    """
    plan = m.get("plan") if isinstance(m.get("plan"), dict) else {}
    for src in (m, plan):
        for key in ("renewal_price", "price", "initial_price", "final_price"):
            v = src.get(key)
            if v is None:
                continue
            try:
                f = float(v)
            except (TypeError, ValueError):
                continue
            if f > 0:
                return f
    return 0.0


def _compute_mrr(memberships: list[dict]) -> tuple[float, int]:
    """(mrr, active_recurring_count) from active recurring memberships."""
    mrr = 0.0
    active = 0
    for m in memberships:
        if not _is_active(m) or not membership_is_recurring(m):
            continue
        price = _renewal_price(m)
        if price <= 0:
            continue
        active += 1
        mrr += price / _period_months(m)
    return round(mrr, 2), active


async def sync_whop_stats() -> dict:
    """Fetch memberships, compute MRR/ARR, and upsert today's snapshot.

    Returns {ok, mrr, arr, active_members, snapshot_date} — degrades to ok=False with
    a reason when the Whop key is unset (local) rather than raising.
    """
    if not settings.whop_api_key:
        logger.info("WHOP_API_KEY not set — skipping Whop stats sync")
        return {"ok": False, "reason": "no_whop_key", "mrr": 0.0, "arr": 0.0, "active_members": 0}

    async with httpx.AsyncClient(timeout=60.0) as client:
        memberships = await _fetch_whop_memberships(client)

    mrr, active = _compute_mrr(memberships)
    arr = round(mrr * 12, 2)
    today = datetime.now(tz=timezone.utc).date()

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
    return {"ok": True, "mrr": mrr, "arr": arr, "active_members": active, "snapshot_date": str(today)}
