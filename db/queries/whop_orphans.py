"""Queries for whop_orphan_payments — Whop coaching payments with no GHL deal.

The matcher upserts qualifying unclaimed memberships here (preserving the human
review status); the New Deals view reads pending/confirmed ones for the month.
"""

from datetime import date, datetime, timezone

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import WhopOrphanPayment

ORPHAN_METRIC_FIELDS = (
    "whop_email", "whop_name", "whop_product_id", "first_payment_date",
    "total_paid", "net_cash_collected", "upfront_cash", "total_refunded",
    "payment_count", "total_installments", "is_splitit", "is_claritypay",
    "plan_months_flag", "provider_fee_pct",
)


async def upsert_orphan(session: AsyncSession, data: dict) -> None:
    """Insert or refresh an orphan membership's metrics. NEVER touches `status`
    (the human's confirm/ignore decision survives every re-match)."""
    values = {"whop_membership_id": data["whop_membership_id"], "last_seen_at": func.now()}
    values.update({k: data.get(k) for k in ORPHAN_METRIC_FIELDS})
    stmt = pg_insert(WhopOrphanPayment).values(**values).on_conflict_do_update(
        index_elements=["whop_membership_id"],
        set_={**{k: data.get(k) for k in ORPHAN_METRIC_FIELDS},
              "last_seen_at": func.now(), "updated_at": func.now()},
    )
    await session.execute(stmt)


async def delete_claimed_orphans(session: AsyncSession, claimed_membership_ids: set[str]) -> int:
    """Remove orphan rows whose membership is now claimed by a GHL deal (so a
    payment never double-counts once it gets matched)."""
    if not claimed_membership_ids:
        return 0
    res = await session.execute(
        delete(WhopOrphanPayment).where(
            WhopOrphanPayment.whop_membership_id.in_(claimed_membership_ids)
        )
    )
    return res.rowcount or 0


async def set_orphan_status(session: AsyncSession, membership_id: str, status: str) -> bool:
    res = await session.execute(
        update(WhopOrphanPayment)
        .where(WhopOrphanPayment.whop_membership_id == membership_id)
        .values(status=status, updated_at=datetime.now(timezone.utc))
    )
    await session.commit()
    return (res.rowcount or 0) > 0


async def get_orphans_for_range(session: AsyncSession, start: date, end: date) -> list[WhopOrphanPayment]:
    """Orphans whose first payment landed in [start, end], excluding ignored ones."""
    rows = (await session.execute(
        select(WhopOrphanPayment)
        .where(WhopOrphanPayment.first_payment_date >= start)
        .where(WhopOrphanPayment.first_payment_date <= end)
        .where(WhopOrphanPayment.status != "ignored")
        .order_by(WhopOrphanPayment.total_paid.desc().nullslast())
    )).scalars().all()
    return list(rows)
