"""Expense line items — classified P&L data loaded from monthly Xero pull.

Supabase is the source of truth. Xero API is only touched once per month
during a manual pull; all dashboard reads come from here.

Vendor classification is handled by vendor_classification.py — the upsert
function auto-assigns the correct bucket based on vendor name.
"""

from datetime import date

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ExpenseLineItem
from db.queries.vendor_classification import classify_vendor

# ── Bucket ordering and labels (drives P&L display) ─────────────────────────
# Only buckets listed here appear in the API response and grand_total.
# non_revenue is intentionally excluded — those items are stored but hidden.

BUCKET_ORDER = ["sales", "marketing_salaries", "tech_tools", "advertising", "paid_ads", "experiments"]

BUCKET_LABELS = {
    "sales": "Sales",
    "marketing_salaries": "Marketing Salaries",
    "tech_tools": "Tech & Tools",
    "advertising": "Digital Advertising",
    "paid_ads": "Paid Ads",
    "experiments": "Experiments",
}


# ── Queries ──────────────────────────────────────────────────────────────────

async def get_available_periods(session: AsyncSession) -> list[dict]:
    """Return all periods that have expense data, newest first."""
    rows = (await session.execute(
        select(ExpenseLineItem.period_start, ExpenseLineItem.period_end)
        .distinct()
        .order_by(ExpenseLineItem.period_end.desc())
    )).all()
    return [{"period_start": str(r.period_start), "period_end": str(r.period_end)} for r in rows]


async def get_expenses_for_period(
    session: AsyncSession,
    period_start: date,
    period_end: date,
) -> dict:
    """Return all line items for a period, grouped by bucket with totals.

    Only buckets in BUCKET_ORDER are included in the response.
    non_revenue items are stored but excluded here.
    """
    rows = (await session.execute(
        select(ExpenseLineItem)
        .where(
            ExpenseLineItem.period_start == period_start,
            ExpenseLineItem.period_end == period_end,
        )
        .order_by(ExpenseLineItem.bucket, ExpenseLineItem.amount.desc())
    )).scalars().all()

    buckets: dict[str, list] = {b: [] for b in BUCKET_ORDER}
    for row in rows:
        if row.bucket not in buckets:
            buckets[row.bucket] = []
        buckets[row.bucket].append({
            "vendor": row.vendor,
            "amount": float(row.amount),
            "is_approximate": row.is_approximate,
            "notes": row.notes,
        })

    result = []
    grand_total = 0.0
    for bucket_key in BUCKET_ORDER:
        items = buckets[bucket_key]
        if not items:
            continue
        bucket_total = sum(i["amount"] for i in items)
        has_approx = any(i["is_approximate"] for i in items)
        grand_total += bucket_total
        result.append({
            "bucket": bucket_key,
            "label": BUCKET_LABELS[bucket_key],
            "items": items,
            "total": bucket_total,
            "has_approximate": has_approx,
        })

    return {
        "period_start": str(period_start),
        "period_end": str(period_end),
        "buckets": result,
        "grand_total": grand_total,
    }


async def upsert_expense_line_items(
    session: AsyncSession,
    period_start: date,
    period_end: date,
    items: list[dict],
    replace: bool = False,
) -> int:
    """Insert or overwrite expense line items for a period.

    Each item must have: bucket, vendor, amount.
    Optional: is_approximate (bool), notes (str).
    If replace=True, all existing rows for the period are deleted first.

    The vendor's bucket is auto-classified via VENDOR_BUCKET_MAP. If the vendor
    is known, the mapped bucket overrides whatever the caller sent. Unknown
    vendors pass through with the caller's bucket unchanged.

    Returns count of rows upserted.
    """
    if replace:
        await session.execute(
            delete(ExpenseLineItem).where(
                ExpenseLineItem.period_start == period_start,
                ExpenseLineItem.period_end == period_end,
            )
        )
    for item in items:
        bucket = classify_vendor(item["vendor"], item["bucket"])
        stmt = pg_insert(ExpenseLineItem).values(
            period_start=period_start,
            period_end=period_end,
            bucket=bucket,
            vendor=item["vendor"],
            amount=item["amount"],
            is_approximate=item.get("is_approximate", False),
            notes=item.get("notes"),
        ).on_conflict_do_update(
            index_elements=["period_start", "period_end", "bucket", "vendor"],
            set_={
                "amount": item["amount"],
                "is_approximate": item.get("is_approximate", False),
                "notes": item.get("notes"),
                "updated_at": func.now(),
            },
        )
        await session.execute(stmt)
    await session.commit()
    return len(items)
