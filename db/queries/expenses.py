"""Expense line items — classified P&L data loaded from monthly Xero pull.

Supabase is the source of truth. Xero API is only touched once per month
during a manual pull; all dashboard reads come from here.
"""

from datetime import date

from sqlalchemy import select, distinct
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import ExpenseLineItem

BUCKET_ORDER = ["sales", "marketing_salaries", "tech_tools", "paid_ads", "experiments"]

BUCKET_LABELS = {
    "sales": "Sales",
    "marketing_salaries": "Marketing Salaries",
    "tech_tools": "Tech & Tools",
    "paid_ads": "Paid Ads",
    "experiments": "Experiments",
}


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
    """Return all line items for a period, grouped by bucket with totals."""
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
) -> int:
    """Insert or overwrite expense line items for a period.

    Each item must have: bucket, vendor, amount.
    Optional: is_approximate (bool), notes (str).
    Returns count of rows upserted.
    """
    for item in items:
        stmt = pg_insert(ExpenseLineItem).values(
            period_start=period_start,
            period_end=period_end,
            bucket=item["bucket"],
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
                "updated_at": "now()",
            },
        )
        await session.execute(stmt)
    await session.commit()
    return len(items)
