"""Revenue line items — Whop payment data for P&L dashboard.

Source of truth is this DB. Whop API is only called during the monthly seed run.
All dashboard reads come from here.
"""

from datetime import date

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import RevenueLineItem

CATEGORY_ORDER = ["cash_collected", "splitit_ar"]

CATEGORY_LABELS = {
    "cash_collected": "Cash Collected",
    "splitit_ar": "Splitit AR (Outstanding)",
}

PRODUCT_TYPE_LABELS = {
    "high_ticket": "High-Ticket",
    "saas": "SaaS",
}


async def get_available_revenue_periods(session: AsyncSession) -> list[dict]:
    """Return all periods that have revenue data, newest first."""
    rows = (await session.execute(
        select(RevenueLineItem.period_start, RevenueLineItem.period_end)
        .distinct()
        .order_by(RevenueLineItem.period_end.desc())
    )).all()
    return [{"period_start": str(r.period_start), "period_end": str(r.period_end)} for r in rows]


async def get_revenue_for_period(
    session: AsyncSession,
    period_start: date,
    period_end: date,
) -> dict:
    """Return revenue line items for a period, grouped by category with totals.

    Returns:
        {
          period_start, period_end,
          categories: [
            {
              category, label,
              items: [{product_type, label, amount, payment_count, notes}],
              total, payment_count
            }
          ],
          grand_total,           # total cash collected (excludes AR — AR is receivable, not income)
          total_cash_collected,
          total_splitit_ar,
        }
    """
    rows = (await session.execute(
        select(RevenueLineItem)
        .where(
            RevenueLineItem.period_start == period_start,
            RevenueLineItem.period_end == period_end,
        )
        .order_by(RevenueLineItem.category, RevenueLineItem.amount.desc())
    )).scalars().all()

    by_category: dict[str, list] = {c: [] for c in CATEGORY_ORDER}
    for row in rows:
        by_category[row.category].append({
            "product_type": row.product_type,
            "label": PRODUCT_TYPE_LABELS.get(row.product_type, row.product_type),
            "amount": float(row.amount),
            "payment_count": row.payment_count,
            "notes": row.notes,
        })

    result = []
    total_cash_collected = 0.0
    total_splitit_ar = 0.0

    for cat_key in CATEGORY_ORDER:
        items = by_category[cat_key]
        if not items:
            continue
        cat_total = sum(i["amount"] for i in items)
        cat_count = sum(i["payment_count"] for i in items)
        if cat_key == "cash_collected":
            total_cash_collected = cat_total
        elif cat_key == "splitit_ar":
            total_splitit_ar = cat_total
        result.append({
            "category": cat_key,
            "label": CATEGORY_LABELS[cat_key],
            "items": items,
            "total": cat_total,
            "payment_count": cat_count,
        })

    return {
        "period_start": str(period_start),
        "period_end": str(period_end),
        "categories": result,
        "grand_total": total_cash_collected,          # cash only — AR is future
        "total_cash_collected": total_cash_collected,
        "total_splitit_ar": total_splitit_ar,
    }


async def get_all_revenue_periods_summary(session: AsyncSession) -> list[dict]:
    """Return monthly totals for all periods — used by the P&L multi-month view.

    Returns list of dicts sorted by period_start desc:
        [{period_start, period_end, cash_collected, splitit_ar}]
    """
    rows = (await session.execute(
        select(
            RevenueLineItem.period_start,
            RevenueLineItem.period_end,
            RevenueLineItem.category,
            func.sum(RevenueLineItem.amount).label("total"),
        )
        .group_by(
            RevenueLineItem.period_start,
            RevenueLineItem.period_end,
            RevenueLineItem.category,
        )
        .order_by(RevenueLineItem.period_end.desc())
    )).all()

    # Pivot: group by period then by category
    periods: dict[tuple, dict] = {}
    for row in rows:
        key = (str(row.period_start), str(row.period_end))
        if key not in periods:
            periods[key] = {
                "period_start": str(row.period_start),
                "period_end": str(row.period_end),
                "cash_collected": 0.0,
                "splitit_ar": 0.0,
            }
        if row.category == "cash_collected":
            periods[key]["cash_collected"] = float(row.total)
        elif row.category == "splitit_ar":
            periods[key]["splitit_ar"] = float(row.total)

    return list(periods.values())


async def upsert_revenue_line_items(
    session: AsyncSession,
    period_start: date,
    period_end: date,
    items: list[dict],
    replace: bool = False,
) -> int:
    """Insert or overwrite revenue line items for a period.

    Each item must have: source, category, product_type, amount, payment_count.
    Optional: notes (str).
    If replace=True, all existing rows for the period are deleted first (clean refresh).
    Returns count of rows upserted.

    Plain English: "replace=True" means wipe the month clean before inserting — so
    running the seed script twice gives the same result as running it once (idempotent).
    """
    if replace:
        await session.execute(
            delete(RevenueLineItem).where(
                RevenueLineItem.period_start == period_start,
                RevenueLineItem.period_end == period_end,
            )
        )
    for item in items:
        stmt = pg_insert(RevenueLineItem).values(
            period_start=period_start,
            period_end=period_end,
            source=item["source"],
            category=item["category"],
            product_type=item["product_type"],
            amount=item["amount"],
            payment_count=item.get("payment_count", 0),
            notes=item.get("notes"),
        ).on_conflict_do_update(
            index_elements=["period_start", "period_end", "source", "category", "product_type"],
            set_={
                "amount": item["amount"],
                "payment_count": item.get("payment_count", 0),
                "notes": item.get("notes"),
                "updated_at": func.now(),
            },
        )
        await session.execute(stmt)
    await session.commit()
    return len(items)
