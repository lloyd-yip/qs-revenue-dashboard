"""Whop product → category mapping queries.

Every Whop product is stored once with an editable category that decides whether its
payments count as deal/coaching revenue (see WhopProductMapping / the Products tab).
The matcher upserts products (preserving any human override); the dashboard reads the
list and sets categories. get_excluded_product_ids drives membership exclusion.
"""

from sqlalchemy import case, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from config import WHOP_EXCLUDED_CATEGORIES, auto_product_category
from db.models import WhopProductMapping


async def upsert_product(
    session: AsyncSession,
    product_id: str,
    *,
    product_name: str | None,
    price_display: str | None = None,
    all_time_revenue: float | None = None,
    active_users: int | None = None,
) -> None:
    """Insert or refresh a product's display fields + auto_category. NEVER overwrites
    `category` once a human has set it (is_manual=True); for auto rows it keeps
    category in sync with the (possibly updated) auto_category."""
    auto = auto_product_category(product_name or "")
    stmt = pg_insert(WhopProductMapping).values(
        product_id=product_id,
        product_name=product_name,
        price_display=price_display,
        all_time_revenue=all_time_revenue,
        active_users=active_users,
        category=auto,
        auto_category=auto,
        is_manual=False,
    )
    # On conflict: refresh display + auto_category always; only move `category` to the
    # new auto value for rows the user has NOT manually pinned.
    stmt = stmt.on_conflict_do_update(
        index_elements=[WhopProductMapping.product_id],
        set_={
            "product_name": stmt.excluded.product_name,
            "price_display": stmt.excluded.price_display,
            "all_time_revenue": stmt.excluded.all_time_revenue,
            "active_users": stmt.excluded.active_users,
            "auto_category": stmt.excluded.auto_category,
            # Keep a human-pinned category; otherwise track the (possibly new) auto value.
            "category": case(
                (WhopProductMapping.is_manual.is_(True), WhopProductMapping.category),
                else_=stmt.excluded.auto_category,
            ),
            "updated_at": func.now(),
        },
    )
    await session.execute(stmt)


async def get_all_product_mappings(session: AsyncSession) -> list[dict]:
    """All products, richest revenue first, for the Products tab."""
    rows = (await session.execute(
        select(WhopProductMapping).order_by(
            WhopProductMapping.all_time_revenue.desc().nullslast(),
            WhopProductMapping.product_name,
        )
    )).scalars().all()
    return [
        {
            "product_id": r.product_id,
            "product_name": r.product_name,
            "price_display": r.price_display,
            "all_time_revenue": float(r.all_time_revenue) if r.all_time_revenue is not None else None,
            "active_users": r.active_users,
            "category": r.category,
            "auto_category": r.auto_category,
            "is_manual": r.is_manual,
        }
        for r in rows
    ]


async def set_product_category(session: AsyncSession, product_id: str, category: str) -> bool:
    """Manually pin a product's category (is_manual=True). Returns False if unknown."""
    row = (await session.execute(
        select(WhopProductMapping).where(WhopProductMapping.product_id == product_id)
    )).scalar_one_or_none()
    if not row:
        return False
    row.category = category
    row.is_manual = True
    await session.commit()
    return True


async def get_excluded_product_ids(session: AsyncSession) -> set[str]:
    """Product IDs whose category excludes them from deal metrics (hermes / ignore)."""
    rows = (await session.execute(
        select(WhopProductMapping.product_id).where(
            WhopProductMapping.category.in_(WHOP_EXCLUDED_CATEGORIES)
        )
    )).scalars().all()
    return set(rows)


async def get_category_by_product(session: AsyncSession) -> dict[str, str]:
    """product_id → category, for tagging deals by their product's bucket."""
    rows = (await session.execute(
        select(WhopProductMapping.product_id, WhopProductMapping.category)
    )).all()
    return {pid: cat for pid, cat in rows}
