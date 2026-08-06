"""Whop product → category mapping endpoints (Deals › Products tab).

Browser-facing and unauthenticated, matching the dashboard/whop_live router
convention (these serve the static Deals page directly). The mapping decides whether
a product's payments count as deal revenue; POST /refresh pulls the live Whop catalogue.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from config import WHOP_PRODUCT_CATEGORIES, settings
from db.queries.product_mappings import (
    get_all_product_mappings,
    set_product_category,
    upsert_product,
)
from db.session import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/dashboard", tags=["products"])


@router.get("/products")
async def list_products(db: AsyncSession = Depends(get_db)) -> dict:
    """All Whop products with their (editable) category, for the Products tab.
    If empty, call POST /products/refresh to pull the catalogue from Whop."""
    products = await get_all_product_mappings(db)
    return {"products": products, "categories": list(WHOP_PRODUCT_CATEGORIES)}


class ProductCategoryInput(BaseModel):
    category: str


@router.put("/products/{product_id}")
async def update_product_category(
    product_id: str, body: ProductCategoryInput, db: AsyncSession = Depends(get_db)
) -> dict:
    """Manually pin a product's category (normal_deal | upsell | hermes | ignore).
    Takes effect on the next matcher run (the mapping drives membership exclusion)."""
    if body.category not in WHOP_PRODUCT_CATEGORIES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid category '{body.category}'. One of: {list(WHOP_PRODUCT_CATEGORIES)}",
        )
    ok = await set_product_category(db, product_id, body.category)
    if not ok:
        raise HTTPException(status_code=404, detail=f"Product {product_id} not found")
    return {"ok": True, "product_id": product_id, "category": body.category}


@router.post("/products/refresh")
async def refresh_products(db: AsyncSession = Depends(get_db)) -> dict:
    """Pull the live Whop product catalogue and upsert each product (auto-categorising
    new ones; never overwriting a manual category). Lets the Products tab populate
    without waiting for a full matcher run."""
    if not settings.whop_api_key:
        raise HTTPException(status_code=400, detail="WHOP_API_KEY not set")
    import httpx

    from sync.whop_payments import (
        _fetch_whop_products,
        product_active_users,
        product_name,
        product_price_display,
        product_revenue,
    )

    async with httpx.AsyncClient(timeout=30.0) as client:
        products = await _fetch_whop_products(client)

    count = 0
    for p in products:
        pid = p.get("id")
        if not pid:
            continue
        await upsert_product(
            db,
            pid,
            product_name=product_name(p),
            price_display=product_price_display(p),
            all_time_revenue=product_revenue(p),
            active_users=product_active_users(p),
        )
        count += 1
    await db.commit()
    return {"ok": True, "products_upserted": count}
