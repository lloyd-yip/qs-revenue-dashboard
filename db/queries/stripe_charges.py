"""Read queries for stripe_charges — the live Stripe cash-inflow channel.

Sync lives in sync/stripe_sync.py (the Stripe API pull). These are read-only: the
Company dashboard sums net Stripe cash (gross − refunds) per month, converting any
non-USD charge to USD with the same ECB rate the P&L uses.
"""

from datetime import date

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.utils.xero_utils import get_eur_usd_rate
from db.models import StripeCharge


async def get_stripe_inflow_for_range(
    session: AsyncSession, start: date, end: date
) -> dict:
    """Sum net Stripe cash (gross − refunded) for the window [start, end], in USD.

    Includes ALL succeeded charges — GHL sub-account subscriptions and commissions
    as well as deal payments — since stripe_charges carries no amount floor. EUR (or
    other non-USD) charges are converted with get_eur_usd_rate, cached per month.

    Returns: {total_usd, count, by_month: [{month, amount, count}]}.
    """
    rows = (await session.execute(
        select(StripeCharge.amount, StripeCharge.refunded_amount,
               StripeCharge.currency, StripeCharge.created)
        .where(StripeCharge.created.isnot(None))
        .where(StripeCharge.created >= start)
        .where(StripeCharge.created <= end)
    )).all()

    rate_cache: dict[tuple[int, int], float] = {}

    def _to_usd(amount: float, currency: str | None, d: date) -> float:
        cur = (currency or "usd").strip().lower()
        if cur == "usd":
            return amount
        if cur == "eur":
            key = (d.year, d.month)
            if key not in rate_cache:
                rate_cache[key] = get_eur_usd_rate(d.year, d.month)
            return amount * rate_cache[key]
        return amount  # unknown currency: pass through rather than drop the cash

    by_month: dict[str, dict] = {}
    total = 0.0
    count = 0
    for amount, refunded, currency, d in rows:
        if d is None:
            continue
        net = float(amount or 0) - float(refunded or 0)
        usd = _to_usd(net, currency, d)
        mk = f"{d.year:04d}-{d.month:02d}"
        m = by_month.setdefault(mk, {"month": mk, "amount": 0.0, "count": 0})
        m["amount"] = round(m["amount"] + usd, 2)
        m["count"] += 1
        total += usd
        count += 1

    return {
        "total_usd": round(total, 2),
        "count": count,
        "by_month": sorted(by_month.values(), key=lambda x: x["month"]),
    }
