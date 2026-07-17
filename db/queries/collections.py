"""Collections projection — how much cash lands each month across ALL deals with
a payment plan, not just the net-new deals that closed this month.

Whop does NOT store future installment dates (see whop_payments.py), so future
cash is a PROJECTION using the approved model: equal monthly installments of size
(total_paid / payment_count) from the first payment date across the plan length
(total_installments). Financed deals (Splitit/ClarityPay) settle 100% upfront, so
they have a single installment in their first-payment month and no future cash.

Everything here is derived from the aggregate columns already on deal_whop_matches
— no per-payment table required. Excluded (separate-offer) deals are skipped.
"""

import calendar
from datetime import date

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import DealWhopMatch


def _add_months(d: date, n: int) -> date:
    m = d.month - 1 + n
    y = d.year + m // 12
    m = m % 12 + 1
    return date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def _mk(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _months_between(start: date, end: date) -> list[str]:
    out, d = [], date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
    while d <= last:
        out.append(_mk(d))
        d = _add_months(d, 1)
    return out


def _deal_schedule(m: DealWhopMatch) -> list[dict]:
    """Projected installments for one deal → [{month, amount, paid, date}]."""
    total_paid = float(m.total_paid) if m.total_paid else 0.0
    fpd = m.first_payment_date
    if not fpd or total_paid <= 0:
        return []
    if m.is_splitit or m.is_claritypay:
        # External financing settles 100% upfront: one installment, no future cash.
        return [{"month": _mk(fpd), "amount": round(total_paid, 2), "paid": True, "date": fpd}]
    paid_count = m.payment_count or 0
    n = max(m.total_installments or paid_count or 1, paid_count, 1)
    size = total_paid / paid_count if paid_count else total_paid / n
    return [
        {"month": _mk(_add_months(fpd, k)), "amount": round(size, 2),
         "paid": k < paid_count, "date": _add_months(fpd, k)}
        for k in range(n)
    ]


async def get_collections_for_range(session: AsyncSession, start: date, end: date) -> dict:
    """Aggregate projected collections for the month window [start, end].

    Returns per-month collected/outstanding/total, window totals (incl. refunds),
    and the outstanding payment-plan breakdown (which accounts, how much left).
    """
    start_key, end_key = _mk(start), _mk(end)
    rows = (await session.execute(
        select(DealWhopMatch).where(DealWhopMatch.is_excluded.isnot(True))
    )).scalars().all()

    months: dict[str, dict] = {
        mk: {"month": mk, "collected": 0.0, "outstanding": 0.0,
             "new_deals": 0.0, "payment_plans": 0.0, "deal_ids": set()}
        for mk in _months_between(start, end)
    }
    plans: list[dict] = []
    refunded_total = 0.0
    window_deal_ids: set[str] = set()
    # Revenue source split: cash from NEW deals (first payment in the window) vs
    # ongoing PAYMENT PLANS (installments from deals that first paid before the window).
    src = {
        "new_deals": {"collected": 0.0, "outstanding": 0.0},
        "payment_plans": {"collected": 0.0, "outstanding": 0.0},
    }

    for r in rows:
        sched = _deal_schedule(r)
        if not sched:
            continue
        in_window = [s for s in sched if start_key <= s["month"] <= end_key]
        is_new_in_window = bool(r.first_payment_date and start_key <= _mk(r.first_payment_date) <= end_key)
        bucket_src = src["new_deals"] if is_new_in_window else src["payment_plans"]
        for s in in_window:
            b = months[s["month"]]
            b["outstanding" if not s["paid"] else "collected"] += s["amount"]
            b["new_deals" if is_new_in_window else "payment_plans"] += s["amount"]
            b["deal_ids"].add(r.ghl_opportunity_id)
            window_deal_ids.add(r.ghl_opportunity_id)
            bucket_src["outstanding" if not s["paid"] else "collected"] += s["amount"]

        refunded = float(r.total_refunded) if r.total_refunded else 0.0
        if in_window and refunded:
            refunded_total += refunded

        # Outstanding payment plan (internal multi-installment plan not fully paid).
        is_financed = bool(r.is_splitit or r.is_claritypay)
        total_n = max(r.total_installments or 0, r.payment_count or 0)
        if not is_financed and total_n > 1:
            all_out = sum(s["amount"] for s in sched if not s["paid"])
            if all_out > 0.5:
                nxt = next((s for s in sched if not s["paid"]), None)
                paid_k = r.payment_count or 0
                size = (float(r.total_paid) / paid_k) if paid_k else None
                plans.append({
                    "ghl_opportunity_id": r.ghl_opportunity_id,
                    "deal_name": r.ghl_opportunity_name,
                    "owner": r.ghl_owner_name or "Unassigned",
                    "account": r.whop_email or r.ghl_contact_email,
                    "installment_size": round(size, 2) if size else None,
                    "paid_count": paid_k,
                    "total_installments": total_n,
                    "collected": round(sum(s["amount"] for s in sched if s["paid"]), 2),
                    "outstanding": round(all_out, 2),
                    "projected_total": round(sum(s["amount"] for s in sched), 2),
                    "next_date": str(nxt["date"]) if nxt else None,
                    "refunded": round(refunded, 2) if refunded else None,
                })

    month_list = []
    for mk in _months_between(start, end):
        b = months[mk]
        collected, outstanding = round(b["collected"], 2), round(b["outstanding"], 2)
        month_list.append({
            "month": mk,
            "collected": collected,
            "outstanding": outstanding,
            "total": round(collected + outstanding, 2),
            "new_deals": round(b["new_deals"], 2),
            "payment_plans": round(b["payment_plans"], 2),
            "deal_count": len(b["deal_ids"]),
        })

    collected_sum = round(sum(m["collected"] for m in month_list), 2)
    outstanding_sum = round(sum(m["outstanding"] for m in month_list), 2)
    plans.sort(key=lambda p: p["outstanding"], reverse=True)

    def _src(k):
        c, o = round(src[k]["collected"], 2), round(src[k]["outstanding"], 2)
        return {"collected": c, "outstanding": o, "total": round(c + o, 2)}

    return {
        "range": {"start": start_key, "end": end_key},
        "months": month_list,
        "totals": {
            "collected": collected_sum,
            "outstanding": outstanding_sum,
            "total": round(collected_sum + outstanding_sum, 2),
            "refunded": round(refunded_total, 2),
            "net_collected": round(collected_sum - refunded_total, 2),
            "deal_count": len(window_deal_ids),
            "plan_count": len(plans),
            "new_deals": _src("new_deals"),
            "payment_plans_revenue": _src("payment_plans"),
        },
        "payment_plans": plans,
    }
