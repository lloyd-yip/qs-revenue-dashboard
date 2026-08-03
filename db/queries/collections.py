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
from collections import defaultdict
from datetime import date

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import DealWhopMatch
from db.queries.common import payment_sources_for
from db.queries.rep_comp import DEFAULT_COMMISSION_PCT, get_rep_comp_settings_map
from db.queries.wise_transfers import get_matched_wise_opp_ids


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


def _clamp_paid(d: date, paid: bool, today: date) -> date:
    """A PAID installment cannot land in the future — cap its (estimated) date at
    today. Payments don't arrive on a perfectly monthly cadence, so the even-spacing
    estimate can push an already-paid installment past today; clamp it so
    'collected' never shows up in a period that hasn't happened yet."""
    return today if (paid and d > today) else d


def _net_mult(m: DealWhopMatch) -> float:
    """Fraction of gross cash QS keeps after the payment provider's fee.

    Financed deals (Splitit/ClarityPay) carry a ~15% provider fee; internal plans
    and pay-in-full carry none. Prefer the stored net/gross ratio so this matches
    the New Deals tab's net_cash_collected exactly, then fall back to
    provider_fee_pct, then a 0.85 default. Non-financed → 1.0 (no fee)."""
    if m.is_splitit or m.is_claritypay:
        if m.net_cash_collected is not None and m.total_paid and float(m.total_paid) > 0:
            return float(m.net_cash_collected) / float(m.total_paid)
        if m.provider_fee_pct is not None:
            return 1.0 - float(m.provider_fee_pct)
        return 0.85
    return 1.0


def _deal_schedule(m: DealWhopMatch, today: date) -> list[dict]:
    """Projected installments for one deal → [{month, amount, gross, paid, date, is_first}].

    `amount` is NET of the payment-provider fee (what QS actually keeps), so the
    Collections cash figures reconcile with the New Deals tab. `gross` is the pre-fee
    installment (== amount for internal/pay-in-full plans, which carry no fee) —
    it's the commission basis, matching how the Sales Reps tab commissions on
    total_paid. Only financed deals differ (gross = total_paid, amount = net)."""
    total_paid = float(m.total_paid) if m.total_paid else 0.0
    fpd = m.first_payment_date
    if not fpd or total_paid <= 0:
        return []
    net_total = total_paid * _net_mult(m)
    if m.is_splitit or m.is_claritypay:
        # External financing settles 100% upfront: one installment, no future cash.
        d = _clamp_paid(fpd, True, today)
        return [{"month": _mk(d), "amount": round(net_total, 2), "gross": round(total_paid, 2),
                 "paid": True, "date": d, "is_first": True}]
    paid_count = m.payment_count or 0
    n = max(m.total_installments or paid_count or 1, paid_count, 1)
    size = net_total / paid_count if paid_count else net_total / n
    gross_size = total_paid / paid_count if paid_count else total_paid / n
    out = []
    for k in range(n):
        paid = k < paid_count
        d = _clamp_paid(_add_months(fpd, k), paid, today)
        out.append({"month": _mk(d), "amount": round(size, 2), "gross": round(gross_size, 2),
                    "paid": paid, "date": d, "is_first": k == 0})
    return out


async def get_collections_for_range(
    session: AsyncSession, start: date, end: date, rep: str | None = None
) -> dict:
    """Aggregate projected collections for the window [start, end].

    Bounds are dates, so custom day-level windows work: installments are filtered
    by their (estimated, day-clamped) date. For month-aligned bounds this is
    identical to the original whole-month behaviour.

    rep: when set, restrict to deals owned by this rep (matched on the deal's
    denormalized owner name; "Unassigned" selects owner-less deals).

    Returns per-month collected/outstanding/refunded/total, window totals (incl.
    refunds), the outstanding payment-plan breakdown, and a per-rep commission
    worksheet (gross cash collected − refunds this period × the rep's commission %).
    """
    start_key, end_key = _mk(start), _mk(end)
    today = date.today()  # paid installments never project past today
    q = select(DealWhopMatch).where(DealWhopMatch.is_excluded.isnot(True))
    if rep:
        # Match on the owner name, tolerant of whitespace/case differences between
        # the /reps dropdown value and the deal's denormalized owner name (some GHL
        # names carry doubled spaces). "Unassigned" selects owner-less deals.
        owner_col = func.coalesce(DealWhopMatch.ghl_owner_name, "Unassigned")
        owner_norm = func.lower(func.trim(func.regexp_replace(owner_col, r"\s+", " ", "g")))
        q = q.where(owner_norm == " ".join(rep.split()).lower())
    rows = (await session.execute(q)).scalars().all()
    wise_opp_ids = await get_matched_wise_opp_ids(session)  # deals with a matched Wise wire

    months: dict[str, dict] = {
        mk: {"month": mk, "collected": 0.0, "outstanding": 0.0, "refunded": 0.0,
             "new_deals": 0.0, "payment_plans": 0.0, "deal_ids": set()}
        for mk in _months_between(start, end)
    }
    plans: list[dict] = []
    refunded_total = 0.0
    window_deal_ids: set[str] = set()
    # Per-rep commission worksheet: gross cash collected this period, refunds this
    # period (by refund month), → commission on the net of the two.
    rep_agg: dict[str, dict] = defaultdict(lambda: {"collected": 0.0, "refunded": 0.0})
    # Per-rep, per-deal audit trail behind the worksheet numbers — one entry per deal
    # that contributed cash or a refund this window, so the UI can show exactly which
    # deals a rep's Cash Collected / Refunded / Commission were computed from.
    rep_deals: dict[str, dict[str, dict]] = defaultdict(dict)
    # Revenue source split, per installment: a deal's FIRST payment = New Deals
    # (net-new that month); its 2nd/3rd/… installments = Payment Plans (ongoing).
    src = {
        "new_deals": {"collected": 0.0, "outstanding": 0.0},
        "payment_plans": {"collected": 0.0, "outstanding": 0.0},
    }

    for r in rows:
        owner = r.ghl_owner_name or "Unassigned"
        sched = _deal_schedule(r, today)
        if not sched:
            continue
        in_window = [s for s in sched if start <= s["date"] <= end]
        deal_gross = 0.0  # gross cash collected by this deal in-window (commission basis)
        for s in in_window:
            b = months[s["month"]]
            paidkey = "collected" if s["paid"] else "outstanding"
            cohort = "new_deals" if s["is_first"] else "payment_plans"
            b[paidkey] += s["amount"]
            b[cohort] += s["amount"]
            b["deal_ids"].add(r.ghl_opportunity_id)
            window_deal_ids.add(r.ghl_opportunity_id)
            src[cohort][paidkey] += s["amount"]
            if s["paid"]:  # commission accrues on gross cash actually collected
                rep_agg[owner]["collected"] += s["gross"]
                deal_gross += s["gross"]
        installments_in_window = sum(1 for s in in_window if s["paid"])

        # Refunds are attributed to the month the refund was INITIATED
        # (last_refund_date), falling back to the deal's first-payment month when the
        # refund date hasn't been captured yet. This decouples a refund from the
        # deal's installment schedule so a later refund lands in its own month.
        refunded = float(r.total_refunded) if r.total_refunded else 0.0
        refund_day = r.last_refund_date or r.first_payment_date
        deal_refund = 0.0  # this deal's refund counted in-window (by refund date)
        if refunded and refund_day and start <= refund_day <= end:
            months[_mk(refund_day)]["refunded"] += refunded
            refunded_total += refunded
            rep_agg[owner]["refunded"] += refunded
            deal_refund = refunded

        # Deal-level audit entry for the commission worksheet drill-down: keep a deal
        # only if it moved cash or refunded this window (net commission basis = gross − refund).
        if deal_gross or deal_refund:
            total_n_deal = max(r.total_installments or 0, r.payment_count or 0, 1)
            rep_deals[owner][r.ghl_opportunity_id] = {
                "ghl_opportunity_id": r.ghl_opportunity_id,
                "deal_name": r.ghl_opportunity_name,
                "email": r.whop_email or r.ghl_contact_email,
                "close_date": str(r.ghl_close_date) if r.ghl_close_date else None,
                "first_payment_date": str(r.first_payment_date) if r.first_payment_date else None,
                "refund_date": str(r.last_refund_date) if r.last_refund_date else None,
                "cash_collected": round(deal_gross, 2),
                "refunded": round(deal_refund, 2),
                "net_collected": round(deal_gross - deal_refund, 2),
                "installments_in_window": installments_in_window,
                "paid_count": r.payment_count or 0,
                "total_installments": total_n_deal,
                "is_financed": bool(r.is_splitit or r.is_claritypay),
                "payment_source": payment_sources_for(r, wise_opp_ids),
            }

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
                    "payment_source": payment_sources_for(r, wise_opp_ids),
                })

    month_list = []
    for mk in _months_between(start, end):
        b = months[mk]
        collected, outstanding = round(b["collected"], 2), round(b["outstanding"], 2)
        refunded = round(b["refunded"], 2)
        month_list.append({
            "month": mk,
            "collected": collected,
            "outstanding": outstanding,
            "refunded": refunded,
            "net_collected": round(collected - refunded, 2),
            "total": round(collected + outstanding, 2),
            "new_deals": round(b["new_deals"], 2),
            "payment_plans": round(b["payment_plans"], 2),
            "deal_count": len(b["deal_ids"]),
        })

    collected_sum = round(sum(m["collected"] for m in month_list), 2)
    outstanding_sum = round(sum(m["outstanding"] for m in month_list), 2)
    plans.sort(key=lambda p: p["outstanding"], reverse=True)

    # Per-rep commission worksheet: commission = commission% × (gross cash collected
    # − refunds this period). Refunds reduce commission in the month they're
    # initiated (a clawback), consistent with the refund-by-refund-date model above.
    # Commission % comes from rep_comp_settings (matched by name); reps with no
    # stored row use the default and are flagged so the UI can label them.
    comp_map = await get_rep_comp_settings_map(session)
    comp_by_name = {v["rep_name"]: v["commission_pct"] for v in comp_map.values()}
    reps_list = []
    for owner, agg in rep_agg.items():
        collected = round(agg["collected"], 2)
        refunded = round(agg["refunded"], 2)
        net = round(collected - refunded, 2)
        pct = comp_by_name.get(owner, DEFAULT_COMMISSION_PCT)
        # Deal-level breakdown, richest-collecting first, each with its own commission
        # contribution (rate × the deal's net) so the modal reconciles to the rep total.
        deals = sorted(rep_deals.get(owner, {}).values(),
                       key=lambda d: d["cash_collected"], reverse=True)
        for d in deals:
            d["commission"] = round(d["net_collected"] * pct / 100.0, 2)
        reps_list.append({
            "rep": owner,
            "cash_collected": collected,
            "refunded": refunded,
            "net_collected": net,
            "commission_pct": pct,
            "commission": round(net * pct / 100.0, 2),
            "is_default_pct": owner not in comp_by_name,
            "deals": deals,
        })
    reps_list = [r for r in reps_list if r["cash_collected"] or r["refunded"]]
    reps_list.sort(key=lambda r: r["cash_collected"], reverse=True)
    commission_total = round(sum(r["commission"] for r in reps_list), 2)

    def _src(k):
        c, o = round(src[k]["collected"], 2), round(src[k]["outstanding"], 2)
        return {"collected": c, "outstanding": o, "total": round(c + o, 2)}

    return {
        "range": {"start": start_key, "end": end_key},
        "rep": rep,
        "months": month_list,
        "totals": {
            "collected": collected_sum,
            "outstanding": outstanding_sum,
            "total": round(collected_sum + outstanding_sum, 2),
            "refunded": round(refunded_total, 2),
            "net_collected": round(collected_sum - refunded_total, 2),
            "commission": commission_total,
            "deal_count": len(window_deal_ids),
            "plan_count": len(plans),
            "new_deals": _src("new_deals"),
            "payment_plans_revenue": _src("payment_plans"),
        },
        "reps": reps_list,
        "payment_plans": plans,
    }
