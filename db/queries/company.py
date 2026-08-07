"""Company-wide CEO overview — one composed payload for the Company dashboard.

This is a THIN assembler: it reuses the existing, battle-tested query functions
rather than re-deriving any metric, per the project's DRY rule. Specifically it
composes:
  • Whop live net cash for the month      → db.queries.whop_live.get_whop_live_summary_for_month
  • Projected collections + payment plans  → db.queries.collections.get_collections_for_range
  • Bank-wire inflow (Wise/Payoneer)       → db.queries.wise_transfers.get_bank_inflow_for_range
  • Reconciled Xero P&L (last synced month)→ db.queries.revenue / db.queries.expenses

Freshness is deliberately explicit: operational rails (Whop, bank wires) are LIVE;
the Xero P&L block is RECONCILED (last synced month) and clearly labelled — the
project's standing decision is never to present unreconciled Xero as live.

Stage 1 covers everything already in the DB. Later stages add live Stripe (incl.
GHL sub-account subscriptions + commissions) and a direct Payoneer connection,
each landing as a new inflow channel here.
"""

from datetime import date

from sqlalchemy.ext.asyncio import AsyncSession

import calendar
import logging

from db.queries.collections import get_collections_for_range
from db.queries.expenses import (
    BUCKET_LABELS,
    BUCKET_ORDER,
    get_available_periods,
    get_expenses_by_month,
    get_expenses_for_period,
)
from db.queries.revenue import (
    get_all_revenue_periods_summary,
    get_available_revenue_periods,
    get_revenue_for_period,
    get_revenue_streams_by_month,
)
from db.queries.stripe_charges import get_stripe_inflow_for_range
from db.queries.whop_live import get_whop_inflow_by_month, get_whop_live_summary_for_month
from db.queries.whop_stats import get_latest_whop_stats, get_whop_stats_by_month
from db.queries.wise_transfers import get_bank_inflow_for_range, get_direct_wire_by_month

logger = logging.getLogger(__name__)

# Inflow channels not yet wired live — shown to the CEO as "coming" so the page is
# honest about what the live number does and does not include today.
ROADMAP_CHANNELS = [
    "Direct Payoneer connection (beyond the Xero bank feed)",
    "Affiliate / referral revenue",
]

_INFLOW_CAVEAT = (
    "Live figure = Whop net cash + Stripe (all succeeded charges, incl. GHL "
    "sub-account subscriptions & commissions) + bank wires received (Wise/Payoneer "
    "via the Xero bank feed). A Stripe charge or wire that also settles a deal "
    "tracked in Collections can overlap; treat the combined total as directional "
    "until per-rail dedupe lands. Direct-Payoneer and affiliate rails come next."
)


def _parse_next(nd: str | None) -> date | None:
    """Parse a payment plan's next_date ('YYYY-MM-DD') into a date, tolerating None."""
    if not nd:
        return None
    try:
        return date.fromisoformat(nd)
    except ValueError:
        return None


def _build_who_owes(collections: dict, start: date, end: date) -> dict:
    """From the collections payload, surface the payment-plan installments OWED on or
    before the window end — i.e. who still needs to pay. Each plan's next_date is the
    next UNPAID installment (paid ones are already clamped out upstream), so a plan
    appears here iff its owed installment lands within the window (due) or before it
    (overdue).
    """
    due, overdue = [], []
    due_total = overdue_total = 0.0
    for p in collections.get("payment_plans", []):
        nd = _parse_next(p.get("next_date"))
        if nd is None or nd > end:
            continue  # nothing owed yet within the window
        owed = p.get("outstanding") or 0.0
        row = {
            "deal_name": p.get("deal_name"),
            "owner": p.get("owner"),
            "account": p.get("account"),
            "installment_size": p.get("installment_size"),
            "outstanding": p.get("outstanding"),
            "paid_count": p.get("paid_count"),
            "total_installments": p.get("total_installments"),
            "next_date": p.get("next_date"),
            "payment_source": p.get("payment_source"),
            "status": "overdue" if nd < start else "due",
        }
        if nd < start:
            overdue.append(row)
            overdue_total += owed
        else:
            due.append(row)
            due_total += owed
    due.sort(key=lambda r: r["next_date"] or "")
    overdue.sort(key=lambda r: r["next_date"] or "")
    return {
        "count": len(due) + len(overdue),
        "due_this_month_total": round(due_total, 2),
        "overdue_total": round(overdue_total, 2),
        "plans": overdue + due,  # overdue first — most urgent for the CEO
    }


def _range_label(start: date, end: date) -> str:
    """Human label for the window: 'Aug 2026' for a single month, else 'Aug 2026 → Nov 2026'."""
    fmt = lambda d: d.strftime("%b %Y")
    if start.year == end.year and start.month == end.month:
        return fmt(start)
    return f"{fmt(start)} → {fmt(end)}"


async def _reconciled_pnl(session: AsyncSession) -> dict:
    """Latest fully-synced Xero P&L month (revenue income + expenses + net).

    Uses the newest available period in each table independently (revenue and
    expense syncs can land on different cadences). Everything here is labelled
    'reconciled / not live' — see the module docstring.
    """
    rev_periods = await get_available_revenue_periods(session)
    exp_periods = await get_available_periods(session)

    revenue = None
    if rev_periods:
        p = rev_periods[0]
        ps, pe = date.fromisoformat(p["period_start"]), date.fromisoformat(p["period_end"])
        r = await get_revenue_for_period(session, ps, pe)
        revenue = {
            "period_start": p["period_start"],
            "period_end": p["period_end"],
            "cash_collected": r["total_cash_collected"],
            "contract_value": r["total_contract_value"],
            "categories": r["categories"],
        }

    expenses = None
    if exp_periods:
        p = exp_periods[0]
        ps, pe = date.fromisoformat(p["period_start"]), date.fromisoformat(p["period_end"])
        e = await get_expenses_for_period(session, ps, pe)
        expenses = {
            "period_start": p["period_start"],
            "period_end": p["period_end"],
            "total": e["grand_total"],
            "buckets": e["buckets"],
        }

    net = None
    if revenue and expenses:
        net = round(revenue["cash_collected"] - expenses["total"], 2)

    return {
        "revenue": revenue,
        "expenses": expenses,
        "net": net,
        "note": "Reconciled Xero P&L — the last synced month, not live. "
                "Xero is only accurate after month-end reconciliation.",
    }


async def get_company_overview(
    session: AsyncSession, start: date, end: date
) -> dict:
    """Assemble the company-wide CEO overview for a date window [start, end].

    Composes live inflow by channel (Whop + bank wires), the collections snapshot,
    who-still-owes (payment-plan installments due/overdue), and the reconciled Xero
    P&L. No metric is recomputed here — each figure comes from its owning query fn.
    The window may span multiple months or be a custom day range (same bounds the
    Deals/Collections tab uses).
    """
    whop = await get_whop_live_summary_for_month(session, start, end)
    collections = await get_collections_for_range(session, start, end)
    bank = await get_bank_inflow_for_range(session, start, end)

    # Stripe is defensive: if the table isn't migrated yet or Stripe errors, the
    # channel degrades to $0 rather than 500-ing the whole CEO page.
    try:
        stripe = await get_stripe_inflow_for_range(session, start, end)
    except Exception as exc:
        logger.warning("Stripe inflow unavailable, defaulting to $0: %s", exc)
        stripe = {"total_usd": 0.0, "count": 0}

    whop_net = whop["totals"].get("net_cash_collected", 0.0) or 0.0
    bank_total = bank.get("total_usd", 0.0) or 0.0
    stripe_total = stripe.get("total_usd", 0.0) or 0.0

    live_inflow = {
        "total": round(whop_net + stripe_total + bank_total, 2),
        "by_channel": [
            {
                "channel": "Whop",
                "amount": round(whop_net, 2),
                "live": True,
                "detail": f"{whop['totals'].get('deal_count', 0)} deals",
            },
            {
                "channel": "Stripe (cards, subs & commissions)",
                "amount": round(stripe_total, 2),
                "live": True,
                "detail": f"{stripe.get('count', 0)} charges",
            },
            {
                "channel": "Bank wires (Wise / Payoneer)",
                "amount": round(bank_total, 2),
                "live": True,
                "detail": f"{bank.get('count', 0)} transfers",
                "by_source": bank.get("by_source", {}),
            },
        ],
        "last_refreshed": whop.get("last_refreshed"),
        "caveat": _INFLOW_CAVEAT,
    }

    ct = collections["totals"]
    collections_summary = {
        "collected": ct["collected"],
        "outstanding": ct["outstanding"],
        "total": ct["total"],
        "refunded": ct["refunded"],
        "net_collected": ct["net_collected"],
        "deal_count": ct["deal_count"],
        "months": collections["months"],
    }

    who_owes = _build_who_owes(collections, start, end)
    reconciled = await _reconciled_pnl(session)

    return {
        "range": {"start": str(start), "end": str(end)},
        "range_label": _range_label(start, end),
        "live_inflow": live_inflow,
        "collections": collections_summary,
        "who_owes": who_owes,
        "reconciled_pnl": reconciled,
        "roadmap": ROADMAP_CHANNELS,
    }


# ── Month-by-month operating dashboard ───────────────────────────────────────

def _trailing_window(months: int) -> tuple[date, date, list[str]]:
    """(start, end, ['YYYY-MM'...]) for the trailing `months` calendar months ending
    with the current month. `end` is the last day of the current month."""
    today = date.today()
    end = date(today.year, today.month, calendar.monthrange(today.year, today.month)[1])
    y, m = today.year, today.month - (months - 1)
    while m <= 0:
        m += 12
        y -= 1
    start = date(y, m, 1)
    keys, cy, cm = [], y, m
    while (cy, cm) <= (end.year, end.month):
        keys.append(f"{cy:04d}-{cm:02d}")
        cm += 1
        if cm > 12:
            cm = 1
            cy += 1
    return start, end, keys


def _series_from_by_month(by_month: list[dict], keys: list[str]) -> list[float]:
    """Align a [{month, amount}] list onto the continuous month axis (0.0 for gaps)."""
    lut = {r["month"]: r.get("amount", 0.0) for r in by_month}
    return [round(lut.get(k, 0.0), 2) for k in keys]


async def get_company_monthly_series(session: AsyncSession, months: int = 12) -> dict:
    """Month-by-month operating dashboard: cash inflow by channel, reconciled revenue,
    expenses by cost bucket, net profit, and MRR/ARR — over the trailing `months`.

    Purely composed from the owning query fns (no metric recomputed). LIVE operational
    inflow (Whop/Stripe/bank) sits alongside RECONCILED Xero revenue/expenses; the
    frontend labels each. Reconciled/MRR points are null for months with no data.
    """
    months = max(1, min(months, 36))
    start, end, keys = _trailing_window(months)

    whop = await get_whop_inflow_by_month(session, start, end)
    stripe = await get_stripe_inflow_for_range(session, start, end)
    bank = await get_bank_inflow_for_range(session, start, end)
    direct = await get_direct_wire_by_month(session, start, end)
    exp = await get_expenses_by_month(session, start, end)
    stats = await get_whop_stats_by_month(session, start, end)
    latest_stats = await get_latest_whop_stats(session)
    streams = await get_revenue_streams_by_month(session, start, end)

    # Reconciled Xero revenue (cash_collected) keyed by month.
    rev_rows = await get_all_revenue_periods_summary(session)
    rev_by_month = {
        r["period_start"][:7]: r.get("cash_collected", 0.0)
        for r in rev_rows if r.get("period_start")
    }

    whop_s = _series_from_by_month(whop["by_month"], keys)
    stripe_s = _series_from_by_month(stripe["by_month"], keys)
    bank_s = _series_from_by_month(bank["by_month"], keys)
    inflow_s = [round(whop_s[i] + stripe_s[i] + bank_s[i], 2) for i in range(len(keys))]

    # Reconciled revenue + net profit are null where a month hasn't been Xero-synced.
    revenue_s = [rev_by_month.get(k) for k in keys]
    expense_total = exp["totals"]
    expenses_s = [round(expense_total.get(k, 0.0), 2) for k in keys]
    net_profit_s = [
        (round(revenue_s[i] - expenses_s[i], 2) if revenue_s[i] is not None else None)
        for i in range(len(keys))
    ]

    mrr_s = [stats["by_month"].get(k, {}).get("mrr") for k in keys]
    arr_s = [stats["by_month"].get(k, {}).get("arr") for k in keys]

    # De-duplicated cash-in channel mix — each dollar counted ONCE, at its true origin:
    #   Whop gross + Stripe gross + DIRECT client wires (bank receipts that are NOT
    #   Whop/Stripe payouts). This is the honest "where does the money come from" split;
    #   it does NOT sum Whop and its own bank payout (the old rail chart's double-count).
    wise_direct = direct["by_bank"].get("Wise (direct)", {})
    payo_direct = direct["by_bank"].get("Payoneer (direct)", {})
    wise_s = [round(wise_direct.get(k, 0.0), 2) for k in keys]
    payo_s = [round(payo_direct.get(k, 0.0), 2) for k in keys]
    mix_channels = [
        {"key": "whop", "label": "Whop", "data": whop_s},
        {"key": "stripe", "label": "Stripe", "data": stripe_s},
        {"key": "wise", "label": "Wise (direct wires)", "data": wise_s},
        {"key": "payoneer", "label": "Payoneer (direct wires)", "data": payo_s},
    ]
    mix_totals = {c["label"]: round(sum(c["data"]), 2) for c in mix_channels}
    mix_grand = round(sum(mix_totals.values()), 2) or 0.0
    mix_pct = {lbl: (round(v / mix_grand * 100, 1) if mix_grand else 0.0) for lbl, v in mix_totals.items()}

    # Revenue streams — two monthly lines: high-ticket offer deals vs recurring subs.
    hi_ticket_s = [round(streams["by_stream"]["high_ticket"].get(k, 0.0), 2) for k in keys]
    recurring_s = [round(streams["by_stream"]["recurring"].get(k, 0.0), 2) for k in keys]

    # Cost buckets present in the window (ordered), each aligned onto the month axis.
    cost_buckets = [b for b in BUCKET_ORDER if b in exp["by_bucket"]]
    costs_by_bucket = {
        b: [round(exp["by_bucket"][b].get(k, 0.0), 2) for k in keys]
        for b in cost_buckets
    }

    def _sum(xs):
        return round(sum(x for x in xs if x is not None), 2)

    return {
        "months": keys,
        "window_months": months,
        "series": {
            "cash_inflow": inflow_s,
            "whop": whop_s,
            "stripe": stripe_s,
            "bank": bank_s,
            "revenue_reconciled": revenue_s,
            "expenses": expenses_s,
            "net_profit": net_profit_s,
            "mrr": mrr_s,
            "arr": arr_s,
        },
        "costs": {
            "buckets": [{"key": b, "label": BUCKET_LABELS.get(b, b)} for b in cost_buckets],
            "by_bucket": costs_by_bucket,
            "total": expenses_s,
        },
        "revenue_streams": {
            "high_ticket": hi_ticket_s,
            "recurring": recurring_s,
            "note": "High-ticket = offer deals (Splitit/installment/upfront) — one-time-ish, "
                    "how much came in that month. Recurring = GHL sub-account subscriptions + "
                    "referral, the reliable monthly base. From the Xero P&L income accounts.",
        },
        "channel_mix": {
            "channels": [{"key": c["key"], "label": c["label"]} for c in mix_channels],
            "by_channel": {c["label"]: c["data"] for c in mix_channels},
            "totals": mix_totals,
            "grand_total": mix_grand,
            "pct": mix_pct,
            "note": "De-duplicated: Whop & Stripe gross + DIRECT client wires only. Whop/Stripe "
                    "payouts landing in the bank are excluded so no dollar is counted twice.",
        },
        "latest_stats": latest_stats,
        "totals": {
            "cash_inflow": _sum(inflow_s),
            "whop": _sum(whop_s),
            "stripe": _sum(stripe_s),
            "bank": _sum(bank_s),
            "revenue_reconciled": _sum(revenue_s),
            "expenses": _sum(expenses_s),
            "net_profit": _sum(net_profit_s),
        },
        "notes": {
            "inflow": "Operational cash in — LIVE (Whop + Stripe + bank wires).",
            "revenue": "Reconciled Xero cash collected — trails ~10 days; blank months are not yet synced.",
            "mrr": "MRR = active recurring Whop subscriptions (excludes Splitit/ClarityPay "
                   "deal financing), snapshotted on sync. Lower than Whop's dashboard, which "
                   "counts in-progress installment plans as recurring. History builds forward "
                   "from the first snapshot.",
        },
    }
