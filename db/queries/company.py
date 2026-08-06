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

from db.queries.collections import get_collections_for_range
from db.queries.expenses import get_available_periods, get_expenses_for_period
from db.queries.revenue import get_available_revenue_periods, get_revenue_for_period
from db.queries.whop_live import get_whop_live_summary_for_month
from db.queries.wise_transfers import get_bank_inflow_for_range

# Inflow channels not yet wired live — shown to the CEO as "coming" so the page is
# honest about what the live number does and does not include today.
ROADMAP_CHANNELS = [
    "Stripe live cash (incl. GHL sub-account subscriptions & commissions)",
    "Direct Payoneer connection (beyond the Xero bank feed)",
    "Affiliate / referral revenue",
]

_INFLOW_CAVEAT = (
    "Live figure = Whop net cash + bank wires received (Wise/Payoneer via the Xero "
    "bank feed). A wire that also settles a deal tracked in Collections can overlap; "
    "treat the combined total as directional. Stripe and direct-Payoneer rails are "
    "added in later stages."
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

    whop_net = whop["totals"].get("net_cash_collected", 0.0) or 0.0
    bank_total = bank.get("total_usd", 0.0) or 0.0

    live_inflow = {
        "total": round(whop_net + bank_total, 2),
        "by_channel": [
            {
                "channel": "Whop",
                "amount": round(whop_net, 2),
                "live": True,
                "detail": f"{whop['totals'].get('deal_count', 0)} deals",
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
