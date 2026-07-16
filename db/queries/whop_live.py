"""Live Whop revenue queries — the real-time P&L read surface.

Separate from deal_matches.py (which owns the reconciliation/matching queries).
These three functions serve the daily-refreshed "Live Whop Revenue" section:

  - get_current_month_refresh_targets: rows the EOD cron should re-fetch from Whop
  - update_live_payment_metrics:       overwrite cash/fee fields after a fresh fetch
  - get_whop_live_summary_for_month:   per-rep aggregation for the dashboard endpoint

All three operate on deal_whop_matches. None touch identity or is_confirmed columns.
"""

from datetime import date, datetime, timezone

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import DealWhopMatch

LIVE_CONFIDENCE_TIERS = ("high", "medium")


async def get_current_month_refresh_targets(
    session: AsyncSession,
    month_start: date,
    month_end: date,
    stale_before: datetime,
) -> list[DealWhopMatch]:
    """Return current-month high/medium matches whose Whop payment data is due a refresh.

    A row qualifies when its first payment landed in [month_start, month_end], it has a
    Whop membership to re-fetch, and it has either never had metrics computed or was last
    refreshed before `stale_before` (the idempotency window cutoff).
    """
    rows = (await session.execute(
        select(DealWhopMatch)
        .where(DealWhopMatch.first_payment_date >= month_start)
        .where(DealWhopMatch.first_payment_date <= month_end)
        .where(DealWhopMatch.match_confidence.in_(LIVE_CONFIDENCE_TIERS))
        .where(DealWhopMatch.whop_membership_id.isnot(None))
        .where(
            (DealWhopMatch.metrics_updated_at.is_(None))
            | (DealWhopMatch.metrics_updated_at < stale_before)
        )
    )).scalars().all()
    return list(rows)


async def update_live_payment_metrics(
    session: AsyncSession,
    ghl_opportunity_id: str,
    metrics: dict,
) -> bool:
    """Overwrite the live cash/fee metric fields on one deal match row.

    Unlike enrich_deal_match_payments (fills NULLs only), this always overwrites —
    net cash changes as new installments land. Never touches total_installments
    (authoritative plan length), identity, or is_confirmed columns.
    """
    updates = {
        "net_cash_collected": metrics.get("net_cash_collected"),
        "provider_fee_pct": metrics.get("provider_fee_pct"),
        "is_splitit": metrics.get("is_splitit"),
        "is_claritypay": metrics.get("is_claritypay"),
        "plan_months_flag": metrics.get("plan_months_flag"),
        "total_paid": metrics.get("total_paid"),
        "upfront_cash": metrics.get("upfront_cash"),
        "payment_count": metrics.get("payment_count"),
        "remaining_ar": metrics.get("remaining_ar"),
        "is_financing": metrics.get("is_financing"),
        # first_payment_date can move EARLIER when a sibling membership's payment
        # is folded in (one deal settled across two memberships) — keep it true
        # to the earliest paid payment; sales cycle + month bucketing follow.
        "first_payment_date": metrics.get("first_payment_date"),
        # total_installments: written directly from the fresh computation — the
        # refresh derives it from the LIVE membership's split_pay (authoritative)
        # or re-runs the inference, so it self-corrects in both directions
        # (an old over-estimate shrinks, a grown plan expands). Never below the
        # payments already collected (enforced in _compute_payment_metrics).
        "total_installments": metrics.get("total_installments"),
        "metrics_updated_at": func.now(),
        "updated_at": func.now(),
    }
    stmt = (
        update(DealWhopMatch)
        .where(DealWhopMatch.ghl_opportunity_id == ghl_opportunity_id)
        .values(**updates)
    )
    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount > 0


def _projected_total(r: DealWhopMatch) -> float | None:
    """Payment-verified projected full contract — mirror of
    common.whop_projected_total_expr() / deal_matches._projected()."""
    if r.total_paid is None:
        return None
    paid = float(r.total_paid)
    if r.is_splitit or r.is_claritypay:
        return paid  # financed → settles 100% upfront
    if r.total_installments and r.total_installments > 0 and r.payment_count and r.payment_count > 0:
        return paid / r.payment_count * r.total_installments
    return paid  # pay-in-full or plan length unknown


def _non_whop_cash(r: DealWhopMatch) -> float:
    """Best-available cash figure for a won deal that settled OUTSIDE Whop (e.g. wire).
    Prefer the GHL cash_collected field, then any recorded total_paid, then the
    contract value (a wire transfer is typically paid in full)."""
    if r.ghl_cash_collected:
        return float(r.ghl_cash_collected)
    if r.total_paid:
        return float(r.total_paid)
    g = r.total_contract_value if r.total_contract_value is not None else r.ghl_monetary_value
    return float(g) if g is not None else 0.0


def _deal_to_live_item(r: DealWhopMatch) -> dict:
    """Shape one DealWhopMatch row into a live-revenue deal item dict.

    needs_review = the deal was won this month but has NO Whop payment (wire/other).
    Such deals are shown highlighted and only counted in the rep/portfolio totals
    once a human confirms them (is_confirmed); until then they sit in a pending bucket.
    """
    gross = r.total_contract_value if r.total_contract_value is not None else r.ghl_monetary_value
    needs_review = r.first_payment_date is None
    if needs_review:
        # No Whop payment: cash comes from GHL / manual entry, no provider fee.
        cash = _non_whop_cash(r) if r.is_confirmed else None
        net = cash
        projected = cash
    else:
        # Pre-refresh fallback: if net hasn't been computed yet, show gross paid (no fee).
        net = r.net_cash_collected if r.net_cash_collected is not None else r.total_paid
        cash = r.total_paid
        projected = _projected_total(r)
    return {
        "ghl_opportunity_id": r.ghl_opportunity_id,
        "ghl_opportunity_name": r.ghl_opportunity_name,
        "ghl_close_date": str(r.ghl_close_date) if r.ghl_close_date else None,
        "first_payment_date": str(r.first_payment_date) if r.first_payment_date else None,
        "whop_email": r.whop_email,
        "ghl_contact_email": r.ghl_contact_email,
        "gross_contract_value": float(gross) if gross is not None else None,
        "upfront_cash": float(r.upfront_cash) if r.upfront_cash is not None else None,
        "total_paid": float(cash) if cash is not None else None,
        "whop_projected": float(projected) if projected is not None else None,
        "net_cash_collected": float(net) if net is not None else None,
        "remaining_ar": float(r.remaining_ar) if r.remaining_ar is not None else None,
        "provider_fee_pct": float(r.provider_fee_pct) if r.provider_fee_pct is not None else None,
        "payment_count": r.payment_count,
        "is_splitit": r.is_splitit,
        "is_claritypay": r.is_claritypay,
        "plan_months_flag": r.plan_months_flag,
        "match_confidence": r.match_confidence,
        "total_installments": r.total_installments,
        "needs_review": needs_review,
        "is_confirmed": bool(r.is_confirmed),
    }


async def get_whop_live_summary_for_month(
    session: AsyncSession,
    month_start: date,
    month_end: date,
) -> dict:
    """Aggregate per-rep live Whop revenue for a calendar month.

    Two groups land in the month:
      • Whop-settled — high/medium matches whose FIRST PAYMENT landed in the month.
      • Needs-review — deals WON in the month (by GHL/won-status close date) with NO
        Whop payment yet (e.g. wire transfer). Shown highlighted; only added to the
        counted totals once confirmed (is_confirmed), otherwise held in a pending bucket.

    Deals with no rep owner are grouped under "Unassigned". Returns reps (sorted by net
    cash desc), portfolio totals, and the most recent refresh timestamp.
    """
    rows = (await session.execute(
        select(DealWhopMatch).where(
            or_(
                # Whop-settled this month
                and_(
                    DealWhopMatch.first_payment_date >= month_start,
                    DealWhopMatch.first_payment_date <= month_end,
                    DealWhopMatch.match_confidence.in_(LIVE_CONFIDENCE_TIERS),
                ),
                # Won this month with no Whop payment → needs review (wire/other).
                # Excludes deals a reviewer has explicitly ignored (hidden).
                and_(
                    DealWhopMatch.first_payment_date.is_(None),
                    DealWhopMatch.ghl_close_date >= month_start,
                    DealWhopMatch.ghl_close_date <= month_end,
                    DealWhopMatch.is_ignored.isnot(True),
                ),
            )
        )
    )).scalars().all()

    rep_buckets: dict[str, dict] = {}
    last_refreshed: datetime | None = None

    for r in rows:
        rep = r.ghl_owner_name or "Unassigned"
        bucket = rep_buckets.setdefault(rep, {
            "rep_name": rep,
            "deal_count": 0,
            "gross_contract_value": 0.0,
            "net_cash_collected": 0.0,
            "flagged_count": 0,
            "pending_count": 0,
            "pending_contract_value": 0.0,
            "deals": [],
        })
        item = _deal_to_live_item(r)
        counts = (not item["needs_review"]) or item["is_confirmed"]
        if counts:
            bucket["deal_count"] += 1
            bucket["gross_contract_value"] += item["gross_contract_value"] or 0.0
            bucket["net_cash_collected"] += item["net_cash_collected"] or 0.0
            if r.plan_months_flag:
                bucket["flagged_count"] += 1
        else:  # unconfirmed needs-review → pending, not counted
            bucket["pending_count"] += 1
            bucket["pending_contract_value"] += item["gross_contract_value"] or 0.0
        bucket["deals"].append(item)

        if r.metrics_updated_at and (last_refreshed is None or r.metrics_updated_at > last_refreshed):
            last_refreshed = r.metrics_updated_at

    # Within a rep: Whop-settled first (newest first), then needs-review pending.
    reps = sorted(rep_buckets.values(), key=lambda b: b["net_cash_collected"], reverse=True)
    for b in reps:
        settled = sorted([d for d in b["deals"] if not d["needs_review"]], key=lambda d: d["first_payment_date"] or "", reverse=True)
        pending = sorted([d for d in b["deals"] if d["needs_review"]], key=lambda d: d["ghl_close_date"] or "", reverse=True)
        b["deals"] = settled + pending
        b["gross_contract_value"] = round(b["gross_contract_value"], 2)
        b["net_cash_collected"] = round(b["net_cash_collected"], 2)
        b["pending_contract_value"] = round(b["pending_contract_value"], 2)

    totals = {
        "gross_contract_value": round(sum(b["gross_contract_value"] for b in reps), 2),
        "net_cash_collected": round(sum(b["net_cash_collected"] for b in reps), 2),
        "deal_count": sum(b["deal_count"] for b in reps),
        "flagged_count": sum(b["flagged_count"] for b in reps),
        "pending_count": sum(b["pending_count"] for b in reps),
        "pending_contract_value": round(sum(b["pending_contract_value"] for b in reps), 2),
    }

    return {
        "reps": reps,
        "totals": totals,
        "last_refreshed": last_refreshed.isoformat() if last_refreshed else None,
    }


async def get_available_deal_months(session: AsyncSession) -> list[str]:
    """Return distinct YYYY-MM that have high/medium deals, newest first, current month always included."""
    rows = (await session.execute(
        select(func.to_char(DealWhopMatch.first_payment_date, "YYYY-MM"))
        .where(DealWhopMatch.first_payment_date.isnot(None))
        .where(DealWhopMatch.match_confidence.in_(LIVE_CONFIDENCE_TIERS))
        .distinct()
    )).scalars().all()
    months = {m for m in rows if m}
    months.add(datetime.now(timezone.utc).strftime("%Y-%m"))
    return sorted(months, reverse=True)
