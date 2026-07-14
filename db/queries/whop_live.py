"""Live Whop revenue queries — the real-time P&L read surface.

Separate from deal_matches.py (which owns the reconciliation/matching queries).
These three functions serve the daily-refreshed "Live Whop Revenue" section:

  - get_current_month_refresh_targets: rows the EOD cron should re-fetch from Whop
  - update_live_payment_metrics:       overwrite cash/fee fields after a fresh fetch
  - get_whop_live_summary_for_month:   per-rep aggregation for the dashboard endpoint

All three operate on deal_whop_matches. None touch identity or is_confirmed columns.
"""

from datetime import date, datetime, timezone

from sqlalchemy import func, select, update
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


def _deal_to_live_item(r: DealWhopMatch) -> dict:
    """Shape one DealWhopMatch row into a live-revenue deal item dict."""
    gross = r.total_contract_value if r.total_contract_value is not None else r.ghl_monetary_value
    # Pre-refresh fallback: if net hasn't been computed yet, show gross paid (no fee applied).
    net = r.net_cash_collected if r.net_cash_collected is not None else r.total_paid
    return {
        "ghl_opportunity_id": r.ghl_opportunity_id,
        "ghl_opportunity_name": r.ghl_opportunity_name,
        "ghl_close_date": str(r.ghl_close_date) if r.ghl_close_date else None,
        "first_payment_date": str(r.first_payment_date) if r.first_payment_date else None,
        "gross_contract_value": float(gross) if gross is not None else None,
        "total_paid": float(r.total_paid) if r.total_paid is not None else None,
        "net_cash_collected": float(net) if net is not None else None,
        "provider_fee_pct": float(r.provider_fee_pct) if r.provider_fee_pct is not None else None,
        "is_splitit": r.is_splitit,
        "is_claritypay": r.is_claritypay,
        "plan_months_flag": r.plan_months_flag,
        "match_confidence": r.match_confidence,
        "total_installments": r.total_installments,
    }


async def get_whop_live_summary_for_month(
    session: AsyncSession,
    month_start: date,
    month_end: date,
) -> dict:
    """Aggregate per-rep live Whop revenue for a calendar month.

    Includes high/medium matches whose first payment landed in the month. Deals with no
    rep owner are grouped under "Unassigned". Returns reps (sorted by net cash desc),
    portfolio totals, and the most recent refresh timestamp.
    """
    rows = (await session.execute(
        select(DealWhopMatch)
        .where(DealWhopMatch.first_payment_date >= month_start)
        .where(DealWhopMatch.first_payment_date <= month_end)
        .where(DealWhopMatch.match_confidence.in_(LIVE_CONFIDENCE_TIERS))
        .order_by(DealWhopMatch.first_payment_date.desc())
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
            "deals": [],
        })
        item = _deal_to_live_item(r)
        bucket["deal_count"] += 1
        bucket["gross_contract_value"] += item["gross_contract_value"] or 0.0
        bucket["net_cash_collected"] += item["net_cash_collected"] or 0.0
        if r.plan_months_flag:
            bucket["flagged_count"] += 1
        bucket["deals"].append(item)

        if r.metrics_updated_at and (last_refreshed is None or r.metrics_updated_at > last_refreshed):
            last_refreshed = r.metrics_updated_at

    reps = sorted(rep_buckets.values(), key=lambda b: b["net_cash_collected"], reverse=True)
    for b in reps:
        b["gross_contract_value"] = round(b["gross_contract_value"], 2)
        b["net_cash_collected"] = round(b["net_cash_collected"], 2)

    totals = {
        "gross_contract_value": round(sum(b["gross_contract_value"] for b in reps), 2),
        "net_cash_collected": round(sum(b["net_cash_collected"] for b in reps), 2),
        "deal_count": sum(b["deal_count"] for b in reps),
        "flagged_count": sum(b["flagged_count"] for b in reps),
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
