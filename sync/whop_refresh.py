"""Daily EOD refresh of current-month Whop payment metrics.

Lightweight alternative to the full matching engine (match_deals_whop.run_matching):
skips GHL contact resolution and the Stripe pass, hitting only the Whop payments API
for current-month high/medium matches that already have a Whop membership. Designed to
run once a day so the "Live Whop Revenue" P&L section reflects last night's cash.

Resilience:
  - Per-row failure isolation — one Whop error increments `errors` and continues.
  - Idempotency — stale-window filtering happens in the query (rows refreshed within
    REFRESH_STALE_HOURS are not returned), so a double-fire in the same evening is a no-op.
  - Per-row commit — each row is committed independently; a late failure never rolls
    back rows already refreshed.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import httpx

from sqlalchemy import select

from db.models import DealWhopMatch
from db.queries.whop_live import (
    get_current_month_refresh_targets,
    update_live_payment_metrics,
)
from db.session import AsyncSessionLocal
from sync.whop_payments import (
    _compute_payment_metrics,
    _fetch_whop_memberships,
    build_membership_email_index,
    build_payment_indexes,
    collect_customer_payments,
    fetch_all_payments,
    membership_is_recurring,
    sibling_memberships,
)

logger = logging.getLogger(__name__)

REFRESH_STALE_HOURS = 6       # rows refreshed within this window are skipped (idempotency)
WHOP_CALL_DELAY_SEC = 0.2     # cushion between Whop API calls to respect rate limits


# Rolling refresh window. Was current-month-only, but Cash Collected stacks
# installments and deals run up to ~6-month payment plans — a deal whose first
# payment landed in a PREVIOUS month must still pick up later installments
# (e.g. first payment Jun 11, next renewal Jul 14).
REFRESH_LOOKBACK_DAYS = 190


async def refresh_current_month_payment_metrics() -> dict:
    """Re-fetch Whop payments for recent high/medium matches and overwrite net cash.

    Covers deals whose first payment landed within REFRESH_LOOKBACK_DAYS, so
    installment plans keep stacking after their first month.
    Returns {refreshed, skipped, errors, flagged}. `skipped` reflects stale-window
    filtering done at the query layer (those rows are never fetched).
    """
    started = datetime.now(timezone.utc)
    month_start = started.date() - timedelta(days=REFRESH_LOOKBACK_DAYS)
    month_end = started.date()
    stale_before = started - timedelta(hours=REFRESH_STALE_HOURS)

    stats = {"refreshed": 0, "skipped": 0, "errors": 0, "flagged": 0}

    async with AsyncSessionLocal() as session:
        targets = await get_current_month_refresh_targets(
            session, month_start, month_end, stale_before
        )
        # Snapshot the fields we need before any commit expires the ORM objects.
        snapshots = [
            {
                "ghl_opportunity_id": r.ghl_opportunity_id,
                "whop_membership_id": r.whop_membership_id,
                "ghl_monetary_value": float(r.ghl_monetary_value or 0),
                "total_installments": r.total_installments,
                "whop_email": r.whop_email,
                "ghl_close_date": r.ghl_close_date,
            }
            for r in targets
        ]
        logger.info(
            f"[whop-refresh] start month={month_start}..{month_end} "
            f"targets={len(snapshots)}"
        )
        if not snapshots:
            logger.info("[whop-refresh] no targets — nothing to refresh")
            return stats

        # Membership index + claim maps for sibling-payment folding — a customer
        # can settle one deal across multiple memberships (Splitit over two cards).
        # Same guards as the matching engine: never fold when the customer has
        # other matched deals, or a membership claimed by a different deal.
        claim_rows = await session.execute(
            select(
                DealWhopMatch.ghl_opportunity_id,
                DealWhopMatch.whop_membership_id,
                DealWhopMatch.whop_email,
            ).where(DealWhopMatch.whop_membership_id.isnot(None))
        )
        claimed_by_membership: dict[str, str] = {}
        deals_by_email: dict[str, set] = {}
        for opp_id, mid, email in claim_rows.all():
            claimed_by_membership[mid] = opp_id
            if email:
                deals_by_email.setdefault(email, set()).add(opp_id)

        async with httpx.AsyncClient(timeout=30.0) as whop_client:
            memberships = await _fetch_whop_memberships(whop_client)
            memberships_by_email = build_membership_email_index(memberships)
            membership_by_id = {m.get("id"): m for m in memberships if m.get("id")}
            # One company-wide payments sweep — also surfaces membership-less
            # direct charges (renewals), invisible to membership-scoped fetches.
            all_payments = await fetch_all_payments(whop_client)
            by_membership, unattached_by_user = build_payment_indexes(all_payments)

            for snap in snapshots:
                try:
                    matched_m = membership_by_id.get(snap["whop_membership_id"])
                    siblings = []
                    other_deal_ids = {
                        o for o in deals_by_email.get(snap["whop_email"] or "", set())
                        if o != snap["ghl_opportunity_id"]
                    }
                    if matched_m and not other_deal_ids:
                        claimed_other = {
                            mid for mid, opp in claimed_by_membership.items()
                            if opp != snap["ghl_opportunity_id"]
                        }
                        siblings = sibling_memberships(
                            matched_m, memberships_by_email, claimed_other,
                            snap["ghl_close_date"],
                        )
                    payments, fold_notes = collect_customer_payments(
                        matched_m or {"id": snap["whop_membership_id"]},
                        siblings if not other_deal_ids else [],
                        by_membership,
                        unattached_by_user if not other_deal_ids else {},
                        snap["ghl_close_date"],
                    )
                    if fold_notes:
                        logger.info(
                            f"[whop-refresh] {snap['ghl_opportunity_id']}: folded — "
                            + "; ".join(fold_notes)
                        )
                    metrics = _compute_payment_metrics(
                        payments,
                        snap["ghl_monetary_value"],
                        installments_override=snap["total_installments"],
                        is_recurring=membership_is_recurring(matched_m),
                    )
                    updated = await update_live_payment_metrics(
                        session, snap["ghl_opportunity_id"], metrics
                    )
                    if updated:
                        stats["refreshed"] += 1
                        if metrics.get("plan_months_flag"):
                            stats["flagged"] += 1
                    await asyncio.sleep(WHOP_CALL_DELAY_SEC)
                except Exception as exc:
                    logger.error(
                        f"[whop-refresh] error on {snap['ghl_opportunity_id']} "
                        f"(membership {snap['whop_membership_id']}): {exc}",
                        exc_info=True,
                    )
                    stats["errors"] += 1

    duration_ms = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
    logger.info(
        f"[whop-refresh] complete refreshed={stats['refreshed']} "
        f"errors={stats['errors']} flagged={stats['flagged']} duration_ms={duration_ms}"
    )
    return stats
