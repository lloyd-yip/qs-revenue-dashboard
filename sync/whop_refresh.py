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

from db.queries.whop_live import (
    get_current_month_refresh_targets,
    update_live_payment_metrics,
)
from db.session import AsyncSessionLocal
from sync.match_deals_whop import _compute_payment_metrics, _fetch_membership_payments

logger = logging.getLogger(__name__)

REFRESH_STALE_HOURS = 6       # rows refreshed within this window are skipped (idempotency)
WHOP_CALL_DELAY_SEC = 0.2     # cushion between Whop API calls to respect rate limits


def _current_month_bounds(today) -> tuple:
    """Return (first_day, last_day) date bounds for the month containing `today`."""
    first = today.replace(day=1)
    if first.month == 12:
        next_first = first.replace(year=first.year + 1, month=1)
    else:
        next_first = first.replace(month=first.month + 1)
    return first, next_first - timedelta(days=1)


async def refresh_current_month_payment_metrics() -> dict:
    """Re-fetch Whop payments for current-month high/medium matches and overwrite net cash.

    Returns {refreshed, skipped, errors, flagged}. `skipped` reflects stale-window
    filtering done at the query layer (those rows are never fetched).
    """
    started = datetime.now(timezone.utc)
    month_start, month_end = _current_month_bounds(started.date())
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

        async with httpx.AsyncClient(timeout=30.0) as whop_client:
            for snap in snapshots:
                try:
                    payments = await _fetch_membership_payments(
                        whop_client, snap["whop_membership_id"]
                    )
                    metrics = _compute_payment_metrics(
                        payments,
                        snap["ghl_monetary_value"],
                        installments_override=snap["total_installments"],
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
