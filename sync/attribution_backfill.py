"""One-shot orchestrator: re-resolve rep attribution without a full re-sync.

Fetches the complete GHL user map (via the fixed get_users name resolution), backfills
NULL opportunity owner names, then propagates them onto matched deals. Fast (one GHL call
+ two set-based updates) and idempotent — the fast alternative to a full sync when only
rep attribution needs fixing.
"""

import asyncio
import logging

from db.queries.attribution_backfill import (
    backfill_opportunity_owner_names,
    get_ownerless_won_deals,
    propagate_owner_names_to_deal_matches,
    set_opportunity_owner,
)
from db.session import AsyncSessionLocal
from sync.ghl_client import FOLLOW_UP_CALENDAR_IDS, GHLClient

logger = logging.getLogger(__name__)


async def backfill_rep_attribution() -> dict:
    """Run the one-shot rep-attribution backfill. Returns {users_resolved, opportunities_updated, deal_matches_updated}."""
    user_map = await GHLClient().get_users()
    resolved = len([n for n in user_map.values() if n])
    logger.info("[attribution-backfill] start — %d users resolved from GHL", resolved)

    async with AsyncSessionLocal() as session:
        opps_updated = await backfill_opportunity_owner_names(session, user_map)
        deals_updated = await propagate_owner_names_to_deal_matches(session)

    logger.info(
        "[attribution-backfill] done — opps=%d deals=%d users=%d",
        opps_updated, deals_updated, resolved,
    )
    return {
        "users_resolved": resolved,
        "opportunities_updated": opps_updated,
        "deal_matches_updated": deals_updated,
    }


async def backfill_owner_from_appointments() -> dict:
    """Recover owner for owner-less won deals using the Call-2 (follow-up) appointment's assigned rep."""
    ghl = GHLClient()
    user_map = await ghl.get_users()
    stats = {"checked": 0, "recovered": 0, "still_unassigned": 0, "errors": 0}

    async with AsyncSessionLocal() as session:
        deals = await get_ownerless_won_deals(session)
        logger.info("[appt-owner-backfill] start — %d owner-less won deals", len(deals))

        for opp_id, contact_id in deals:
            try:
                appts = await ghl.get_contact_appointments(contact_id)
                followup = next(
                    (a for a in appts if a.get("calendarId") in FOLLOW_UP_CALENDAR_IDS), None
                )
                uid = followup.get("assignedUserId") if followup else None
                name = user_map.get(uid) if uid else None
                stats["checked"] += 1
                if uid and name:
                    await set_opportunity_owner(session, opp_id, uid, name)
                    stats["recovered"] += 1
                else:
                    stats["still_unassigned"] += 1
                await asyncio.sleep(0.12)
            except Exception as exc:
                logger.error("[appt-owner-backfill] error on %s: %s", opp_id, exc, exc_info=True)
                stats["errors"] += 1

        await propagate_owner_names_to_deal_matches(session)

    logger.info("[appt-owner-backfill] done — %s", stats)
    return stats
