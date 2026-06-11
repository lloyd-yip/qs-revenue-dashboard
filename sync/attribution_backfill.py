"""One-shot orchestrator: re-resolve rep attribution without a full re-sync.

Fetches the complete GHL user map (via the fixed get_users name resolution), backfills
NULL opportunity owner names, then propagates them onto matched deals. Fast (one GHL call
+ two set-based updates) and idempotent — the fast alternative to a full sync when only
rep attribution needs fixing.
"""

import logging

from db.queries.attribution_backfill import (
    backfill_opportunity_owner_names,
    propagate_owner_names_to_deal_matches,
)
from db.session import AsyncSessionLocal
from sync.ghl_client import GHLClient

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
