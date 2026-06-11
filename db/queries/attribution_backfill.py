"""One-shot rep-attribution backfill queries.

Fills in NULL opportunity owner names from a complete GHL user map, then propagates
the resolved names onto matched deals. Set-based and idempotent — only touches rows
whose owner name is still NULL, so re-running is a no-op once resolved.
"""

from sqlalchemy import text, update
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity


async def backfill_opportunity_owner_names(
    session: AsyncSession, user_map: dict[str, str]
) -> int:
    """Fill NULL opportunity_owner_name from the user map keyed by owner_id. Returns rows updated."""
    updated = 0
    for uid, name in user_map.items():
        if not name:
            continue
        result = await session.execute(
            update(Opportunity)
            .where(Opportunity.opportunity_owner_id == uid)
            .where(Opportunity.opportunity_owner_name.is_(None))
            .values(opportunity_owner_name=name)
        )
        updated += result.rowcount or 0
    await session.commit()
    return updated


async def propagate_owner_names_to_deal_matches(session: AsyncSession) -> int:
    """Copy resolved opportunity owner names onto matched deals with a NULL owner. Returns rows updated."""
    result = await session.execute(
        text(
            """
            UPDATE deal_whop_matches dwm
            SET ghl_owner_name = o.opportunity_owner_name,
                updated_at = now()
            FROM opportunities o
            WHERE dwm.ghl_opportunity_id = o.ghl_opportunity_id
              AND o.opportunity_owner_name IS NOT NULL
              AND dwm.ghl_owner_name IS NULL
            """
        )
    )
    await session.commit()
    return result.rowcount or 0
