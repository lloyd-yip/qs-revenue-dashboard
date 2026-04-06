"""Rep list query — populates the rep filter dropdown."""

from sqlalchemy import and_, distinct, select
from sqlalchemy.ext.asyncio import AsyncSession

from config import REP_ROSTER, ACTIVE_REP_NAMES, INACTIVE_REP_NAMES, OTHER_REP_NAMES
from db.models import Opportunity


async def get_reps(session: AsyncSession) -> list[dict]:
    """Return all distinct reps who own at least one non-excluded opportunity.

    Each rep includes a 'category' field (active / inactive / other) from the
    REP_ROSTER config. Reps not in the roster are classified as 'other'.
    Results are returned grouped by category: active first, then inactive, then other.
    """
    result = await session.execute(
        select(
            Opportunity.opportunity_owner_id,
            Opportunity.opportunity_owner_name,
        )
        .where(
            and_(
                Opportunity.is_excluded.is_(False),
                Opportunity.opportunity_owner_id.isnot(None),
                Opportunity.opportunity_owner_name.isnot(None),
            )
        )
        .distinct()
        .order_by(Opportunity.opportunity_owner_name)
    )

    reps = []
    for row in result.all():
        name = row.opportunity_owner_name or "Unassigned"
        category = REP_ROSTER.get(name, "other")
        reps.append({
            "rep_id": row.opportunity_owner_id,
            "rep_name": name,
            "category": category,
        })

    # Sort by category priority (active → inactive → other), then by name
    CATEGORY_ORDER = {"active": 0, "inactive": 1, "other": 2}
    reps.sort(key=lambda r: (CATEGORY_ORDER.get(r["category"], 2), r["rep_name"]))

    return reps
