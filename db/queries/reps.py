"""Rep list query — populates the rep filter dropdown."""

from sqlalchemy import and_, distinct, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity


async def get_reps(session: AsyncSession) -> list[dict]:
    """Return all distinct reps who own at least one non-excluded opportunity."""
    result = await session.execute(
        select(
            Opportunity.opportunity_owner_id,
            Opportunity.opportunity_owner_name,
        )
        .where(
            and_(
                Opportunity.is_excluded.is_(False),
                Opportunity.opportunity_owner_id.isnot(None),
            )
        )
        .distinct()
        .order_by(Opportunity.opportunity_owner_name)
    )

    return [
        {"rep_id": row.opportunity_owner_id, "rep_name": row.opportunity_owner_name or "Unassigned"}
        for row in result.all()
    ]
