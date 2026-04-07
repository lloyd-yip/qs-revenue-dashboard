"""Funnel Economics — period marketing spend and rep compensation storage + retrieval.

These two tables hold manually-entered financial data that powers RORI calculations
in the Cost Efficiency by Rep table. Data is entered via the Funnel Economics tab
inputs panel after each accounting period.
"""

from datetime import date

from sqlalchemy import and_, delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.models import PeriodMarketingSpend, RepCompensation


async def get_period_inputs(
    session: AsyncSession,
    start: date,
    end: date,
) -> dict | None:
    """Return saved marketing spend + rep comps for the exact period.

    Returns None if no data has been saved for this period yet.
    Returns a dict with keys: marketing_spend (float|None), rep_comps (list[dict]).
    """
    spend_row = await session.scalar(
        select(PeriodMarketingSpend).where(
            and_(
                PeriodMarketingSpend.period_start == start,
                PeriodMarketingSpend.period_end == end,
            )
        )
    )

    comp_rows = (await session.execute(
        select(RepCompensation).where(
            and_(
                RepCompensation.period_start == start,
                RepCompensation.period_end == end,
            )
        ).order_by(RepCompensation.rep_name)
    )).scalars().all()

    if spend_row is None and not comp_rows:
        return None

    return {
        "marketing_spend": float(spend_row.amount) if spend_row else None,
        "rep_comps": [
            {
                "rep_id": r.rep_id,
                "rep_name": r.rep_name,
                "total_comp": float(r.total_comp),
            }
            for r in comp_rows
        ],
    }


async def upsert_marketing_spend(
    session: AsyncSession,
    start: date,
    end: date,
    amount: float,
) -> None:
    """Save or overwrite total marketing spend for the exact period.

    Upsert = insert if new period, update if it already exists.
    """
    stmt = pg_insert(PeriodMarketingSpend).values(
        period_start=start,
        period_end=end,
        amount=amount,
    ).on_conflict_do_update(
        index_elements=["period_start", "period_end"],
        set_={"amount": amount, "updated_at": "now()"},
    )
    await session.execute(stmt)
    await session.commit()


async def upsert_rep_compensations(
    session: AsyncSession,
    start: date,
    end: date,
    reps: list[dict],
) -> None:
    """Save or overwrite comp for a list of reps for the exact period.

    Each dict in reps must have: rep_id, rep_name, total_comp.
    Existing reps not in the list are left unchanged.
    """
    for rep in reps:
        stmt = pg_insert(RepCompensation).values(
            rep_id=rep["rep_id"],
            rep_name=rep["rep_name"],
            period_start=start,
            period_end=end,
            total_comp=rep["total_comp"],
        ).on_conflict_do_update(
            index_elements=["rep_id", "period_start", "period_end"],
            set_={"total_comp": rep["total_comp"], "rep_name": rep["rep_name"], "updated_at": "now()"},
        )
        await session.execute(stmt)
    await session.commit()
