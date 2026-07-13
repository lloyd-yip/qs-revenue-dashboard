"""Rep compensation settings — per-rep base salary + commission % config.

These settings drive the DERIVED rep cost used by the Sales dashboard
(see db/queries/metrics_by_rep.py): base salary is prorated over the selected
window and commission accrues as commission_pct × cash collected on the rep's
cohort deals. Reps without a row fall back to the defaults below.
"""

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import RepCompSetting

DEFAULT_BASE_SALARY_MONTHLY = 0.0
DEFAULT_COMMISSION_PCT = 10.0


async def get_rep_comp_settings_map(session: AsyncSession) -> dict[str, dict]:
    """Return {rep_id: {rep_name, base_salary_monthly, commission_pct}} for all stored rows."""
    result = await session.execute(select(RepCompSetting))
    return {
        row.rep_id: {
            "rep_name": row.rep_name,
            "base_salary_monthly": float(row.base_salary_monthly),
            "commission_pct": float(row.commission_pct),
        }
        for row in result.scalars().all()
    }


async def upsert_rep_comp_setting(
    session: AsyncSession,
    rep_id: str,
    rep_name: str,
    base_salary_monthly: float,
    commission_pct: float,
) -> None:
    """Insert or update the comp settings row for one rep."""
    stmt = pg_insert(RepCompSetting).values(
        rep_id=rep_id,
        rep_name=rep_name,
        base_salary_monthly=base_salary_monthly,
        commission_pct=commission_pct,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[RepCompSetting.rep_id],
        set_={
            "rep_name": stmt.excluded.rep_name,
            "base_salary_monthly": stmt.excluded.base_salary_monthly,
            "commission_pct": stmt.excluded.commission_pct,
            "updated_at": func.now(),
        },
    )
    await session.execute(stmt)
    await session.commit()
