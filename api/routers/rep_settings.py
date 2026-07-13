"""Rep compensation settings endpoints — drive the Sales Reps tab.

Browser-facing and unauthenticated, matching the dashboard router convention
(these serve static/dashboard.html directly, same as the Funnel Economics
period-input save endpoints).
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from db.queries.rep_comp import (
    DEFAULT_BASE_SALARY_MONTHLY,
    DEFAULT_COMMISSION_PCT,
    get_rep_comp_settings_map,
    upsert_rep_comp_setting,
)
from db.queries.reps import get_reps
from db.session import get_db

router = APIRouter(prefix="/api/dashboard", tags=["rep-settings"])


class RepCompSettingItem(BaseModel):
    rep_id: str
    rep_name: str
    category: str = "other"
    base_salary_monthly: float
    commission_pct: float
    is_default: bool  # True when no row is stored yet (showing fallback values)


class RepSettingsResponse(BaseModel):
    data: list[RepCompSettingItem]


class SaveRepCompSettingRequest(BaseModel):
    rep_id: str
    rep_name: str
    base_salary_monthly: float = Field(ge=0, le=1_000_000)
    commission_pct: float = Field(ge=0, le=100)


@router.get("/rep-settings", response_model=RepSettingsResponse)
async def rep_settings(db: AsyncSession = Depends(get_db)):
    """All reps (from synced opportunities) merged with their stored comp settings.

    Reps without a stored row get the defaults (base $0 / commission 10%) with
    is_default=True so the UI can show they haven't been reviewed yet.
    """
    reps = await get_reps(db)
    stored = await get_rep_comp_settings_map(db)

    items = []
    for rep in reps:
        row = stored.get(rep["rep_id"])
        items.append(RepCompSettingItem(
            rep_id=rep["rep_id"],
            rep_name=rep["rep_name"],
            category=rep["category"],
            base_salary_monthly=row["base_salary_monthly"] if row else DEFAULT_BASE_SALARY_MONTHLY,
            commission_pct=row["commission_pct"] if row else DEFAULT_COMMISSION_PCT,
            is_default=row is None,
        ))
    return RepSettingsResponse(data=items)


@router.put("/rep-settings", response_model=RepCompSettingItem)
async def save_rep_setting(body: SaveRepCompSettingRequest, db: AsyncSession = Depends(get_db)):
    """Upsert one rep's comp settings (base salary + commission %)."""
    if not body.rep_id.strip():
        raise HTTPException(status_code=422, detail="rep_id must not be empty")
    await upsert_rep_comp_setting(
        db,
        rep_id=body.rep_id,
        rep_name=body.rep_name,
        base_salary_monthly=round(body.base_salary_monthly, 2),
        commission_pct=round(body.commission_pct, 2),
    )
    return RepCompSettingItem(
        rep_id=body.rep_id,
        rep_name=body.rep_name,
        base_salary_monthly=round(body.base_salary_monthly, 2),
        commission_pct=round(body.commission_pct, 2),
        is_default=False,
    )
