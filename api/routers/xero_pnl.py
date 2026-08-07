"""Xero full-P&L sync endpoints — populate xero_pnl_lines (company-wide P&L).

Bearer-protected like the other /xero/sync-* endpoints. The monthly cron (5th of each
month) calls sync_xero_pnl for the previous, now-reconciled month via the stored refresh
token; these endpoints let you backfill history or re-pull a month on demand.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query

from api.utils.xero_utils import verify_bearer
from sync.xero_pnl_sync import backfill_xero_pnl, sync_xero_pnl

logger = logging.getLogger(__name__)
router = APIRouter(tags=["xero"])


@router.post("/xero/sync-pnl", dependencies=[Depends(verify_bearer)])
async def xero_sync_pnl(
    month: str = Query(..., pattern=r"^\d{4}-\d{2}$", description="Month YYYY-MM"),
    xero_token: str | None = Query(None, description="Optional 30-min token; else uses the stored refresh token"),
) -> dict:
    """Pull one month's full Xero P&L into xero_pnl_lines. Net should match Xero exactly."""
    try:
        return await sync_xero_pnl(month, xero_token=xero_token)
    except Exception as exc:
        logger.error("Xero P&L sync failed for %s: %s", month, exc, exc_info=True)
        raise HTTPException(status_code=502, detail=f"Xero P&L sync failed: {exc}")


@router.post("/xero/sync-pnl-backfill", dependencies=[Depends(verify_bearer)])
async def xero_sync_pnl_backfill(
    months: int = Query(12, ge=1, le=36, description="Trailing complete months to backfill"),
) -> dict:
    """Backfill the trailing N complete months of the full Xero P&L (skips the current,
    in-progress month). One bad month is reported but does not abort the rest."""
    return await backfill_xero_pnl(months)
