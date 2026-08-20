"""AI-channel (Retell voice / Appointwise SMS) dashboard endpoints.

Extracted from dashboard.py, which was past 1,200 lines. Browser-facing and unauthenticated,
matching the sibling dashboard/whop_live/company routers the same pages already call.
"""

import logging

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from api.utils.dashboard_params import date_params, meta
from db.queries.ai_channels import (
    get_ai_channel_stats,
    get_ai_data_quality,
    get_appointwise_agent_breakdown,
    get_appointwise_sms_stats,
    get_retell_agent_breakdown,
    get_retell_calls,
    get_vera_chat_contacts,
)
from db.session import get_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dashboard", tags=["ai-channels"])


@router.get("/channel-detail/ai")
async def channel_detail_ai(
    channel: str = Query(...),
    params: tuple = Depends(date_params),
    db: AsyncSession = Depends(get_db),
):
    """AI-channel extras (booked/held/confidence + Retell call volume) — Retell/Appointwise only."""
    start, end, date_by = params
    data = await get_ai_channel_stats(db, channel, start, end, date_by)
    return {"data": data, "meta": meta(start, end, date_by)}


@router.get("/retell/calls")
async def retell_calls(
    filter: str = Query("all", description="all | dq"),
    connection: str = Query("all", description="all | picked_up | no_answer"),
    min_minutes: float = Query(0.0, ge=0, description="Only calls at least this many minutes"),
    params: tuple = Depends(date_params),
    db: AsyncSession = Depends(get_db),
):
    """Retell voice-call reconciliation list. Filter by disqualified, connection, and length."""
    start, end, date_by = params
    conn = connection if connection in ("picked_up", "no_answer") else "all"
    data = await get_retell_calls(
        db, start, end, "dq" if filter == "dq" else "all", conn, min_minutes
    )
    return {"data": data, "meta": meta(start, end, date_by)}


@router.post("/retell/refresh")
async def retell_refresh(background_tasks: BackgroundTasks):
    """Pull new Retell calls now — powers the 'Sync now' button on the Retell channel page.

    Incremental (calls since the newest stored one), so it returns in seconds. Runs in the
    background; the page polls last_synced_at to know when it landed. Browser-facing/no-auth
    like the sibling /pnl/whop-refresh and /company/stripe-refresh buttons; the authenticated
    /api/sync/retell remains for admin/full re-pulls.
    """
    background_tasks.add_task(_run_retell_refresh)
    return {"status": "triggered", "mode": "incremental"}


async def _run_retell_refresh() -> None:
    from sync.retell_sync import run_retell_sync
    try:
        summary = await run_retell_sync(incremental=True)
        logger.info("Retell manual refresh complete: %s", summary)
    except Exception as exc:
        logger.error("Retell manual refresh failed: %s", exc)


@router.get("/retell/recording/{call_id}")
async def retell_recording(call_id: str, db: AsyncSession = Depends(get_db)):
    """Stream a Retell call recording through the server (dodges CORS + URL expiry)."""
    import httpx
    from fastapi.responses import StreamingResponse
    from sqlalchemy import select as _select
    from db.models import RetellCall

    row = (await db.execute(
        _select(RetellCall.recording_url).where(RetellCall.retell_call_id == call_id)
    )).one_or_none()
    url = row[0] if row else None
    if not url:
        # Try to refresh the URL from Retell.
        from api.utils.retell_utils import get_retell_config
        from sync.retell_client import RetellClient
        cfg = await get_retell_config()
        if cfg.api_key:
            call = await RetellClient(cfg.api_key).get_call(call_id)
            url = (call or {}).get("recording_url")
    if not url:
        raise HTTPException(status_code=404, detail="No recording for this call.")

    async def _stream():
        async with httpx.AsyncClient(timeout=60.0) as client:
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                async for chunk in resp.aiter_bytes():
                    yield chunk

    return StreamingResponse(_stream(), media_type="audio/mpeg")


@router.get("/retell/agents")
async def retell_agents(
    params: tuple = Depends(date_params),
    db: AsyncSession = Depends(get_db),
):
    """Per-Retell-agent call performance (names resolved via the Retell API when connected)."""
    start, end, date_by = params
    data = await get_retell_agent_breakdown(db, start, end)
    if data:
        from api.utils.retell_utils import get_retell_config
        from sync.retell_client import RetellClient
        cfg = await get_retell_config()
        names = await RetellClient(cfg.api_key).list_agents() if cfg.api_key else {}
        for row in data:
            row["agent_name"] = names.get(row["agent_id"])
    return {"data": data, "meta": meta(start, end, date_by)}


@router.get("/appointwise/sms")
async def appointwise_sms(db: AsyncSession = Depends(get_db)):
    """Appointwise SMS engagement (from GHL Conversations)."""
    return {"data": await get_appointwise_sms_stats(db)}


@router.get("/appointwise/agents")
async def appointwise_agents(db: AsyncSession = Depends(get_db)):
    """Per-Appointwise-agent performance (from webhook events). Empty until webhooks flow."""
    return {"data": await get_appointwise_agent_breakdown(db)}


@router.get("/ai/data-quality")
async def ai_data_quality(db: AsyncSession = Depends(get_db)):
    """AI data-quality tiles (leaks, ownerless wons, tag gaps, source='call', freshness)."""
    return {"data": await get_ai_data_quality(db)}


@router.get("/ai/vera-chat-contacts")
async def ai_vera_chat_contacts(
    limit: int = Query(5, ge=1, le=20),
    db: AsyncSession = Depends(get_db),
):
    """Vera-chat opps (folded into Appointwise) — for fact-checking the classification."""
    return {"data": await get_vera_chat_contacts(db, limit)}
