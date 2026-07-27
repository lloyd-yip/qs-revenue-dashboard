"""Inbound webhooks from external systems (currently Appointwise agents).

Appointwise has no read-API, so its agents push events here via a Webhook Node — the only
source of which-agent-handled-which-contact. Secret-protected (shared secret in a header or
?token=), NOT bearer-auth (the external caller can't send our token). Tolerant payload.
"""

import logging

from fastapi import APIRouter, Header, HTTPException, Query, Request
from sqlalchemy.dialects.postgresql import insert as pg_insert

from api.utils.appointwise_utils import APPOINTWISE_SETTING_WEBHOOK_SECRET
from db.models import AppointwiseEvent
from db.queries.settings import get_setting
from db.session import AsyncSessionLocal

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/webhooks", tags=["webhooks"])


def _first(payload: dict, *keys):
    for k in keys:
        v = payload.get(k)
        if v not in (None, ""):
            return v
    return None


@router.post("/appointwise")
async def appointwise_webhook(
    request: Request,
    token: str | None = Query(None),
    x_appointwise_secret: str | None = Header(None),
) -> dict:
    """Ingest one Appointwise agent event. Verify the shared secret, then store it.

    Accepts a flexible JSON body — we read agent/contact identifiers from common key names.
    """
    async with AsyncSessionLocal() as session:
        expected = await get_setting(session, APPOINTWISE_SETTING_WEBHOOK_SECRET)
    supplied = x_appointwise_secret or token
    if not expected or supplied != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing webhook secret.")

    try:
        payload = await request.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {"value": payload}

    row = {
        "event_type": _first(payload, "event", "event_type", "type", "outcome"),
        "agent_name": _first(payload, "agent_name", "agent", "agentName"),
        "agent_id": _first(payload, "agent_id", "agentId"),
        "ghl_contact_id": _first(payload, "contact_id", "ghl_contact_id", "contactId", "ghlContactId"),
        "phone": _first(payload, "phone", "contact_phone", "phoneNumber"),
        "email": _first(payload, "email", "contact_email"),
        "raw": payload,
    }
    async with AsyncSessionLocal() as session:
        await session.execute(pg_insert(AppointwiseEvent).values(**row))
        await session.commit()
    return {"ok": True}
