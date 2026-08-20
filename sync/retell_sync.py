"""Retell call sync — pull voice calls from Retell, match to GHL contacts by phone, upsert.

Matching uses opportunities.contact_phone (populated from the GHL contact at opportunity
sync time). Match is by normalized last-10-digits, so +1/country-code differences don't
break it. Re-runnable: upsert on retell_call_id, match fields refreshed each run (so calls
match as soon as contact_phone is backfilled).
"""

import logging
from datetime import datetime, timezone

from sqlalchemy import and_, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from api.utils.retell_utils import get_retell_config
from db.models import Opportunity, RetellCall
from db.session import AsyncSessionLocal
from sync.retell_client import RetellClient

logger = logging.getLogger(__name__)


def normalize_phone(phone: str | None) -> str | None:
    """Return the last 10 digits of a phone number (US-friendly), or None."""
    if not phone:
        return None
    digits = "".join(ch for ch in str(phone) if ch.isdigit())
    if len(digits) < 10:
        return digits or None
    return digits[-10:]


def _ms_to_dt(ms: int | None) -> datetime | None:
    if not ms:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    except (ValueError, OSError, TypeError):
        return None


async def _build_phone_index(session) -> dict[str, tuple[str, str]]:
    """Map normalized phone → (ghl_contact_id, contact_name) from stored opportunities."""
    result = await session.execute(
        select(
            Opportunity.contact_phone,
            Opportunity.ghl_contact_id,
            Opportunity.opportunity_name,
        ).where(Opportunity.contact_phone.isnot(None))
    )
    index: dict[str, tuple[str, str]] = {}
    for phone, contact_id, name in result.all():
        norm = normalize_phone(phone)
        if norm and contact_id and norm not in index:
            index[norm] = (contact_id, name or "")
    return index


def _row_from_call(call: dict, phone_index: dict[str, tuple[str, str]]) -> dict:
    """Build a retell_calls upsert row from a raw Retell call dict + phone match."""
    to_num = call.get("to_number")
    from_num = call.get("from_number")
    # The customer number is whichever side isn't the agent — try both against the index.
    match = None
    for candidate in (to_num, from_num):
        norm = normalize_phone(candidate)
        if norm and norm in phone_index:
            match = phone_index[norm]
            break

    analysis = call.get("call_analysis") or None
    duration_ms = call.get("duration_ms")
    # Retell per-call cost. combined_cost is a FLOAT denominated in cents (0.55, 4.1833333),
    # so it must not be int()-ed — that floored every sub-cent call to zero. Stored in USD.
    cost = call.get("call_cost") or {}
    combined = cost.get("combined_cost") if isinstance(cost, dict) else None
    cost_usd = round(combined / 100.0, 6) if isinstance(combined, (int, float)) else None
    return {
        "retell_call_id": call.get("call_id"),
        "agent_id": call.get("agent_id"),
        "direction": call.get("direction"),
        "from_number": from_num,
        "to_number": to_num,
        "started_at": _ms_to_dt(call.get("start_timestamp")),
        "duration_sec": int(duration_ms / 1000) if duration_ms else None,
        "call_status": call.get("call_status"),
        "disconnect_reason": call.get("disconnection_reason"),
        "cost_usd": cost_usd,
        "recording_url": call.get("recording_url"),
        "transcript": call.get("transcript"),
        "analysis": analysis,
        "ghl_contact_id": match[0] if match else None,
        "ghl_contact_name": match[1] if match else None,
        "match_method": "phone_exact" if match else "unmatched",
        "match_confidence": "HIGH" if match else None,
    }


async def run_retell_sync(
    max_calls: int | None = None,
    after_ms: int | None = None,
    incremental: bool = False,
) -> dict:
    """Pull Retell calls, match to GHL contacts, upsert. Returns a summary dict.

    incremental=True (used by the 6-hourly schedule) pulls only calls since the most
    recent stored call (minus a 1-day overlap buffer for late-arriving data), so routine
    runs are cheap. A manual/full run (incremental=False) re-pulls everything.
    No-op (returns 'skipped') when no API key is configured.
    """
    cfg = await get_retell_config()
    if not cfg.api_key:
        logger.info("Retell sync skipped — no API key configured")
        return {"status": "skipped", "reason": "no_api_key", "calls": 0, "matched": 0}

    client = RetellClient(cfg.api_key)
    processed = 0
    matched = 0

    async with AsyncSessionLocal() as session:
        if incremental and after_ms is None:
            latest = (await session.execute(select(func.max(RetellCall.started_at)))).scalar()
            if latest:
                after_ms = int((latest.timestamp() - 86400) * 1000)  # 1-day overlap buffer
        phone_index = await _build_phone_index(session)

        batch: list[dict] = []
        async for call in client.list_calls(max_calls=max_calls, after_ms=after_ms):
            if not call.get("call_id"):
                continue
            row = _row_from_call(call, phone_index)
            if row["ghl_contact_id"]:
                matched += 1
            batch.append(row)
            processed += 1
            if len(batch) >= 200:
                await _upsert_calls(session, batch)
                batch = []
        if batch:
            await _upsert_calls(session, batch)

    logger.info("Retell sync: %d calls processed, %d matched to GHL contacts", processed, matched)
    return {"status": "completed", "calls": processed, "matched": matched}


async def match_unmatched_by_ghl_lookup(delay_s: float = 0.12, max_lookups: int | None = None) -> dict:
    """Link still-unmatched Retell calls to their GHL contact via phone lookup.

    For Retell numbers that have no opportunity (so weren't in the phone index), look the
    contact up directly in GHL (~1 call per distinct number) and stamp contact id + name.
    Small quota (≈ number of distinct unmatched numbers). Idempotent.
    """
    from sync.ghl_client import GHLClient
    ghl = GHLClient()
    stats = {"looked_up": 0, "matched": 0, "not_found": 0}

    async with AsyncSessionLocal() as session:
        numbers = (await session.execute(
            select(RetellCall.to_number)
            .where(and_(RetellCall.ghl_contact_id.is_(None), RetellCall.to_number.isnot(None)))
            .distinct()
        )).scalars().all()
        logger.info("Retell contact lookup: %d distinct unmatched numbers", len(numbers))

        for i, num in enumerate(numbers):
            if max_lookups and i >= max_lookups:
                break
            contact = await ghl.get_contact_by_phone(num)
            stats["looked_up"] += 1
            if contact and contact.get("id"):
                name = (contact.get("contactName")
                        or f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip()
                        or None)
                await session.execute(
                    update(RetellCall)
                    .where(RetellCall.to_number == num, RetellCall.ghl_contact_id.is_(None))
                    .values(ghl_contact_id=contact["id"], ghl_contact_name=name,
                            match_method="phone_lookup", match_confidence="HIGH")
                )
                stats["matched"] += 1
            else:
                stats["not_found"] += 1
            if stats["looked_up"] % 100 == 0:
                await session.commit()
                logger.info("Retell contact lookup: %s", stats)
        await session.commit()

    logger.info("Retell contact lookup done: %s", stats)
    return stats


async def _upsert_calls(session, rows: list[dict]) -> None:
    for row in rows:
        stmt = (
            pg_insert(RetellCall)
            .values(**row)
            .on_conflict_do_update(
                index_elements=["retell_call_id"],
                # updated_at must be set explicitly: the model's onupdate=func.now() is an
                # ORM-level hook and does not fire for a Core on_conflict_do_update, so
                # without this the column would freeze at first-insert time and stop being
                # a usable "last synced" signal.
                set_={
                    **{k: v for k, v in row.items() if k != "retell_call_id"},
                    "updated_at": func.now(),
                },
            )
        )
        await session.execute(stmt)
    await session.commit()
