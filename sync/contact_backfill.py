"""Backfill GHL contact tags/email/phone onto existing opportunities + reclassify AI channels.

Two entry points:
  backfill_contact_data() — fetches the GHL contact (tags/email/phone) for sales opps that
      don't have tags yet, stores them, and reclassifies. The fast, idempotent alternative
      to a full opportunity re-sync when only the new contact fields are needed.
  reclassify_ai_from_stored_tags() — recompute canonical_channel from ALREADY-stored tags,
      no GHL calls. Use to re-apply classification-logic changes cheaply.

Both leave non-AI channels untouched, and move opps in/out of the AI channels per the
tag-first classifier. Scope: sales pipeline only.
"""

import asyncio
import logging

from sqlalchemy import select

from config import SALES_PIPELINE_ID
from db.models import Opportunity, SourceNormalization
from db.session import AsyncSessionLocal
from sync.ghl_client import GHLClient
from sync.normalizer import (
    APPOINTWISE_CHANNEL,
    RETELL_VERA_CHANNEL,
    classify_ai_channel,
    resolve_canonical_channel,
)

logger = logging.getLogger(__name__)

_AI_CHANNELS = {RETELL_VERA_CHANNEL, APPOINTWISE_CHANNEL}


async def _load_normalization_map(session) -> dict[str, str]:
    rows = await session.execute(select(SourceNormalization.raw_value, SourceNormalization.canonical_channel))
    return {r[0]: r[1] for r in rows.all()}


def _recompute_channel(opp: Opportunity, norm_map: dict[str, str]) -> str | None:
    """New canonical_channel for one opp, or None to leave unchanged.

    AI classification (tag-first) wins; if an opp was AI but no longer qualifies, fall back
    to the source-based channel; otherwise leave the existing (non-AI) value alone.
    """
    ai = classify_ai_channel(opp.contact_tags, opp.op_book_campaign_source)
    if ai:
        return ai
    if opp.canonical_channel in _AI_CHANNELS:
        # Was AI, no longer qualifies — re-resolve from source (raw_ghl_source not stored).
        return resolve_canonical_channel(
            norm_map, opp.attr_first_utm_source, opp.op_book_campaign_source, None
        )
    return None


async def reclassify_ai_from_stored_tags() -> dict:
    """Recompute canonical_channel from stored contact_tags (no GHL). Returns counts."""
    changed = 0
    async with AsyncSessionLocal() as session:
        norm_map = await _load_normalization_map(session)
        result = await session.execute(
            select(Opportunity).where(Opportunity.pipeline_id == SALES_PIPELINE_ID)
        )
        opps = result.scalars().all()
        for opp in opps:
            new_channel = _recompute_channel(opp, norm_map)
            if new_channel and new_channel != opp.canonical_channel:
                opp.canonical_channel = new_channel
                changed += 1
        await session.commit()
    logger.info("[reclassify-ai] recomputed channels — %d opps changed", changed)
    return {"scope": "sales_pipeline", "changed": changed}


async def backfill_contact_data(only_missing: bool = True, delay_s: float = 0.12) -> dict:
    """Fetch GHL contact tags/email/phone for sales opps + reclassify. GHL-heavy; run server-side.

    only_missing=True limits to opps that don't have contact_tags yet (cheap re-runs).
    """
    ghl = GHLClient()
    stats = {"checked": 0, "updated": 0, "reclassified": 0, "errors": 0}

    async with AsyncSessionLocal() as session:
        norm_map = await _load_normalization_map(session)
        q = select(Opportunity).where(
            Opportunity.pipeline_id == SALES_PIPELINE_ID,
            Opportunity.ghl_contact_id.isnot(None),
        )
        if only_missing:
            q = q.where(Opportunity.contact_tags.is_(None))
        opps = (await session.execute(q)).scalars().all()
        logger.info("[contact-backfill] start — %d opps to fetch", len(opps))

        # De-dupe by contact so we fetch each contact once.
        contact_cache: dict[str, dict | None] = {}
        for opp in opps:
            cid = opp.ghl_contact_id
            try:
                if cid not in contact_cache:
                    contact_cache[cid] = await ghl.get_contact(cid)
                    await asyncio.sleep(delay_s)
                contact = contact_cache[cid]
                stats["checked"] += 1
                if contact:
                    raw_tags = contact.get("tags")
                    opp.contact_tags = [str(t) for t in raw_tags if t] if isinstance(raw_tags, list) else None
                    opp.contact_email = (contact.get("email") or "").strip().lower() or None
                    opp.contact_phone = (contact.get("phone") or "").strip() or None
                    stats["updated"] += 1
                new_channel = _recompute_channel(opp, norm_map)
                if new_channel and new_channel != opp.canonical_channel:
                    opp.canonical_channel = new_channel
                    stats["reclassified"] += 1
                if stats["checked"] % 200 == 0:
                    await session.commit()
            except Exception as exc:
                logger.error("[contact-backfill] error on opp %s: %s", opp.ghl_opportunity_id, exc)
                stats["errors"] += 1
        await session.commit()

    logger.info("[contact-backfill] done — %s", stats)
    return stats
