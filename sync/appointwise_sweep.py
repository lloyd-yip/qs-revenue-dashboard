"""Appointwise SMS sweep — fetch the FULL messaged audience (not just booked leads).

The main sync is opportunity-driven, so it only captures SMS for Appointwise contacts who
booked. This sweep reaches every Appointwise-tagged contact (via GHL contact-search by tag),
pulls their SMS from GHL Conversations, and upserts contact_sms_stats — so the funnel
(messaged → replied → booked) reflects the whole audience. Runs weekly / on demand.
"""

import asyncio
import logging

from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.models import ContactSmsStats
from db.session import AsyncSessionLocal
from sync.ghl_client import GHLClient
from sync.sms_stats import APPOINTWISE_TAGS, compute_sms_stats

logger = logging.getLogger(__name__)


async def run_appointwise_sms_sweep(delay_s: float = 0.12) -> dict:
    """Fetch SMS for all Appointwise-tagged contacts + upsert contact_sms_stats."""
    ghl = GHLClient()
    stats = {"contacts": 0, "with_sms": 0, "errors": 0}
    seen: set[str] = set()

    async with AsyncSessionLocal() as session:
        for tag in sorted(APPOINTWISE_TAGS):
            contacts = await ghl.get_contacts_by_tag(tag)
            for contact in contacts:
                cid = contact.get("id")
                if not cid or cid in seen:
                    continue
                seen.add(cid)
                stats["contacts"] += 1
                try:
                    messages = await ghl.get_contact_messages(cid)
                    sms = compute_sms_stats(cid, messages)
                    if sms:
                        stmt = (
                            pg_insert(ContactSmsStats)
                            .values(**sms)
                            .on_conflict_do_update(
                                index_elements=["ghl_contact_id"],
                                set_={k: v for k, v in sms.items() if k != "ghl_contact_id"},
                            )
                        )
                        await session.execute(stmt)
                        stats["with_sms"] += 1
                    if stats["contacts"] % 100 == 0:
                        await session.commit()
                        logger.info("[appointwise-sweep] %s", stats)
                    await asyncio.sleep(delay_s)
                except Exception as exc:
                    logger.warning("[appointwise-sweep] error on contact %s: %s", cid, exc)
                    stats["errors"] += 1
        await session.commit()

    logger.info("[appointwise-sweep] done — %s", stats)
    return stats
