"""Appointment resolver — matches Fireflies transcripts to GHL opportunities
and auto-updates call1_appointment_status (Showed / No Show) in GHL.

Logic:
  Fireflies transcript found + prospect spoke >= MIN_PROSPECT_SENTENCES → Showed
  Fireflies transcript found + prospect spoke 0 sentences             → No Show
  No Fireflies transcript found                                        → skipped (stays in noncompliance)

Runs daily at 7pm EST via APScheduler. Default lookback: 3 days.
First-deploy retroactive run: call resolve_appointments(lookback_days=30).
"""

import logging
from datetime import date, timedelta

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from db.models import Opportunity
from db.session import AsyncSessionLocal
from sync.fireflies_client import MIN_PROSPECT_SENTENCES, FirefliesClient
from sync.ghl_client import CUSTOM_FIELD_IDS, GHLClient

logger = logging.getLogger(__name__)

CALL1_STATUS_FIELD_ID = CUSTOM_FIELD_IDS["call1_appointment_status"]

# Only attempt name matching if the opportunity name has at least this many words.
# Single-name opps (e.g. "Ryan", "Kara") are too ambiguous to match reliably.
MIN_NAME_WORDS = 2


async def resolve_appointments(lookback_days: int = 3) -> dict:
    """Resolve unresolved 1st-call appointments using Fireflies transcripts.

    Returns a summary: {"showed": N, "no_show": N, "skipped": N, "errors": N}
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=lookback_days - 1)

    logger.info(
        "Appointment resolver: scanning %s → %s (%d days lookback)",
        start_date, end_date, lookback_days,
    )

    fireflies = FirefliesClient()
    ghl = GHLClient()
    summary: dict[str, int] = {"showed": 0, "no_show": 0, "skipped": 0, "errors": 0}

    async with AsyncSessionLocal() as session:
        opps = await _fetch_unresolved(session, start_date, end_date)
        logger.info("Appointment resolver: %d unresolved opps to check", len(opps))

        for opp in opps:
            try:
                outcome = await _resolve_one(opp, fireflies, ghl)
                summary[outcome] += 1
            except Exception as exc:
                logger.error(
                    "Appointment resolver: error on opp %s — %s",
                    opp.ghl_opportunity_id, exc,
                )
                summary["errors"] += 1

    logger.info("Appointment resolver: done — %s", summary)
    return summary


async def _fetch_unresolved(
    session: AsyncSession,
    start_date: date,
    end_date: date,
) -> list[Opportunity]:
    """Opps with a past 1st call that are still Confirmed (rep never updated them)."""
    result = await session.execute(
        select(Opportunity).where(
            and_(
                Opportunity.is_excluded.is_(False),
                Opportunity.call1_appointment_date.isnot(None),
                func.date(Opportunity.call1_appointment_date) >= start_date,
                func.date(Opportunity.call1_appointment_date) <= end_date,
                # Only touch Confirmed — never overwrite a rep-entered Showed/No Show/Cancelled
                Opportunity.call1_appointment_status == "Confirmed",
            )
        )
    )
    return list(result.scalars().all())


async def _resolve_one(
    opp: Opportunity,
    fireflies: FirefliesClient,
    ghl: GHLClient,
) -> str:
    """Attempt to resolve a single opp. Returns 'showed', 'no_show', or 'skipped'."""
    rep_email = settings.rep_email_map.get((opp.opportunity_owner_name or "").strip())
    if not rep_email:
        logger.debug(
            "Resolver: unknown rep %r — skipping %s",
            opp.opportunity_owner_name, opp.ghl_opportunity_id,
        )
        return "skipped"

    contact_name = (opp.opportunity_name or "").strip()
    if not _is_matchable(contact_name):
        logger.debug(
            "Resolver: name %r too ambiguous — skipping %s",
            contact_name, opp.ghl_opportunity_id,
        )
        return "skipped"

    appt_date = opp.call1_appointment_date.date()  # type: ignore[union-attr]
    transcripts = await fireflies.get_transcripts_for_date(appt_date, rep_email)

    matches = [
        t for t in transcripts
        if contact_name.lower() in (t.get("title") or "").lower()
    ]

    if not matches:
        # No Fireflies transcript — leave in noncompliance, don't flip
        logger.debug(
            "Resolver: no Fireflies transcript for %r on %s — leaving in noncompliance",
            contact_name, appt_date,
        )
        return "skipped"

    # If duplicate transcripts for the same call, pick the one with most audio
    best = max(matches, key=lambda t: t.get("duration") or 0)
    sentences = await fireflies.get_sentences(best["id"])

    prospect_count = _count_prospect_sentences(sentences, opp.opportunity_owner_name)

    new_status = "Showed" if prospect_count >= MIN_PROSPECT_SENTENCES else "No Show"

    await ghl.update_opportunity_custom_fields(
        opp.ghl_opportunity_id,
        [{"id": CALL1_STATUS_FIELD_ID, "field_value": new_status}],
    )

    logger.info(
        "Resolver: %s → %s (prospect sentences: %d, transcript: %s)",
        contact_name, new_status, prospect_count, best["id"],
    )
    return "showed" if new_status == "Showed" else "no_show"


def _count_prospect_sentences(sentences: list[dict], rep_name: str | None) -> int:
    """Count sentences from anyone who is not the rep (i.e. the prospect)."""
    if not sentences:
        return 0
    rep_lower = (rep_name or "").lower().strip()
    return sum(
        1 for s in sentences
        if (s.get("speaker_name") or "").lower().strip() != rep_lower
        and (s.get("text") or "").strip()
    )


def _is_matchable(name: str) -> bool:
    """True if the name is specific enough to safely match against Fireflies titles."""
    return bool(name) and len(name.split()) >= MIN_NAME_WORDS
