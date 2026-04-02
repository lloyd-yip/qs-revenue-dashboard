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

from sqlalchemy import and_, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity
from db.session import AsyncSessionLocal
from sync.fireflies_client import MIN_PROSPECT_SENTENCES, FirefliesClient
from sync.ghl_client import GHLClient
from sync.sync_engine import _find_appointment_for_date

logger = logging.getLogger(__name__)

# Only attempt name matching if the opportunity name has at least this many words.
# Single-name opps (e.g. "Ryan", "Kara") are too ambiguous to match reliably.
# Map our internal status values to GHL calendar appointment status values
_GHL_STATUS_MAP = {
    "Showed": "showed",
    "No Show": "noshow",
}


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

    # Fetch once per run — covers all reps automatically, no manual config needed
    user_email_map = await ghl.get_user_email_map()
    logger.info("Appointment resolver: loaded %d GHL user emails", len(user_email_map))

    async with AsyncSessionLocal() as session:
        opps = await _fetch_unresolved(session, start_date, end_date)
        logger.info("Appointment resolver: %d unresolved opps to check", len(opps))

        for opp in opps:
            try:
                outcome = await _resolve_one(opp, fireflies, ghl, session, user_email_map)
                summary[outcome] += 1
            except Exception as exc:
                logger.error(
                    "Appointment resolver: error on opp %s — %s",
                    opp.ghl_opportunity_id, exc,
                )
                summary["errors"] += 1

        await session.commit()

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
                # Only touch Confirmed or NULL — never overwrite a rep-entered Showed/No Show/Cancelled
                # NULL occurs on records synced before the calendar-appointment read was added
                or_(
                    Opportunity.call1_appointment_status == "Confirmed",
                    Opportunity.call1_appointment_status.is_(None),
                ),
            )
        )
    )
    return list(result.scalars().all())


async def _resolve_one(
    opp: Opportunity,
    fireflies: FirefliesClient,
    ghl: GHLClient,
    session: AsyncSession,
    user_email_map: dict[str, str],
) -> str:
    """Attempt to resolve a single opp. Returns 'showed', 'no_show', or 'skipped'."""
    rep_email = user_email_map.get(opp.opportunity_owner_id or "")
    if not rep_email:
        logger.info("Resolver: skip %s — owner_id %r not in GHL user email map", opp.ghl_opportunity_id, opp.opportunity_owner_id)
        return "skipped"

    contact_id = opp.ghl_contact_id
    if not contact_id:
        logger.info("Resolver: skip %s — no contact_id", opp.ghl_opportunity_id)
        return "skipped"

    # Fetch the GHL calendar appointment — it has the real call date AND the
    # full contact name in its title (same title Fireflies uses). This avoids
    # relying on opp.opportunity_name which GHL often fills with just a first name.
    appointments = await ghl.get_contact_appointments(contact_id)
    cal_appt = _find_appointment_for_date(
        appointments, opp.call1_appointment_date  # type: ignore[arg-type]
    )
    if not cal_appt:
        logger.info(
            "Resolver: skip %s (%r) — no calendar appt on %s (found %d total appts)",
            opp.ghl_opportunity_id, opp.opportunity_name, opp.call1_appointment_date, len(appointments),
        )
        return "skipped"

    appt_title = (cal_appt.get("title") or "").strip()
    appt_date = opp.call1_appointment_date.date()  # type: ignore[union-attr]

    transcripts = await fireflies.get_transcripts_for_date(appt_date, rep_email)

    # Match Fireflies transcript by title — Fireflies copies the GHL calendar title
    matches = [
        t for t in transcripts
        if _titles_match(appt_title, t.get("title") or "")
    ]

    if not matches:
        logger.info(
            "Resolver: skip %r on %s — no Fireflies title match (appt title: %r, found %d transcripts)",
            opp.opportunity_name, appt_date, appt_title, len(transcripts),
        )
        return "skipped"

    # If multiple matches, pick longest (most audio = real call)
    best = max(matches, key=lambda t: t.get("duration") or 0)
    sentences = await fireflies.get_sentences(best["id"])

    prospect_count = _count_prospect_sentences(sentences, opp.opportunity_owner_name)
    new_status = "Showed" if prospect_count >= MIN_PROSPECT_SENTENCES else "No Show"
    ghl_status = _GHL_STATUS_MAP[new_status]

    # Update the GHL calendar appointment status (what reps see)
    appointment_id = cal_appt.get("id")
    if appointment_id:
        await ghl.update_appointment_status(appointment_id, ghl_status)
    else:
        logger.warning("Resolver: appointment record has no id for opp %s", opp.ghl_opportunity_id)

    # Update DB directly — don't wait for next sync cycle
    await session.execute(
        text("UPDATE opportunities SET call1_appointment_status = :status WHERE ghl_opportunity_id = :id"),
        {"status": new_status, "id": opp.ghl_opportunity_id},
    )

    logger.info(
        "Resolver: %r → %s (prospect sentences: %d, transcript: %s, appt: %s)",
        appt_title, new_status, prospect_count, best["id"], appointment_id or "missing",
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


def _titles_match(appt_title: str, ff_title: str) -> bool:
    """True if the GHL calendar appointment title matches a Fireflies transcript title.

    Fireflies copies the GHL calendar event title verbatim, so this is usually
    an exact or near-exact match. We check both directions to handle minor
    truncation differences.
    """
    a = appt_title.lower().strip()
    f = ff_title.lower().strip()
    return bool(a) and bool(f) and (a == f or a in f or f in a)
