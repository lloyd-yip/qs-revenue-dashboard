"""Channel normalization and compliance flag logic.

Resolves canonical_channel from UTM attribution data using the source_normalization
table, and computes the rep_compliance_failure flag.
"""

import logging
from datetime import datetime, timedelta, timezone

from sync.ghl_client import (
    EXCLUDED_STAGE_IDS,
    NO_SHOW_STAGE_ID,
    CANCELLED_STAGE_ID,
    TEMP_RYAN_STAGE_PREFIX,
    UPCOMING_1ST_CALL_BOOKED_STAGE_ID,
)

logger = logging.getLogger(__name__)

# Canonical channels — used as fallback strings when no normalization match found
CHANNEL_UNKNOWN = "Unknown"

# Stage names that contain showed-state signals (for stages without confirmed IDs)
SHOWED_STAGE_NAME_FRAGMENTS = {
    "warm list",
    "hot list",
    "long term nurture",
}

def is_excluded_stage(stage_id: str | None, stage_name: str | None) -> bool:
    """Return True if the opportunity should be excluded from all metrics."""
    if stage_id in EXCLUDED_STAGE_IDS:
        return True
    if stage_name and stage_name.startswith(TEMP_RYAN_STAGE_PREFIX):
        return True
    return False


def resolve_canonical_channel(
    normalization_map: dict[str, str],
    attr_first_utm_source: str | None,
    op_book_campaign_source: str | None,
    raw_ghl_source: str | None,
) -> str:
    """Resolve the canonical channel using a three-tier fallback.

    Priority:
    1. attr_first_utm_source (GHL native first-touch UTM — most reliable)
    2. op_book_campaign_source (captured at booking time)
    3. raw_ghl_source (messy, normalize what we can)
    4. 'Unknown' if nothing matches

    normalization_map: {raw_value: canonical_channel} loaded from source_normalization table.
    """
    candidates = [attr_first_utm_source, op_book_campaign_source, raw_ghl_source]

    for raw in candidates:
        if not raw:
            continue
        # Exact match
        channel = normalization_map.get(raw)
        if channel:
            return channel
        # Case-insensitive fallback
        raw_lower = raw.lower()
        for key, val in normalization_map.items():
            if key.lower() == raw_lower:
                return val

    return CHANNEL_UNKNOWN


def compute_compliance_failure(
    pipeline_stage_id: str | None,
    call1_appointment_date: datetime | None,
    call1_appointment_status: str | None,
) -> bool:
    """Flag a rep compliance failure.

    Conditions:
    - Stage is "Upcoming 1st Call Booked"
    - Call 1 appointment date has passed end of day (UTC)
    - Call 1 appointment status is still Confirmed (or not set)
    """
    if pipeline_stage_id != UPCOMING_1ST_CALL_BOOKED_STAGE_ID:
        return False
    if call1_appointment_date is None:
        return False

    now_utc = datetime.now(timezone.utc)
    # End of the appointment day
    appt_eod = call1_appointment_date.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
    if now_utc <= appt_eod:
        return False

    # Both signals must still be stale (Confirmed or unset)
    stale_statuses = {None, "Confirmed", "confirmed"}
    return call1_appointment_status in stale_statuses


def compute_outcome_unfilled(
    call1_appointment_date: datetime | None,
    call1_appointment_status: str | None,
) -> bool:
    """Flag outcome_unfilled: appointment passed + 12h grace, status never updated.

    Broader than rep_compliance_failure — no stage restriction.
    Catches any opp where the rep failed to log Showed / No Show / Cancelled
    regardless of which pipeline stage the opp is currently in.

    12-hour grace period: reps are expected to update by 2 AM the following day.
    """
    if call1_appointment_date is None:
        return False

    now_utc = datetime.now(timezone.utc)
    grace_deadline = call1_appointment_date + timedelta(hours=12)
    if now_utc <= grace_deadline:
        return False  # Still within grace period

    stale_statuses = {None, "Confirmed", "confirmed"}
    return call1_appointment_status in stale_statuses


def compute_post_call_note_word_count(
    notes: list[dict],
    owner_id: str | None,
    call1_appointment_date: datetime | None,
) -> int | None:
    """Compute word count of rep's best qualifying post-call note.

    Returns:
        None  — notes check not applicable (no appointment date, or future appt)
        0     — showed, no qualifying rep note found within 12h window
        N     — word count of the longest qualifying rep note

    A qualifying note must:
    - Be created by the rep (userId == owner_id)
    - Have a dateAdded between call1_appointment_date and call1_appointment_date + 72h

    72h window (not 12h) because appointment dates are stored as date-only (midnight UTC),
    meaning the window would effectively end at noon UTC the same day — far too narrow.
    72h gives reps up to ~3 days and still clearly associates the note with the appointment.
    """
    if call1_appointment_date is None or owner_id is None:
        return None

    assert call1_appointment_date is not None  # narrowed above
    appt_date: datetime = call1_appointment_date
    grace_deadline: datetime = appt_date + timedelta(hours=72)

    qualifying: list[int] = []
    for note in notes:
        user_id = note.get("userId", "")
        if user_id != owner_id:
            continue  # Exclude automation / setter notes
        date_added_raw = note.get("dateAdded")
        if not date_added_raw:
            continue
        try:
            date_added: datetime = datetime.fromisoformat(date_added_raw.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue
        if appt_date <= date_added <= grace_deadline:
            body_text = note.get("bodyText") or ""
            qualifying.append(len(body_text.split()))

    return max(qualifying) if qualifying else 0


def parse_ghl_datetime(value: str | None) -> datetime | None:
    """Parse a GHL datetime string to a timezone-aware datetime.

    GHL returns ISO 8601 strings, sometimes with 'Z', sometimes with offset.
    """
    if not value:
        return None
    try:
        # Handle 'Z' suffix
        value = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        logger.warning("Could not parse GHL datetime: %r", value)
        return None
