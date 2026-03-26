"""Critical-path tests for QS Dashboard.

Tests the functions most likely to silently corrupt data:
1. Compliance flag logic (compute_compliance_failure, compute_outcome_unfilled)
2. Channel normalization (resolve_canonical_channel)
3. Post-call note word count (compute_post_call_note_word_count)

These are pure functions — no DB or API calls required.
"""

from datetime import datetime, timedelta, timezone

import pytest

# Add parent to path so sync.normalizer is importable
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sync.normalizer import (
    compute_compliance_failure,
    compute_outcome_unfilled,
    compute_post_call_note_word_count,
    is_excluded_stage,
    parse_ghl_datetime,
    resolve_canonical_channel,
)
from sync.ghl_client import (
    EXCLUDED_STAGE_IDS,
    TEMP_RYAN_STAGE_PREFIX,
    UPCOMING_1ST_CALL_BOOKED_STAGE_ID,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

NORMALIZATION_MAP = {
    "Facebook": "Facebook",
    "facebook": "Facebook",
    "Google Ads": "Google",
    "google ads": "Google",
    "Organic": "Organic",
    "referral": "Referral",
    "YT Ads": "YouTube",
}

# A datetime safely in the past (48h ago)
PAST_APPT = datetime.now(timezone.utc) - timedelta(hours=48)
# A datetime in the future
FUTURE_APPT = datetime.now(timezone.utc) + timedelta(hours=48)
# A datetime just within the 12h grace period
WITHIN_GRACE = datetime.now(timezone.utc) - timedelta(hours=6)


# ── 1. Compliance Flag Tests ─────────────────────────────────────────────────

class TestComputeComplianceFailure:
    """compute_compliance_failure: stage-specific, end-of-day check."""

    def test_flags_true_when_conditions_met(self):
        """Past appointment + correct stage + Confirmed status = True."""
        result = compute_compliance_failure(
            pipeline_stage_id=UPCOMING_1ST_CALL_BOOKED_STAGE_ID,
            call1_appointment_date=PAST_APPT,
            call1_appointment_status="Confirmed",
        )
        assert result is True

    def test_flags_true_with_none_status(self):
        """None status counts as stale — should flag."""
        result = compute_compliance_failure(
            pipeline_stage_id=UPCOMING_1ST_CALL_BOOKED_STAGE_ID,
            call1_appointment_date=PAST_APPT,
            call1_appointment_status=None,
        )
        assert result is True

    def test_false_wrong_stage(self):
        """Different stage = no flag, even if appointment is past."""
        result = compute_compliance_failure(
            pipeline_stage_id="some-other-stage-id",
            call1_appointment_date=PAST_APPT,
            call1_appointment_status="Confirmed",
        )
        assert result is False

    def test_false_future_appointment(self):
        """Future appointment = no flag."""
        result = compute_compliance_failure(
            pipeline_stage_id=UPCOMING_1ST_CALL_BOOKED_STAGE_ID,
            call1_appointment_date=FUTURE_APPT,
            call1_appointment_status="Confirmed",
        )
        assert result is False

    def test_false_showed_status(self):
        """Showed status = not stale, no flag."""
        result = compute_compliance_failure(
            pipeline_stage_id=UPCOMING_1ST_CALL_BOOKED_STAGE_ID,
            call1_appointment_date=PAST_APPT,
            call1_appointment_status="Showed",
        )
        assert result is False

    def test_false_no_appointment_date(self):
        """No appointment date = no flag."""
        result = compute_compliance_failure(
            pipeline_stage_id=UPCOMING_1ST_CALL_BOOKED_STAGE_ID,
            call1_appointment_date=None,
            call1_appointment_status="Confirmed",
        )
        assert result is False

    def test_case_sensitive_status(self):
        """lowercase 'confirmed' also counts as stale."""
        result = compute_compliance_failure(
            pipeline_stage_id=UPCOMING_1ST_CALL_BOOKED_STAGE_ID,
            call1_appointment_date=PAST_APPT,
            call1_appointment_status="confirmed",
        )
        assert result is True


class TestComputeOutcomeUnfilled:
    """compute_outcome_unfilled: any stage, 12h grace period."""

    def test_flags_true_past_grace_period(self):
        """Appointment 48h ago + Confirmed = True."""
        result = compute_outcome_unfilled(
            call1_appointment_date=PAST_APPT,
            call1_appointment_status="Confirmed",
        )
        assert result is True

    def test_false_within_grace_period(self):
        """Appointment 6h ago = still within 12h grace = False."""
        result = compute_outcome_unfilled(
            call1_appointment_date=WITHIN_GRACE,
            call1_appointment_status="Confirmed",
        )
        assert result is False

    def test_false_status_updated(self):
        """Status changed to Showed = not stale."""
        result = compute_outcome_unfilled(
            call1_appointment_date=PAST_APPT,
            call1_appointment_status="Showed",
        )
        assert result is False

    def test_false_no_show_status(self):
        """No Show status = rep logged it, not stale."""
        result = compute_outcome_unfilled(
            call1_appointment_date=PAST_APPT,
            call1_appointment_status="No Show",
        )
        assert result is False

    def test_false_no_appointment(self):
        result = compute_outcome_unfilled(
            call1_appointment_date=None,
            call1_appointment_status="Confirmed",
        )
        assert result is False

    def test_boundary_just_inside_grace(self):
        """Appointment 11h59m ago — still within 12h grace = False."""
        just_inside = datetime.now(timezone.utc) - timedelta(hours=11, minutes=59)
        result = compute_outcome_unfilled(
            call1_appointment_date=just_inside,
            call1_appointment_status="Confirmed",
        )
        assert result is False

    def test_boundary_just_past_grace(self):
        """Appointment 12h1m ago — past grace = True."""
        just_past = datetime.now(timezone.utc) - timedelta(hours=12, minutes=1)
        result = compute_outcome_unfilled(
            call1_appointment_date=just_past,
            call1_appointment_status="Confirmed",
        )
        assert result is True


# ── 2. Channel Normalization Tests ───────────────────────────────────────────

class TestResolveCanonicalChannel:
    """resolve_canonical_channel: three-tier fallback, case-insensitive."""

    def test_exact_match_first_priority(self):
        """UTM source (first priority) exact match."""
        result = resolve_canonical_channel(
            NORMALIZATION_MAP,
            attr_first_utm_source="Facebook",
            op_book_campaign_source="Google Ads",
            raw_ghl_source="referral",
        )
        assert result == "Facebook"

    def test_falls_through_to_second_priority(self):
        """When UTM source is None, use op_book_campaign_source."""
        result = resolve_canonical_channel(
            NORMALIZATION_MAP,
            attr_first_utm_source=None,
            op_book_campaign_source="Google Ads",
            raw_ghl_source="referral",
        )
        assert result == "Google"

    def test_falls_through_to_third_priority(self):
        """When first two are None, use raw GHL source."""
        result = resolve_canonical_channel(
            NORMALIZATION_MAP,
            attr_first_utm_source=None,
            op_book_campaign_source=None,
            raw_ghl_source="referral",
        )
        assert result == "Referral"

    def test_case_insensitive_fallback(self):
        """Case-insensitive match when exact fails."""
        result = resolve_canonical_channel(
            NORMALIZATION_MAP,
            attr_first_utm_source="FACEBOOK",
            op_book_campaign_source=None,
            raw_ghl_source=None,
        )
        assert result == "Facebook"

    def test_unknown_when_nothing_matches(self):
        """All sources present but none in normalization map."""
        result = resolve_canonical_channel(
            NORMALIZATION_MAP,
            attr_first_utm_source="TikTok",
            op_book_campaign_source="Snapchat",
            raw_ghl_source="carrier_pigeon",
        )
        assert result == "Unknown"

    def test_unknown_when_all_none(self):
        """No sources at all."""
        result = resolve_canonical_channel(
            NORMALIZATION_MAP,
            attr_first_utm_source=None,
            op_book_campaign_source=None,
            raw_ghl_source=None,
        )
        assert result == "Unknown"

    def test_empty_string_treated_as_missing(self):
        """Empty string should be treated as falsy / missing."""
        result = resolve_canonical_channel(
            NORMALIZATION_MAP,
            attr_first_utm_source="",
            op_book_campaign_source="Organic",
            raw_ghl_source=None,
        )
        assert result == "Organic"

    def test_skips_none_tries_next(self):
        """First source None, second unrecognized, third matches."""
        result = resolve_canonical_channel(
            NORMALIZATION_MAP,
            attr_first_utm_source=None,
            op_book_campaign_source="unknown_thing",
            raw_ghl_source="YT Ads",
        )
        assert result == "YouTube"


# ── 3. Post-Call Note Word Count Tests ───────────────────────────────────────

class TestComputePostCallNoteWordCount:
    """compute_post_call_note_word_count: qualifying note detection."""

    def test_qualifying_note_returns_word_count(self):
        """Rep's note within 72h window returns word count."""
        appt = datetime(2025, 6, 1, 14, 0, tzinfo=timezone.utc)
        notes = [
            {
                "userId": "rep-123",
                "dateAdded": "2025-06-01T16:00:00+00:00",
                "bodyText": "Spoke with prospect about pricing and timeline for next steps",
            }
        ]
        result = compute_post_call_note_word_count(notes, "rep-123", appt)
        assert result == 10  # "Spoke with prospect about pricing and timeline for next steps"

    def test_no_qualifying_note_returns_zero(self):
        """No notes from the rep → 0 (not None)."""
        appt = datetime(2025, 6, 1, 14, 0, tzinfo=timezone.utc)
        notes = [
            {
                "userId": "other-user",
                "dateAdded": "2025-06-01T16:00:00+00:00",
                "bodyText": "Automation note",
            }
        ]
        result = compute_post_call_note_word_count(notes, "rep-123", appt)
        assert result == 0

    def test_note_outside_window_excluded(self):
        """Note from rep but outside 72h window → 0."""
        appt = datetime(2025, 6, 1, 14, 0, tzinfo=timezone.utc)
        notes = [
            {
                "userId": "rep-123",
                "dateAdded": "2025-06-10T16:00:00+00:00",  # 9 days later
                "bodyText": "Late note should not count",
            }
        ]
        result = compute_post_call_note_word_count(notes, "rep-123", appt)
        assert result == 0

    def test_no_appointment_returns_none(self):
        """No appointment date → None (not applicable)."""
        result = compute_post_call_note_word_count([], "rep-123", None)
        assert result is None

    def test_no_owner_returns_none(self):
        """No owner ID → None."""
        appt = datetime(2025, 6, 1, 14, 0, tzinfo=timezone.utc)
        result = compute_post_call_note_word_count([], None, appt)
        assert result is None

    def test_picks_longest_qualifying_note(self):
        """Multiple qualifying notes → returns max word count."""
        appt = datetime(2025, 6, 1, 14, 0, tzinfo=timezone.utc)
        notes = [
            {
                "userId": "rep-123",
                "dateAdded": "2025-06-01T16:00:00+00:00",
                "bodyText": "Short note",
            },
            {
                "userId": "rep-123",
                "dateAdded": "2025-06-01T18:00:00+00:00",
                "bodyText": "This is a much longer and more detailed note about the call",
            },
        ]
        result = compute_post_call_note_word_count(notes, "rep-123", appt)
        assert result == 12  # "This is a much longer and more detailed note about the call"


# ── 4. Stage Exclusion Tests ─────────────────────────────────────────────────

class TestIsExcludedStage:
    def test_excluded_stage_id(self):
        excluded_id = next(iter(EXCLUDED_STAGE_IDS))
        assert is_excluded_stage(excluded_id, "Some Name") is True

    def test_temp_ryan_prefix(self):
        assert is_excluded_stage("random-id", "Temp Ryan Something") is True

    def test_normal_stage(self):
        assert is_excluded_stage("normal-id", "1st Call Done") is False

    def test_none_inputs(self):
        assert is_excluded_stage(None, None) is False


# ── 5. GHL Datetime Parsing Tests ────────────────────────────────────────────

class TestParseGhlDatetime:
    def test_iso_with_z_suffix(self):
        result = parse_ghl_datetime("2025-06-01T14:00:00Z")
        assert result == datetime(2025, 6, 1, 14, 0, tzinfo=timezone.utc)

    def test_iso_with_offset(self):
        result = parse_ghl_datetime("2025-06-01T14:00:00+00:00")
        assert result == datetime(2025, 6, 1, 14, 0, tzinfo=timezone.utc)

    def test_naive_gets_utc(self):
        result = parse_ghl_datetime("2025-06-01T14:00:00")
        assert result is not None
        assert result.tzinfo == timezone.utc

    def test_none_returns_none(self):
        assert parse_ghl_datetime(None) is None

    def test_empty_string_returns_none(self):
        assert parse_ghl_datetime("") is None

    def test_garbage_returns_none(self):
        assert parse_ghl_datetime("not-a-date") is None
