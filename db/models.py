import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Integer,
    JSON,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from db.session import Base


class Opportunity(Base):
    __tablename__ = "opportunities"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # GHL identifiers
    ghl_opportunity_id: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    ghl_contact_id: Mapped[str | None] = mapped_column(String, nullable=True)
    opportunity_name: Mapped[str | None] = mapped_column(String, nullable=True)

    # Pipeline stage
    pipeline_stage_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    pipeline_stage_name: Mapped[str | None] = mapped_column(String, nullable=True)

    # Exclusion flag — TRUE for Duplicates and Temp Ryan stages
    is_excluded: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)

    # Rep attribution (Opportunity Owner — not appointment assigned user)
    opportunity_owner_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    opportunity_owner_name: Mapped[str | None] = mapped_column(String, nullable=True)

    # Deal value (rep-entered estimate — directional only)
    monetary_value: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)

    # Per-call status (from Opportunity custom fields — primary show/no-show signal)
    # Field IDs: Call 1 = V82ErbW24izA5aQUzRUv, Call 2 = WMj5zj7G8wBTtp3OqjKp
    # Values: Confirmed / Showed / No Show / Cancelled
    call1_appointment_status: Mapped[str | None] = mapped_column(String, nullable=True)
    call2_appointment_status: Mapped[str | None] = mapped_column(String, nullable=True)

    # Appointment dates (from Opportunity custom fields)
    # Call 1 initial: We5c2Oiz8kC3FgjOO2XD | Call 1 rescheduled: bFDWu3koncdxn26h6nAm | Call 2: oRRLUFWNYEeYSDVqV3DK
    call1_appointment_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    call2_appointment_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)


    # Qualification fields (dropdowns on Opportunity)
    # Lead Quality: M8RuTSXsLhZMvdMWAlLr — Great / Ok / Barely Passable / Bad / DQ
    lead_quality: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    # Financial Qual: BLtbMbfQhd0ODu7ywNIu — Yes / Somewhat / No / I don't know
    financial_qual: Mapped[str | None] = mapped_column(String, nullable=True)
    # Intent To Transform: IY2SCImbFeg5qkGRpCmy
    intent_to_transform: Mapped[str | None] = mapped_column(String, nullable=True)
    # Pre Call Indoctrination: ogT4HksPoylcBN7vNgtX
    pre_call_indoctrination: Mapped[str | None] = mapped_column(String, nullable=True)
    # Business Fit: WugaBcJwKZzXaxrXlGg4
    business_fit: Mapped[str | None] = mapped_column(String, nullable=True)
    # Pain/Goal Oriented: WJddOo1awmnVDVlKgf8Q — Pain / Goal / I don't know
    pain_goal_oriented: Mapped[str | None] = mapped_column(String, nullable=True)
    # DQ Reason: zVSqT9ogJzXIBUi49F1F
    dq_reason: Mapped[str | None] = mapped_column(String, nullable=True)
    # Deal Lost Reasons: PDM9cXyNljhX9qeQpSAH
    deal_lost_reasons: Mapped[str | None] = mapped_column(String, nullable=True)

    # Firmographic
    # Business Industry: fyYxLA4EvjZpifanMBm2
    business_industry: Mapped[str | None] = mapped_column(String, nullable=True)
    # Current Revenue of Business: E2xd173q56x3GB5m1qm1
    current_revenue: Mapped[str | None] = mapped_column(String, nullable=True)

    # Attribution — first touch (from attributions[0] on GHL opportunity)
    attr_first_utm_source: Mapped[str | None] = mapped_column(String, nullable=True)
    attr_first_utm_medium: Mapped[str | None] = mapped_column(String, nullable=True)
    attr_first_utm_campaign: Mapped[str | None] = mapped_column(String, nullable=True)

    # Attribution — last touch (from attributions[-1] on GHL opportunity)
    attr_last_utm_source: Mapped[str | None] = mapped_column(String, nullable=True)
    attr_last_utm_medium: Mapped[str | None] = mapped_column(String, nullable=True)
    attr_last_utm_campaign: Mapped[str | None] = mapped_column(String, nullable=True)

    # Booking-time attribution custom fields (secondary fallback)
    # OP Book - Campaign Source: siKjWZIScNTHSk38LJqt
    op_book_campaign_source: Mapped[str | None] = mapped_column(String, nullable=True)
    # OP Book - Campaign Medium: itX1JvyAAUtxbHuXtMOB
    op_book_campaign_medium: Mapped[str | None] = mapped_column(String, nullable=True)
    # OP Book - Campaign Name: MYzEZQzFw8G42mrTJVKy
    op_book_campaign_name: Mapped[str | None] = mapped_column(String, nullable=True)

    # Canonical channel — computed at sync time
    # Priority: attr_first_utm_source → op_book_campaign_source → raw source field → 'Unknown'
    canonical_channel: Mapped[str | None] = mapped_column(String, nullable=True, index=True)

    # Rep compliance failure flag — set when:
    #   pipeline_stage = Upcoming 1st Call Booked
    #   AND call1_appointment_date has passed EOD
    #   AND call1_appointment_status is still Confirmed (or NULL)
    rep_compliance_failure: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)

    # Outcome unfilled — broader signal: appointment passed + 12h grace, status never updated.
    # No stage restriction — catches all opps where rep forgot to log the call outcome.
    # Used as the show rate denominator exclusion (replaces rep_compliance_failure for that purpose).
    outcome_unfilled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, index=True)

    # Post-call note word count — word count of rep's own note within 12h of appointment.
    # NULL  = notes check not applicable (no-show / cancelled / future appointment)
    # 0     = showed, no qualifying rep note found within 12h window
    # N     = word count of the longest qualifying rep note found
    post_call_note_word_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Compliance history timestamps
    # Set when outcome_unfilled first becomes True — never cleared
    outcome_unfilled_first_flagged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Set when outcome_unfilled transitions True → False (rep fixed it)
    outcome_unfilled_resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # GHL timestamps
    created_at_ghl: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    updated_at_ghl: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # Sync metadata
    synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class SyncRun(Base):
    __tablename__ = "sync_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sync_type: Mapped[str] = mapped_column(String, nullable=False)  # 'incremental' | 'full'
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String, nullable=False)  # 'running' | 'completed' | 'failed'
    opportunities_synced: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    errors_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_details: Mapped[list | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class SourceNormalization(Base):
    """Configurable mapping from raw UTM/source values to canonical channel names.
    Seeded from the UTM Builder CSV. Editable without code changes."""

    __tablename__ = "source_normalization"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    raw_value: Mapped[str] = mapped_column(String, unique=True, nullable=False, index=True)
    canonical_channel: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
