"""Pydantic response models for all API endpoints."""

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel


class MetaMixin(BaseModel):
    date_start: date
    date_end: date
    date_by: str
    generated_at: datetime


class SummaryData(BaseModel):
    calls_booked_1st: int
    shows_1st: int
    show_rate_1st: float | None
    no_show_rate_1st: float | None
    calls_booked_2nd: int
    shows_2nd: int
    show_rate_2nd: float | None
    qualification_rate: float | None
    dq_rate: float | None
    close_rate: float | None
    units_closed: int
    projected_contract_value: float
    total_shows: int
    compliance_failures: int


class SummaryResponse(BaseModel):
    data: SummaryData
    meta: MetaMixin


class RepMetrics(BaseModel):
    rep_id: str | None
    rep_name: str
    calls_booked_1st: int
    shows_1st: int
    show_rate_1st: float | None
    no_show_rate_1st: float | None
    calls_booked_2nd: int
    shows_2nd: int
    show_rate_2nd: float | None
    qualification_rate: float | None
    dq_rate: float | None
    close_rate: float | None
    units_closed: int
    projected_contract_value: float
    total_shows: int
    compliance_failures: int


class ByRepResponse(BaseModel):
    data: list[RepMetrics]
    meta: MetaMixin


class ChannelBreakdown(BaseModel):
    channel: str
    total_ops: int
    shows: int
    units_closed: int
    projected_contract_value: float


class LeadSourceResponse(BaseModel):
    data: list[ChannelBreakdown]
    meta: MetaMixin


class QualificationBreakdownItem(BaseModel):
    value: str
    count: int


class QualificationData(BaseModel):
    lead_quality: list[QualificationBreakdownItem]
    financial_qual: list[QualificationBreakdownItem]
    intent_to_transform: list[QualificationBreakdownItem]
    pre_call_indoctrination: list[QualificationBreakdownItem]
    business_fit: list[QualificationBreakdownItem]
    pain_goal_oriented: list[QualificationBreakdownItem]
    business_industry: list[QualificationBreakdownItem]
    current_revenue: list[QualificationBreakdownItem]
    dq_reason: list[QualificationBreakdownItem]
    deal_lost_reasons: list[QualificationBreakdownItem]


class QualificationResponse(BaseModel):
    data: QualificationData
    meta: MetaMixin


class SyncStatusData(BaseModel):
    sync_type: str
    status: str
    started_at: datetime
    completed_at: datetime | None
    opportunities_synced: int
    errors_count: int


class SyncStatusResponse(BaseModel):
    data: SyncStatusData | None
    message: str


class SyncTriggerResponse(BaseModel):
    message: str
    sync_type: str


class HealthResponse(BaseModel):
    status: str
    db_connected: bool
    last_sync_at: datetime | None
