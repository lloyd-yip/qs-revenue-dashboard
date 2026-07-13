"""Finance-domain ORM models — manual financial inputs, expense/revenue line items.

Extracted from db/models.py (which re-exports these, so `from db.models import X`
keeps working everywhere).
"""

import uuid
from datetime import date, datetime

from sqlalchemy import Date, DateTime, Boolean, Integer, Numeric, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from db.session import Base


class PeriodMarketingSpend(Base):
    """Total marketing spend entered manually for a specific date range.

    One row per period. Unique on (period_start, period_end) — re-saving the same
    period overwrites the previous value.
    """

    __tablename__ = "period_marketing_spend"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    period_start: Mapped[datetime] = mapped_column(Date, nullable=False)
    period_end: Mapped[datetime] = mapped_column(Date, nullable=False)
    amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class RepCompensation(Base):
    """Total compensation (base + bonus) per rep per date range, entered manually.

    Unique on (rep_id, period_start, period_end) — re-saving overwrites.
    """

    __tablename__ = "rep_compensation"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    rep_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    rep_name: Mapped[str] = mapped_column(String, nullable=False)
    period_start: Mapped[datetime] = mapped_column(Date, nullable=False)
    period_end: Mapped[datetime] = mapped_column(Date, nullable=False)
    total_comp: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class RepCompSetting(Base):
    """Per-rep compensation model — base salary + commission rate, one row per rep.

    Drives the DERIVED rep cost on the Sales dashboard: base salary is prorated
    over the selected window, commission accrues as commission_pct × cash collected
    on the rep's cohort deals. Deliberately NOT read from Xero payouts — cash-basis
    payouts lag split-payment deals by months, so they mis-attribute cost to the
    wrong period. Missing rows fall back to base 0 / commission 10%.
    """

    __tablename__ = "rep_comp_settings"

    rep_id: Mapped[str] = mapped_column(String, primary_key=True)
    rep_name: Mapped[str] = mapped_column(String, nullable=False)
    base_salary_monthly: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False, default=0)
    commission_pct: Mapped[float] = mapped_column(Numeric(5, 2), nullable=False, default=10)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class ExpenseLineItem(Base):
    """Classified expense line items stored from monthly Xero pull.

    One row per (period_start, period_end, bucket, vendor).
    Upsert on that unique key — re-loading the same month overwrites cleanly.
    bucket values: 'sales' | 'marketing_salaries' | 'tech_tools' | 'paid_ads' | 'experiments'
    """

    __tablename__ = "expense_line_items"
    __table_args__ = (
        UniqueConstraint("period_start", "period_end", "bucket", "vendor"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    period_start: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    period_end: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    bucket: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    vendor: Mapped[str] = mapped_column(String(150), nullable=False)
    amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    is_approximate: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class RevenueLineItem(Base):
    """Revenue line items loaded from Whop payments API (and future Xero income).

    One row per (period_start, period_end, source, category, product_type).
    source values: 'whop'
    category values: 'cash_collected' | 'splitit_ar'
    product_type values: 'high_ticket' | 'saas'
    Upsert on the unique key — re-seeding the same month overwrites cleanly.
    """

    __tablename__ = "revenue_line_items"
    __table_args__ = (
        UniqueConstraint("period_start", "period_end", "source", "category", "product_type"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    period_start: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    period_end: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    source: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    category: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    product_type: Mapped[str] = mapped_column(String(100), nullable=False)
    amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)
    payment_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
