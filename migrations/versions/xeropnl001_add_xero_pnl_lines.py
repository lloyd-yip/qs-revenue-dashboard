"""Add xero_pnl_lines — faithful monthly copy of the Xero P&L (company-wide).

Revision ID: xeropnl001
Revises: whopstats001
Create Date: 2026-08-07

Plain English: one row per (month, section, account) straight from the Xero Profit &
Loss report — EVERY income and expense account, nothing dropped. The Company
dashboard reads this so Revenue / Expenses / Net Profit match Xero exactly (the
marketing-only expense view on the P&L page is unaffected — this is a separate store).
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "xeropnl001"
down_revision = "whopstats001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "xero_pnl_lines",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("period_start", sa.Date(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("section", sa.String(120), nullable=False),      # Xero section title
        sa.Column("account", sa.String(200), nullable=False),      # account/line name
        sa.Column("is_income", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("amount_usd", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("amount_eur", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("eur_usd", sa.Numeric(8, 4), nullable=True),
        sa.Column("synced_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.UniqueConstraint("period_start", "period_end", "section", "account", name="uq_xero_pnl_line"),
    )
    op.create_index("ix_xero_pnl_lines_period_start", "xero_pnl_lines", ["period_start"])


def downgrade() -> None:
    op.drop_table("xero_pnl_lines")
