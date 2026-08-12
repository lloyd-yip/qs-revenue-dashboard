"""Add xero_pnl_line_items — per-payee detail behind each Company-wide P&L account.

Revision ID: xeropnldetail001
Revises: xeropnl001
Create Date: 2026-08-12

Plain English: the P&L report gives account totals (e.g. "Tools - Funnel $7,215"). This
table stores the payees inside each account (from Xero SPEND bank transactions) so the
Company-wide P&L can expand an account down to its line items. One row per (month,
account, payee).
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "xeropnldetail001"
down_revision = "xeropnl001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "xero_pnl_line_items",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("period_start", sa.Date(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("account", sa.String(200), nullable=False),
        sa.Column("payee", sa.String(300), nullable=False),
        sa.Column("amount_usd", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("synced_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.UniqueConstraint("period_start", "period_end", "account", "payee", name="uq_xero_pnl_line_item"),
    )
    op.create_index("ix_xero_pnl_line_items_period_start", "xero_pnl_line_items", ["period_start"])


def downgrade() -> None:
    op.drop_table("xero_pnl_line_items")
