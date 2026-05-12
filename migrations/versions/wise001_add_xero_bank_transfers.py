"""Add xero_bank_transfers table for Wise wire reconciliation.

Revision ID: wise001
Revises: deals001
Create Date: 2026-05-12

Plain English: This creates a new table to store incoming wire transfers
from Wise (pulled via Xero's bank feed API). Each row is one transfer.
The table links transfers back to GHL deals once the matcher runs.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "wise001"
down_revision = "deals001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "xero_bank_transfers",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("xero_transaction_id", sa.String(100), unique=True, nullable=False),
        sa.Column("xero_account_id", sa.String(100), nullable=True),
        sa.Column("account_name", sa.String(100), nullable=True),
        sa.Column("date", sa.Date(), nullable=True),
        sa.Column("amount", sa.Numeric(12, 2), nullable=True),
        sa.Column("currency", sa.String(10), nullable=False, server_default="USD"),
        sa.Column("contact_name", sa.String(300), nullable=True),
        sa.Column("reference", sa.String(500), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("is_reconciled", sa.Boolean(), nullable=True),
        sa.Column("ghl_opportunity_id", sa.String(100), nullable=True),
        sa.Column("match_method", sa.String(50), nullable=False, server_default="none"),
        sa.Column("match_confidence", sa.String(20), nullable=False, server_default="unmatched"),
        sa.Column("match_score", sa.Numeric(5, 3), nullable=False, server_default="0"),
        sa.Column("is_confirmed", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("synced_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )
    op.create_index("ix_xero_bank_transfers_xero_transaction_id", "xero_bank_transfers", ["xero_transaction_id"])
    op.create_index("ix_xero_bank_transfers_date", "xero_bank_transfers", ["date"])
    op.create_index("ix_xero_bank_transfers_ghl_opportunity_id", "xero_bank_transfers", ["ghl_opportunity_id"])
    op.create_index("ix_xero_bank_transfers_match_confidence", "xero_bank_transfers", ["match_confidence"])


def downgrade() -> None:
    op.drop_table("xero_bank_transfers")
