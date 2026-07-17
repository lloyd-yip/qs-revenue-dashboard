"""Add total_refunded to deal_whop_matches — cash refunded back to the customer
on this deal's Whop payments (status='refunded' / a refunded amount).

Revision ID: dealrefund001
Revises: dealexcl001
Create Date: 2026-07-17

Plain English:
  - total_refunded: sum of refunded amounts across the deal's Whop payments.
    Surfaced on the Collections view so we can see gross cash collected vs. how
    much was refunded (net = total_paid - total_refunded). Populated by the
    matcher / nightly refresh from the Whop payment refund status.

VERIFICATION — run in Supabase SQL editor after deploy:
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'deal_whop_matches' AND column_name = 'total_refunded';
    -- Should return 1 row (numeric).
"""

from alembic import op


revision = "dealrefund001"
down_revision = "dealexcl001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE deal_whop_matches
        ADD COLUMN IF NOT EXISTS total_refunded NUMERIC(12, 2)
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE deal_whop_matches
        DROP COLUMN IF EXISTS total_refunded
    """)
