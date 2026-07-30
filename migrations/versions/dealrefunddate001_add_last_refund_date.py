"""Add last_refund_date to deal_whop_matches + whop_orphan_payments — the DATE a
deal's refund was initiated (latest refunded Whop payment), so the Collections view
can attribute a refund to the month it happened rather than the deal's first-payment
month.

Revision ID: dealrefunddate001
Revises: awh001
Create Date: 2026-07-30

Plain English:
  - last_refund_date: date of the most recent refunded Whop payment on this deal
    (from the payment's refunded_at timestamp). NULL when there's no refund, or
    until the next Whop refresh backfills it. Collections uses it to bucket a
    refund into the month it was initiated; when NULL it falls back to the deal's
    first_payment_date month (the prior behaviour), so this is safe pre-backfill.

VERIFICATION — run in Supabase SQL editor after deploy:
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE column_name = 'last_refund_date'
    ORDER BY table_name;
    -- Should return 2 rows (deal_whop_matches, whop_orphan_payments), type date.
"""

from alembic import op


revision = "dealrefunddate001"
down_revision = "awh001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE deal_whop_matches
        ADD COLUMN IF NOT EXISTS last_refund_date DATE
    """)
    op.execute("""
        ALTER TABLE whop_orphan_payments
        ADD COLUMN IF NOT EXISTS last_refund_date DATE
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE deal_whop_matches
        DROP COLUMN IF EXISTS last_refund_date
    """)
    op.execute("""
        ALTER TABLE whop_orphan_payments
        DROP COLUMN IF EXISTS last_refund_date
    """)
