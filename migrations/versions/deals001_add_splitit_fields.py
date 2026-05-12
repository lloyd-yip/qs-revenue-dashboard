"""Add Splitit fields to deal_whop_matches — is_splitit flag, first payment date,
and total installments count for payment plan visibility on the Deals dashboard.

Revision ID: deals001
Revises: funnel001
Create Date: 2026-05-12

Plain English:
  - is_splitit: TRUE when the deal's first Whop payment used the Splitit processor.
    Splitit = customer pays installments to Splitit, QS receives 100% upfront.
  - first_payment_date: date of the earliest paid Whop payment for matched deals.
    Used as the canonical close date (more reliable than GHL's wonlostabandoned_date
    custom field, which can be set to future dates by reps or automation errors).
  - total_installments: count of ALL payment records (all statuses) returned by Whop
    for the membership. Used as a proxy for total plan length in months.
    NOTE: this assumes Whop pre-creates future payment records at signup.
    Debug log on first Run Match confirms this — if Whop does NOT pre-create records,
    total_installments will equal payment_count (paid-only) and must be sourced
    from the plan/membership object instead.

VERIFICATION — run in Supabase SQL editor after deploy:
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'deal_whop_matches'
      AND column_name IN ('is_splitit', 'first_payment_date', 'total_installments');
    -- Should return 3 rows.

    -- After running Run Match, check backfill:
    SELECT is_splitit, first_payment_date, total_installments, match_confidence
    FROM deal_whop_matches
    WHERE match_confidence IN ('high', 'medium')
    LIMIT 10;

SILENT FAILURE SIGNAL: If first_payment_date is NULL for all HIGH matches,
the payments fetch is failing or no payments exist yet in Whop for those memberships.
Check Railway logs from the Run Match run for payment fetch errors.
"""

from alembic import op


revision = "deals001"
down_revision = "funnel001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE deal_whop_matches
        ADD COLUMN IF NOT EXISTS is_splitit BOOLEAN,
        ADD COLUMN IF NOT EXISTS first_payment_date DATE,
        ADD COLUMN IF NOT EXISTS total_installments INTEGER
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE deal_whop_matches
        DROP COLUMN IF EXISTS is_splitit,
        DROP COLUMN IF EXISTS first_payment_date,
        DROP COLUMN IF EXISTS total_installments
    """)
