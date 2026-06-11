"""Add ClarityPay and net cash columns to deal_whop_matches.

Revision ID: whop001
Revises: wise001
Create Date: 2026-06-11

Plain English:
  - is_claritypay: TRUE when the deal's Whop payments used the ClarityPay processor.
    ClarityPay = external financing, QS receives the full amount upfront minus a 15% fee.
    NOTE: ClarityPay is invisible at the membership level (membership.payment_processor
    reads "multi_psp"). It is ONLY detectable on the PAYMENT object
    (payment.payment_processor == "claritypay"). Verified against live Whop data 2026-06-11.
  - provider_fee_pct: 0.15 for Splitit/ClarityPay, 0.0 for internal plans / pay-in-full.
  - net_cash_collected: total_paid * (1 - provider_fee_pct). What QS actually keeps.
    For Splitit/ClarityPay, Whop records the full contract as a single upfront payment,
    so total_paid == full contract and net = contract * 0.85.
    For internal plans, total_paid = installments collected to date (no fee).
  - plan_months_flag: TRUE when an internal plan (NOT Splitit/ClarityPay) has
    total_installments > 3. Sourced from membership.split_pay_required_payments,
    NOT len(payments) — Whop does not pre-create future installment records.
    Flags unusually long internal plans for Lloyd's review.

VERIFICATION — run in Supabase SQL editor after deploy:
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'deal_whop_matches'
      AND column_name IN ('is_claritypay', 'provider_fee_pct', 'net_cash_collected', 'plan_months_flag');
    -- Should return 4 rows.

    -- After running the EOD refresh, check backfill:
    SELECT ghl_owner_name, net_cash_collected, provider_fee_pct, is_splitit, is_claritypay, plan_months_flag
    FROM deal_whop_matches
    WHERE match_confidence IN ('high', 'medium')
      AND first_payment_date >= date_trunc('month', CURRENT_DATE)
    LIMIT 10;

SILENT FAILURE SIGNAL: If net_cash_collected is NULL for all current-month high/medium
matches after the EOD refresh runs, check Railway logs for "[whop-refresh]" lines —
the Whop payments fetch is failing or no matched deals have a whop_membership_id.
"""

from alembic import op


revision = "whop001"
down_revision = "wise001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE deal_whop_matches
        ADD COLUMN IF NOT EXISTS is_claritypay BOOLEAN,
        ADD COLUMN IF NOT EXISTS provider_fee_pct NUMERIC(5, 4),
        ADD COLUMN IF NOT EXISTS net_cash_collected NUMERIC(12, 2),
        ADD COLUMN IF NOT EXISTS plan_months_flag BOOLEAN
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE deal_whop_matches
        DROP COLUMN IF EXISTS is_claritypay,
        DROP COLUMN IF EXISTS provider_fee_pct,
        DROP COLUMN IF EXISTS net_cash_collected,
        DROP COLUMN IF EXISTS plan_months_flag
    """)
