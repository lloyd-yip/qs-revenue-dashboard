"""Add whop_orphan_payments — Whop coaching payments with NO matching GHL deal.

Revision ID: whoporphan001
Revises: dealrefund001
Create Date: 2026-07-21

Plain English:
  Some real coaching payments land on Whop for a customer who has no closed-won
  GHL opportunity (never created, or created under a different contact). The
  matcher can't attach them to a deal, so they're invisible to the dashboard.
  This table holds those "orphan" memberships (unclaimed, non-excluded product,
  paid >= the coaching floor) so they can be reviewed on the New Deals tab and
  confirmed to count (under Unassigned) — or ignored.

  status: pending (awaiting review) | confirmed (counts) | ignored (hidden).

VERIFICATION — run in Supabase SQL editor after deploy:
    SELECT COUNT(*) FROM whop_orphan_payments;   -- table exists (0 until Run Match)
"""

from alembic import op


revision = "whoporphan001"
down_revision = "dealrefund001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS whop_orphan_payments (
            id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            whop_membership_id  varchar(100) NOT NULL UNIQUE,
            whop_email          varchar(255),
            whop_name           varchar(150),
            whop_product_id     varchar(100),
            first_payment_date  date,
            total_paid          numeric(12,2),
            net_cash_collected  numeric(12,2),
            upfront_cash        numeric(12,2),
            total_refunded      numeric(12,2),
            payment_count       integer,
            total_installments  integer,
            is_splitit          boolean,
            is_claritypay       boolean,
            plan_months_flag    boolean,
            provider_fee_pct    numeric(5,4),
            status              varchar(20) NOT NULL DEFAULT 'pending',
            last_seen_at        timestamptz,
            created_at          timestamptz NOT NULL DEFAULT now(),
            updated_at          timestamptz NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_whop_orphan_first_payment_date ON whop_orphan_payments (first_payment_date)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_whop_orphan_status ON whop_orphan_payments (status)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS whop_orphan_payments")
