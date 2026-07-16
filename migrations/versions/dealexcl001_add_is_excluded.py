"""Add is_excluded flag to deal_whop_matches — deals whose Whop product is a
separate, non-coaching offer (e.g. Calendar Automation) are excluded from every
QS revenue metric.

Revision ID: dealexcl001
Revises: dealignore001
Create Date: 2026-07-16

Plain English:
  - is_excluded: TRUE when the deal's Whop presence is a product listed in
    config.EXCLUDED_WHOP_PRODUCT_IDS (a separately-sold offer, not QS coaching).
    Set automatically by the matcher; distinct from is_ignored (a manual
    per-deal dismissal of a no-Whop review row). Excluded deals are filtered out
    of the Live + Historical Deals views and carry no cash into other metrics.

VERIFICATION — run in Supabase SQL editor after deploy:
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'deal_whop_matches' AND column_name = 'is_excluded';
    -- Should return 1 row (boolean).
"""

from alembic import op


revision = "dealexcl001"
down_revision = "dealignore001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE deal_whop_matches
        ADD COLUMN IF NOT EXISTS is_excluded BOOLEAN NOT NULL DEFAULT FALSE
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE deal_whop_matches
        DROP COLUMN IF EXISTS is_excluded
    """)
