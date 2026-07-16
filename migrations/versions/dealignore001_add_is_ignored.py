"""Add is_ignored flag to deal_whop_matches — lets a reviewer dismiss a
needs-review (no-Whop) deal from the Live lens without counting it.

Revision ID: dealignore001
Revises: syncguard001
Create Date: 2026-07-16

Plain English:
  - is_ignored: TRUE when a human decided a won-this-month deal with no Whop
    payment (e.g. a mistaken/duplicate close, or one that will never settle via
    Whop) should DISAPPEAR from the Live · This Month review list. Distinct from
    is_confirmed (which makes a wire/no-Whop deal COUNT). A deal is either
    pending review, confirmed (counts), or ignored (hidden) — never counted when
    ignored.

VERIFICATION — run in Supabase SQL editor after deploy:
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'deal_whop_matches' AND column_name = 'is_ignored';
    -- Should return 1 row (boolean).
"""

from alembic import op


revision = "dealignore001"
down_revision = "syncguard001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE deal_whop_matches
        ADD COLUMN IF NOT EXISTS is_ignored BOOLEAN NOT NULL DEFAULT FALSE
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE deal_whop_matches
        DROP COLUMN IF EXISTS is_ignored
    """)
