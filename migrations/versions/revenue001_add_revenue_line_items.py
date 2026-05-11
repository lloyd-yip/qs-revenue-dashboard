"""Add revenue_line_items table for P&L revenue tracking from Whop.

Revision ID: revenue001
Revises: expenses001
Create Date: 2026-05-11

── VERIFICATION (run these in Supabase SQL editor after deploying) ──────────

1. Confirm table exists and has correct columns:
   SELECT column_name, data_type
   FROM information_schema.columns
   WHERE table_name = 'revenue_line_items'
   ORDER BY ordinal_position;
   → Should return: id, period_start, period_end, source, category,
     product_type, amount, payment_count, notes, created_at, updated_at

2. Confirm unique constraint is in place:
   SELECT constraint_name FROM information_schema.table_constraints
   WHERE table_name = 'revenue_line_items' AND constraint_type = 'UNIQUE';
   → Should return one row (the composite unique key)

3. Confirm migration is recorded as applied:
   SELECT version_num FROM alembic_version;
   → Should include 'revenue001'

── SILENT FAILURE SIGNAL ───────────────────────────────────────────────────
If the seed script posts revenue data and returns {"ok": true} but
GET /api/dashboard/revenue returns empty [], the migration didn't run —
the table doesn't exist on Railway. Check Railway deploy logs for
"[ERROR] relation revenue_line_items does not exist".

── PLAIN ENGLISH ────────────────────────────────────────────────────────────
Each row = one revenue bucket for one month. The UNIQUE constraint on
(period_start, period_end, source, category, product_type) means re-seeding
the same month always overwrites, never duplicates — safe to run twice.
"""

from alembic import op

revision = "revenue001"
down_revision = "expenses001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS revenue_line_items (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            period_start DATE NOT NULL,
            period_end DATE NOT NULL,
            source VARCHAR(50) NOT NULL,
            category VARCHAR(50) NOT NULL,
            product_type VARCHAR(100) NOT NULL,
            amount NUMERIC(12, 2) NOT NULL,
            payment_count INTEGER NOT NULL DEFAULT 0,
            notes TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (period_start, period_end, source, category, product_type)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_revenue_line_items_period_start
        ON revenue_line_items (period_start)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_revenue_line_items_period_end
        ON revenue_line_items (period_end)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_revenue_line_items_source
        ON revenue_line_items (source)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_revenue_line_items_category
        ON revenue_line_items (category)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS revenue_line_items")
