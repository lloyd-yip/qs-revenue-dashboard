"""Add expense_line_items table for P&L expense tracking.

Revision ID: expenses001
Revises: upsell001
Create Date: 2026-05-08
"""

from alembic import op

revision = "expenses001"
down_revision = "upsell001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS expense_line_items (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            period_start DATE NOT NULL,
            period_end DATE NOT NULL,
            bucket VARCHAR(50) NOT NULL,
            vendor VARCHAR(150) NOT NULL,
            amount NUMERIC(12, 2) NOT NULL,
            is_approximate BOOLEAN NOT NULL DEFAULT FALSE,
            notes TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (period_start, period_end, bucket, vendor)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_expense_line_items_period_start
        ON expense_line_items (period_start)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_expense_line_items_period_end
        ON expense_line_items (period_end)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_expense_line_items_bucket
        ON expense_line_items (bucket)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS expense_line_items")
