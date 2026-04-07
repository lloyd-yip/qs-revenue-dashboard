"""Add period_marketing_spend and rep_compensation tables for Funnel Economics RORI.

Revision ID: fe001
Revises: merge_001
Create Date: 2026-04-07
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = 'fe001'
down_revision = 'merge_001'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS period_marketing_spend (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            period_start DATE NOT NULL,
            period_end DATE NOT NULL,
            amount NUMERIC(12, 2) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (period_start, period_end)
        )
    """)
    op.execute("""
        CREATE TABLE IF NOT EXISTS rep_compensation (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            rep_id TEXT NOT NULL,
            rep_name TEXT NOT NULL,
            period_start DATE NOT NULL,
            period_end DATE NOT NULL,
            total_comp NUMERIC(12, 2) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (rep_id, period_start, period_end)
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS rep_compensation")
    op.execute("DROP TABLE IF EXISTS period_marketing_spend")
