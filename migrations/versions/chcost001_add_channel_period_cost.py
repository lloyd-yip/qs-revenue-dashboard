"""add channel_period_cost — manual per-channel cost for ROI

Powers the ROI / cost-per-show|qual|close figures on the new per-channel detail page.
Marketing spend today is period-level only (period_marketing_spend), not per-channel,
so a lightweight manual per-channel cost is stored here, keyed to the exact date range
the manager is analysing. Reversible.

Revision ID: chcost001
Revises: aichan001
Create Date: 2026-07-27

"""
from typing import Sequence, Union

from alembic import op


revision: str = 'chcost001'
down_revision: Union[str, Sequence[str], None] = 'aichan001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS channel_period_cost (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            channel TEXT NOT NULL,
            period_start DATE NOT NULL,
            period_end DATE NOT NULL,
            cost NUMERIC(12, 2) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_channel_period_cost UNIQUE (channel, period_start, period_end)
        );
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_channel_period_cost_channel "
        "ON channel_period_cost (channel);"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_channel_period_cost_channel;")
    op.execute("DROP TABLE IF EXISTS channel_period_cost;")
