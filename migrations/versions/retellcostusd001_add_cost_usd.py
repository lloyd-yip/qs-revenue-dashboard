"""add retell_calls.cost_usd — precise per-call cost (replaces truncated cost_cents)

Retell's call_cost.combined_cost is a FLOAT denominated in cents (e.g. 0.55, 4.1833333).
It was stored via int(), which floored every sub-cent call to 0 and dropped the fraction
on the rest — July summed to $23.75 against $151.64 of real per-call cost. A numeric
column keeps the exact value, in USD.

cost_cents is left in place (unused) so this is trivially reversible.

Revision ID: retellcostusd001
Revises: xeropnldetail001
Create Date: 2026-08-20

"""
from typing import Sequence, Union

from alembic import op


revision: str = 'retellcostusd001'
down_revision: Union[str, Sequence[str], None] = 'xeropnldetail001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE retell_calls ADD COLUMN IF NOT EXISTS cost_usd NUMERIC(12, 6)")


def downgrade() -> None:
    op.execute("ALTER TABLE retell_calls DROP COLUMN IF EXISTS cost_usd")
