"""add retell_calls.cost_cents — per-call cost from the Retell API

Lets the Retell channel Cost auto-populate from Retell's own billing (sum of per-call
combined_cost), falling back to the Xero "Retellai" expense line when unavailable.
Additive + reversible.

Revision ID: retellcost001
Revises: retell001
Create Date: 2026-07-27

"""
from typing import Sequence, Union

from alembic import op


revision: str = 'retellcost001'
down_revision: Union[str, Sequence[str], None] = 'retell001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE retell_calls ADD COLUMN IF NOT EXISTS cost_cents INTEGER")


def downgrade() -> None:
    op.execute("ALTER TABLE retell_calls DROP COLUMN IF EXISTS cost_cents")
