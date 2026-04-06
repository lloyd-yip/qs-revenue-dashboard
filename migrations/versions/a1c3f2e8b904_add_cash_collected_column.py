"""add_cash_collected_column

Revision ID: a1c3f2e8b904
Revises: 47ff03aec99f
Create Date: 2026-04-06

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'a1c3f2e8b904'
down_revision: Union[str, Sequence[str], None] = '47ff03aec99f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # IF NOT EXISTS guards against re-running when the column already exists in production
    op.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS cash_collected NUMERIC(12, 2)")


def downgrade() -> None:
    op.drop_column('opportunities', 'cash_collected')
