"""add_call2_calendar_id_column

Revision ID: bfefce47f6ac
Revises: 47ff03aec99f
Create Date: 2026-04-04 21:30:42.123056

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'bfefce47f6ac'
down_revision: Union[str, Sequence[str], None] = '47ff03aec99f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # IF NOT EXISTS guards against re-running when the column already exists in production
    op.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS call2_calendar_id TEXT")


def downgrade() -> None:
    op.drop_column('opportunities', 'call2_calendar_id')
