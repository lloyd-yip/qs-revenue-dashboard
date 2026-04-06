"""add call1_calendar_id column

Revision ID: e5d989ac5418
Revises: 004
Create Date: 2026-04-03 14:19:06.008758

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e5d989ac5418'
down_revision: Union[str, Sequence[str], None] = '004'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('opportunities', sa.Column('call1_calendar_id', sa.String(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('opportunities', 'call1_calendar_id')
