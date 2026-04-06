"""add_projected_deal_size_column

Revision ID: 47ff03aec99f
Revises: e5d989ac5418
Create Date: 2026-04-03 20:17:19.953832

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '47ff03aec99f'
down_revision: Union[str, Sequence[str], None] = 'e5d989ac5418'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('opportunities', sa.Column('projected_deal_size', sa.Numeric(precision=12, scale=2), nullable=True))


def downgrade() -> None:
    op.drop_column('opportunities', 'projected_deal_size')
