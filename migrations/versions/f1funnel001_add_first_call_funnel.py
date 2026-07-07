"""add first_call_funnel column

Reporting funnel for the 1st call, derived from the 1st-call calendar name during
sync (webinar / outreach / referral). Powers the Lead-Quality-by-Channel funnel
toggle and the Pipeline-Intelligence funnel segment. Part of the F1 calendar-based
funnel rework.

Revision ID: f1funnel001
Revises: whop001
Create Date: 2026-07-07

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f1funnel001'
down_revision: Union[str, Sequence[str], None] = 'whop001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('opportunities', sa.Column('first_call_funnel', sa.String(), nullable=True))
    op.create_index('ix_opportunities_first_call_funnel', 'opportunities', ['first_call_funnel'])


def downgrade() -> None:
    op.drop_index('ix_opportunities_first_call_funnel', table_name='opportunities')
    op.drop_column('opportunities', 'first_call_funnel')
