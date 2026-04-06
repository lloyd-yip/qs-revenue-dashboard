"""Merge heads: a1c3f2e8b904 (cash_collected) and bfefce47f6ac (call2_calendar_id)

Both branches descended from 47ff03aec99f, creating two heads. This merge
migration resolves the fork so `alembic upgrade head` works again.

Revision ID: merge_001
Revises: a1c3f2e8b904, bfefce47f6ac
Create Date: 2026-04-06
"""

from alembic import op

revision = 'merge_001'
down_revision = ('a1c3f2e8b904', 'bfefce47f6ac')
branch_labels = None
depends_on = None


def upgrade():
    pass  # No schema changes — this is a merge point only


def downgrade():
    pass
