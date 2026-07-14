"""Add sync_runs.checkpoint — resumable sync state.

Revision ID: syncresume001
Revises: repcomp001
Create Date: 2026-07-14

Plain English: every deploy restarts the app and killed any in-flight sync,
which then had to start over from page one. The engine now saves its position
(GHL pagination cursor, seen ids, counters) into this column after every page;
on boot the app finds a fresh 'running' run with a checkpoint and continues it
where it left off.

VERIFICATION — run in Supabase SQL editor after deploy:
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'sync_runs' AND column_name = 'checkpoint';
    -- Should return: checkpoint
"""

from alembic import op

revision = "syncresume001"
down_revision = "repcomp001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE sync_runs ADD COLUMN IF NOT EXISTS checkpoint jsonb")


def downgrade() -> None:
    op.execute("ALTER TABLE sync_runs DROP COLUMN IF EXISTS checkpoint")
