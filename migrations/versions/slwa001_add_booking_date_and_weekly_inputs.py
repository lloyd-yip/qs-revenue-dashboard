"""Add call1 booking date and SLWA weekly manual inputs.

Revision ID: slwa001
Revises: fe001
Create Date: 2026-04-07
"""

from alembic import op

revision = "slwa001"
down_revision = "fe001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE opportunities
        ADD COLUMN IF NOT EXISTS call1_booking_date TIMESTAMPTZ
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_opportunities_call1_booking_date
        ON opportunities (call1_booking_date)
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS slwa_weekly_inputs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            channel_key TEXT NOT NULL,
            section TEXT NOT NULL,
            week_start DATE NOT NULL,
            message_sent NUMERIC(12, 2),
            links_sent NUMERIC(12, 2),
            changes_to_funnel TEXT,
            copy TEXT,
            groups TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (channel_key, section, week_start)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_slwa_weekly_inputs_channel_key
        ON slwa_weekly_inputs (channel_key)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_slwa_weekly_inputs_section
        ON slwa_weekly_inputs (section)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_slwa_weekly_inputs_week_start
        ON slwa_weekly_inputs (week_start)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS slwa_weekly_inputs")
    op.execute("DROP INDEX IF EXISTS ix_opportunities_call1_booking_date")
    op.execute("ALTER TABLE opportunities DROP COLUMN IF EXISTS call1_booking_date")
