"""Add rep_comp_settings table — per-rep base salary + commission % config.

Revision ID: repcomp001
Revises: f1funnel002
Create Date: 2026-07-13

Plain English: This creates one settings row per sales rep holding their comp
model (monthly base salary + commission %). The Sales dashboard derives rep cost
from these instead of reading Xero payouts — payouts are cash-basis and lag
split-payment deals by months, so they attribute cost to the wrong period.
Reps with no row fall back to base $0 / commission 10% (the current default),
so no seed data is required.

VERIFICATION — run this in Supabase SQL editor after deploy:
    SELECT table_name FROM information_schema.tables
    WHERE table_name = 'rep_comp_settings';
    -- Should return: rep_comp_settings

    SELECT version_num FROM alembic_version;
    -- Should include 'repcomp001'.
"""

from alembic import op

revision = "repcomp001"
down_revision = "f1funnel002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS rep_comp_settings (
            rep_id              VARCHAR       PRIMARY KEY,
            rep_name            VARCHAR       NOT NULL,
            base_salary_monthly NUMERIC(12,2) NOT NULL DEFAULT 0,
            commission_pct      NUMERIC(5,2)  NOT NULL DEFAULT 10,
            created_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
            updated_at          TIMESTAMPTZ   NOT NULL DEFAULT now()
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS rep_comp_settings")
