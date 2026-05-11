"""Add app_settings table for persistent key-value config (e.g. Xero refresh token).

Revision ID: settings001
Revises: revenue001
Create Date: 2026-05-11

Plain English: This creates a small "settings drawer" in the database — a simple
table with two columns (key, value) that persists config across Railway restarts.
We use it to store the Xero refresh token once, so the app can call Xero forever
without you having to log in again.

VERIFICATION — run this in Supabase SQL editor after deploy:
    SELECT key, updated_at FROM app_settings;
    -- Should return 0 rows (empty until OAuth completes).
    -- After you click Approve in Xero, run again — should show 1 row with key='xero_refresh_token'.

    -- Confirm the table exists at all:
    SELECT table_name FROM information_schema.tables
    WHERE table_name = 'app_settings';
    -- Should return: app_settings

    -- Confirm the migration ran in Alembic's tracking table:
    SELECT version_num FROM alembic_version;
    -- Should include 'settings001' in the result.

SILENT FAILURE SIGNAL: If /xero/callback returns a 500 error after Xero approval,
the migration likely didn't run (table doesn't exist). Check Railway deploy logs for
"alembic upgrade head" — it should complete without errors before uvicorn starts.
"""

from alembic import op

revision = "settings001"
down_revision = "revenue001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            key         VARCHAR(100) PRIMARY KEY,
            value       TEXT        NOT NULL,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app_settings")
