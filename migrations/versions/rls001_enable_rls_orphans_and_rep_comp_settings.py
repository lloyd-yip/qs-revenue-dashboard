"""Enable Row-Level Security on the two public tables that were missing it.

Revision ID: rls001
Revises: whoporphan001
Create Date: 2026-07-22

Plain English: Supabase auto-exposes every table in the `public` schema through
its PostgREST Data API, guarded only by the public `anon` key. Any such table
WITHOUT row-level security is readable/writable by anyone who has that key —
which is exactly what Supabase's security advisor flagged (`rls_disabled_in_public`,
ERROR) for `whop_orphan_payments` and `rep_comp_settings`. Both are recent tables
where RLS was never turned on; every other table in this schema already has it.

Enabling RLS with NO policies = deny-all for the anon/authenticated API roles
(closes the hole). It does NOT affect this app: the backend connects as the
`postgres` owner role (see db/session.py + config.database_url), which bypasses
RLS entirely, and the frontend never talks to Supabase directly.

This migration mirrors a change already applied live to production on 2026-07-22
so the schema stays reproducible from source and cannot regress on a rebuild.
Re-running the ENABLE is idempotent, so applying this against the already-fixed
prod DB is a no-op.

VERIFICATION (Supabase SQL editor / psql) — both should report relrowsecurity = true:
    SELECT relname, relrowsecurity FROM pg_class
    WHERE relname IN ('whop_orphan_payments', 'rep_comp_settings');
"""

from alembic import op

revision = "rls001"
down_revision = "whoporphan001"
branch_labels = None
depends_on = None

_TABLES = ("whop_orphan_payments", "rep_comp_settings")


def upgrade() -> None:
    for table in _TABLES:
        op.execute(f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY")


def downgrade() -> None:
    for table in _TABLES:
        op.execute(f"ALTER TABLE public.{table} DISABLE ROW LEVEL SECURITY")
