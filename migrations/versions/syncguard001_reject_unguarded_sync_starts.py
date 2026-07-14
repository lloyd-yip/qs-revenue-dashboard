"""DB-level sync guard: reject sync starts from processes that don't hold the guard lock.

Revision ID: syncguard001
Revises: syncresume001
Create Date: 2026-07-15

Plain English: orphaned OLD-code app processes (zombie containers / stray hosts
we cannot locate — the Supabase pooler masks client IPs) kept starting hourly
incremental syncs against this database, surviving every deploy and restart of
the known service. No app-side fix can reach a process running old code, but the
database is the one chokepoint every copy of the app must pass through.

Current code (db/advisory_lock.SYNC_GUARD_LOCK_KEY) takes
pg_advisory_xact_lock(728455002) in the same transaction that INSERTs the
sync_runs row. This trigger rejects any INSERT of a status='running' row whose
backend does NOT hold that lock — so outdated processes get an exception at
run-creation time (before any GHL API work) and can no longer start syncs,
no matter where they run. UPDATEs are untouched: in-flight old runs can still
close out their status.

EMERGENCY DISABLE (Supabase SQL editor):
    DROP TRIGGER IF EXISTS sync_runs_guard_lock ON sync_runs;

VERIFICATION — this should ERROR with 'sync start rejected':
    INSERT INTO sync_runs (id, sync_type, started_at, status, opportunities_synced, errors_count)
    VALUES (gen_random_uuid(), 'incremental', now(), 'running', 0, 0);
"""

from alembic import op

revision = "syncguard001"
down_revision = "syncresume001"
branch_labels = None
depends_on = None

# Must match db/advisory_lock.SYNC_GUARD_LOCK_KEY (bigint form: classid=0, objid=key)
_GUARD_KEY = 728_455_002


def upgrade() -> None:
    op.execute(f"""
        CREATE OR REPLACE FUNCTION sync_runs_require_guard_lock() RETURNS trigger AS $fn$
        BEGIN
          IF NEW.status = 'running' AND NOT EXISTS (
            SELECT 1 FROM pg_locks
            WHERE locktype = 'advisory'
              AND classid = 0
              AND objid = {_GUARD_KEY}
              AND granted
              AND pid = pg_backend_pid()
          ) THEN
            RAISE EXCEPTION 'sync start rejected: this process does not hold the sync-guard advisory lock'
              USING HINT = 'Outdated app process — only current code (db/advisory_lock.py, pg_advisory_xact_lock) may start syncs.';
          END IF;
          RETURN NEW;
        END
        $fn$ LANGUAGE plpgsql;
    """)
    op.execute("DROP TRIGGER IF EXISTS sync_runs_guard_lock ON sync_runs")
    op.execute("""
        CREATE TRIGGER sync_runs_guard_lock
          BEFORE INSERT ON sync_runs
          FOR EACH ROW EXECUTE FUNCTION sync_runs_require_guard_lock()
    """)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS sync_runs_guard_lock ON sync_runs")
    op.execute("DROP FUNCTION IF EXISTS sync_runs_require_guard_lock()")
