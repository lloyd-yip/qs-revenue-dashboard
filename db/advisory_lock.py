"""Postgres advisory locks — single-scheduler election + atomic sync guard.

Two problems need coordination that a single app process can't provide alone:

1. SCHEDULER LEADER (best-effort). Multiple app processes can coexist — a Railway
   deploy overlaps old+new, a container can outlive its deploy, or the service may
   run >1 replica. If each started its own APScheduler they'd each fire the
   incremental sync on its own boot schedule: the exact cause of the duplicate
   hourly syncs this module was written to kill. `try_acquire_scheduler_leadership`
   elects one scheduler process via a *session-level* advisory lock held on a
   dedicated connection for the process lifetime.

   Caveat: a session-level lock only holds on a stable session. Behind a
   transaction-mode pooler (e.g. Supabase's pgbouncer on 6543) the lock can't be
   held, so leadership degrades to "everyone is leader". That's why leadership is
   only an OPTIMISATION — the atomic guard in (2) is what actually prevents
   overlapping syncs, and it works regardless of pooling mode.

2. SYNC GUARD (correctness). The run_sync concurrency check (SELECT running rows,
   then INSERT a new one) is not atomic across connections. Wrapping it in a
   *transaction-level* advisory lock (`pg_advisory_xact_lock(SYNC_GUARD_LOCK_KEY)`)
   serialises it so two callers can never both create a 'running' row. Transaction
   locks are acquired and released within one transaction (one pooler checkout), so
   they are fully reliable even under transaction pooling.
"""

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from db.session import engine

logger = logging.getLogger(__name__)

# Arbitrary but FIXED keys — must be stable across deploys and unique per purpose.
# (bigint, single-argument advisory-lock form.)
SCHEDULER_LEADER_LOCK_KEY = 728_455_001
SYNC_GUARD_LOCK_KEY = 728_455_002


async def try_acquire_scheduler_leadership() -> AsyncConnection | None:
    """Try to become the sole scheduler process.

    On success returns the dedicated OPEN connection holding the session-level lock —
    keep it open for the process lifetime; closing it (on shutdown) releases the lock
    so the next process can take over. Returns None if another process already holds
    the lock (this process should then run without a scheduler).
    """
    conn = await engine.connect()
    try:
        acquired = (
            await conn.execute(
                text("SELECT pg_try_advisory_lock(:k)"),
                {"k": SCHEDULER_LEADER_LOCK_KEY},
            )
        ).scalar_one()
        # End the transaction so the connection isn't left idle-in-transaction; a
        # session-level advisory lock survives commit (it releases only on session
        # end / explicit unlock), so leadership is retained on this held connection.
        await conn.commit()
    except Exception:
        await conn.close()
        raise

    if acquired:
        return conn
    await conn.close()
    return None
