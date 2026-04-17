"""Add pipeline_id column to opportunities — enables multi-pipeline syncing.

Revision ID: upsell001
Revises: fe001
Create Date: 2026-04-15
"""

from alembic import op

revision = 'upsell001'
down_revision = 'slwa001'
branch_labels = None
depends_on = None

SALES_PIPELINE_ID = "zbI8YxmB9qhk1h4cInnq"


def upgrade() -> None:
    # Add column — idempotent
    op.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS pipeline_id TEXT")
    # Backfill all existing rows (sales pipeline only)
    op.execute(f"UPDATE opportunities SET pipeline_id = '{SALES_PIPELINE_ID}' WHERE pipeline_id IS NULL")
    # Index for fast per-pipeline queries
    op.execute("CREATE INDEX IF NOT EXISTS ix_opportunities_pipeline_id ON opportunities (pipeline_id)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_opportunities_pipeline_id")
    op.execute("ALTER TABLE opportunities DROP COLUMN IF EXISTS pipeline_id")


# ── PLAIN ENGLISH ─────────────────────────────────────────────────────────────
# This migration adds a "which pipeline does this deal belong to?" column to
# every opportunity row. Before this column existed, the DB had no way to tell
# a sales-pipeline deal from an upsell-pipeline deal — they'd all look the same.
# Existing rows get backfilled with the Sales pipeline ID so nothing breaks.
# Future syncs write their own pipeline_id at insert time.
#
# Chain: slwa001 → upsell001 (this file is the current head)
#
# ── VERIFICATION ──────────────────────────────────────────────────────────────
# Step 1 — confirm the column exists and is populated:
#
#   SELECT pipeline_id, COUNT(*)
#   FROM opportunities
#   GROUP BY pipeline_id;
#
# Expected BEFORE first upsell sync:
#   pipeline_id                           | count
#   zbI8YxmB9qhk1h4cInnq (sales)         | <your total opp count, e.g. 3243>
#
# Expected AFTER first upsell sync:
#   pipeline_id                           | count
#   zbI8YxmB9qhk1h4cInnq (sales)         | <sales count>
#   NjidsHukHHUpYtTcQefX (upsell)        | <upsell count, should be > 0>
#
# Step 2 — confirm the index exists (needed for fast upsell tab queries):
#
#   SELECT indexname FROM pg_indexes
#   WHERE tablename = 'opportunities'
#   AND indexname = 'ix_opportunities_pipeline_id';
#
# Expected: one row returned. Zero rows = index missing = upsell tab will be slow.
#
# ── SILENT FAILURE SIGNALS ────────────────────────────────────────────────────
# If the Upsells tab shows 0 opps after you run a sync:
#   SELECT COUNT(*) FROM opportunities WHERE pipeline_id = 'NjidsHukHHUpYtTcQefX';
#   → Zero = sync ran but didn't write pipeline_id for upsell rows.
#     Check sync_engine.py: pipeline_id must be in the row dict returned by _build_opportunity_row.
#
# If ALL opportunities show 0 pipeline_id (NULL):
#   SELECT COUNT(*) FROM opportunities WHERE pipeline_id IS NULL;
#   → Non-zero = backfill didn't run OR migration ran before the column was added.
#     Fix: run the UPDATE manually:
#     UPDATE opportunities SET pipeline_id = 'zbI8YxmB9qhk1h4cInnq' WHERE pipeline_id IS NULL;
