"""Add deal_whop_matches table — GHL ↔ Whop deal reconciliation.

Revision ID: dealmatches001
Revises: settings001
Create Date: 2026-05-12

── WHAT THIS TABLE IS ──────────────────────────────────────────────────────
Each row = one GHL closed-won deal, matched (or attempted) to a Whop membership.

The ghl_opportunity_id UNIQUE constraint is the idempotency gate:
  - Re-running the matching engine updates the row (via upsert) UNLESS
    is_confirmed=True, in which case the match is locked forever.
  - Manual matches set is_confirmed=True and are never overwritten by
    the auto-matcher.

── PAYMENT METRIC FIELDS ───────────────────────────────────────────────────
  upfront_cash:       GHL rep-entered cash_collected (directional)
  total_paid:         Sum of Whop payments with status='paid'
  total_contract_value: GHL monetary_value (contract value from CRM)
  remaining_ar:       total_contract_value - total_paid
  is_financing:       True if multiple Whop payments exist + remaining AR > 0

── VERIFICATION (run these in Supabase SQL editor after deploying) ──────────

1. Confirm migration recorded:
   SELECT version_num FROM alembic_version WHERE version_num = 'dealmatches001';
   → Must return exactly 1 row. If 0 rows, the migration didn't run — check
     Railway deploy logs for alembic errors.

2. Confirm table shape (27 columns expected):
   SELECT column_name, data_type, is_nullable
   FROM information_schema.columns
   WHERE table_name = 'deal_whop_matches'
   ORDER BY ordinal_position;
   → Key columns to spot-check:
     ghl_opportunity_id  | character varying | NO   (this is the idempotency key)
     match_confidence    | character varying | NO   (default 'unmatched')
     is_confirmed        | boolean           | NO   (default false)
     total_paid          | numeric           | YES  (null until Whop payments fetched)
     remaining_ar        | numeric           | YES  (null until match computed)

3. Confirm unique constraint exists (the idempotency gate):
   SELECT constraint_name, constraint_type
   FROM information_schema.table_constraints
   WHERE table_name = 'deal_whop_matches';
   → Should see one UNIQUE constraint on ghl_opportunity_id.

4. After running POST /api/dashboard/deals/run-match, spot-check a known deal:
   SELECT ghl_opportunity_name, ghl_owner_name, ghl_close_date,
          whop_email, match_confidence, match_score, total_paid,
          total_contract_value, remaining_ar
   FROM deal_whop_matches
   WHERE match_confidence = 'high'
   ORDER BY ghl_close_date DESC LIMIT 5;
   → High-confidence rows should have whop_email populated and total_paid > 0.

── SILENT FAILURE SIGNAL ───────────────────────────────────────────────────
• /api/dashboard/deals/run-match returns {"matched": 0, "unmatched": 0} → the
  table doesn't exist yet. Look for "relation deal_whop_matches does not exist"
  in Railway deploy logs.
• All rows show match_confidence = 'unmatched' after run → Whop API key is
  missing or wrong. Check Railway env var WHOP_API_KEY.
• ghl_contact_email is NULL on all rows → GHL contact fetch is failing silently.
  Check Railway logs for "Failed to fetch contact" warnings.

── PLAIN ENGLISH ────────────────────────────────────────────────────────────
One row = one GHL closed-won deal. When the matcher runs, it tries to find the
matching Whop membership (the customer's purchase). The UNIQUE constraint on
ghl_opportunity_id means running the matcher twice gives the same result as
running it once — it updates, never duplicates. The is_confirmed flag is the
"lock" — once you manually confirm a match, the auto-matcher will never touch
that row again even if it runs again.
"""

from alembic import op

revision = "dealmatches001"
down_revision = "settings001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS deal_whop_matches (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

            -- GHL deal (idempotency key — one row per GHL opportunity)
            ghl_opportunity_id VARCHAR(100) NOT NULL,

            -- GHL deal data (denormalized for display — avoid joins)
            ghl_close_date DATE,
            ghl_opportunity_name VARCHAR(300),
            ghl_owner_name VARCHAR(150),
            ghl_contact_id VARCHAR(100),
            ghl_contact_email VARCHAR(255),
            ghl_contact_name VARCHAR(150),
            ghl_monetary_value NUMERIC(12, 2),
            ghl_cash_collected NUMERIC(12, 2),

            -- Whop match data (NULL if unmatched)
            whop_membership_id VARCHAR(100),
            whop_email VARCHAR(255),
            whop_name VARCHAR(150),
            whop_product_id VARCHAR(100),
            whop_plan_name VARCHAR(150),
            whop_created_at TIMESTAMPTZ,

            -- Match quality
            match_confidence VARCHAR(20) NOT NULL DEFAULT 'unmatched',
            match_score NUMERIC(5, 3) NOT NULL DEFAULT 0,
            match_method VARCHAR(100) NOT NULL DEFAULT 'none',

            -- Manual override / idempotency gate
            -- Once is_confirmed=TRUE, the auto-matcher NEVER touches this row
            is_confirmed BOOLEAN NOT NULL DEFAULT FALSE,
            confirmed_by VARCHAR(100),
            confirmed_at TIMESTAMPTZ,

            -- Computed payment metrics (from Whop payments API)
            upfront_cash NUMERIC(12, 2),
            total_paid NUMERIC(12, 2),
            total_contract_value NUMERIC(12, 2),
            remaining_ar NUMERIC(12, 2),
            is_financing BOOLEAN,
            payment_count INTEGER,

            -- Operational timestamps
            matched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            metrics_updated_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

            UNIQUE (ghl_opportunity_id)
        )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_deal_whop_matches_ghl_close_date
        ON deal_whop_matches (ghl_close_date)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_deal_whop_matches_ghl_owner_name
        ON deal_whop_matches (ghl_owner_name)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_deal_whop_matches_match_confidence
        ON deal_whop_matches (match_confidence)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_deal_whop_matches_is_confirmed
        ON deal_whop_matches (is_confirmed)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS deal_whop_matches")
