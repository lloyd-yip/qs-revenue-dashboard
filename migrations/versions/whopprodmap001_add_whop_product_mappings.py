"""Add whop_product_mappings — one row per Whop product with an editable CATEGORY
that decides whether the product's payments count as deal/coaching revenue.

Revision ID: whopprodmap001
Revises: dealrefunddate001
Create Date: 2026-08-06

Plain English:
  Replaces the hardcoded "excluded product" name-patterns with a real, user-editable
  mapping (the Deals › Products tab). Every Whop product is upserted here by the
  matcher with an auto-suggested category; Lloyd can override it. Category meaning:
    • normal_deal — coaching / split-payment revenue (COUNTS)
    • upsell      — upsell revenue (COUNTS; bucketed separately for reporting)
    • hermes      — Calendar Automation (EXCLUDED from deal metrics)
    • ignore      — one-off offers e.g. OpenClaw workshop (EXCLUDED)
  is_manual=TRUE means a human set the category, so the auto-mapper never overwrites it.

VERIFICATION — run in Supabase SQL editor after apply:
    SELECT category, count(*) FROM whop_product_mappings GROUP BY category;
"""

from alembic import op


revision = "whopprodmap001"
down_revision = "dealrefunddate001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS whop_product_mappings (
            product_id        VARCHAR(100) PRIMARY KEY,
            product_name      VARCHAR(300),
            price_display     VARCHAR(100),
            all_time_revenue  NUMERIC(14, 2),
            active_users      INTEGER,
            category          VARCHAR(30)  NOT NULL DEFAULT 'normal_deal',
            auto_category     VARCHAR(30),
            is_manual         BOOLEAN      NOT NULL DEFAULT FALSE,
            first_seen_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
            updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_whop_product_mappings_category
        ON whop_product_mappings (category)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS whop_product_mappings")
