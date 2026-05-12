"""Add is_primary_funnel flag to source_normalization — marks channels that belong to
the primary webinar invite funnel. Enables cost card auto-population from Xero data.

Revision ID: funnel001
Revises: dealmatches001
Create Date: 2026-05-12

Plain English: Adds a true/false column to the source channel mapping table.
Any channel marked TRUE feeds the 4 cost cards on the Funnel Economics tab
(Cost/Call Booked, Cost/Show, Cost/Qual Show, Cost/Acquisition).
"Webinar Live" is the only primary funnel channel right now — experiment channels
(Meta Ads, Slack, Unknown, etc.) are excluded from cost calculations until proven.

VERIFICATION — run in Supabase SQL editor after deploy:
    -- Confirm the column exists:
    SELECT column_name, data_type, column_default
    FROM information_schema.columns
    WHERE table_name = 'source_normalization' AND column_name = 'is_primary_funnel';
    -- Should return: is_primary_funnel | boolean | false

    -- Confirm Webinar Live is flagged:
    SELECT canonical_channel, is_primary_funnel
    FROM source_normalization
    WHERE is_primary_funnel = true;
    -- Should return: Webinar Live | true
    -- (plus any other rows mapped to "Webinar Live")

    -- Confirm Alembic tracking:
    SELECT version_num FROM alembic_version;
    -- Should include 'funnel001'

SILENT FAILURE SIGNAL: If the cost cards on Funnel Economics still show "—" after
this migration, check that source_normalization has rows with canonical_channel =
'Webinar Live'. If the table is empty, no opportunities are being resolved to that
channel (check the normalizer + opportunity sync logs).
"""

from alembic import op


revision = "funnel001"
down_revision = "dealmatches001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add the column — default FALSE means all existing channels start as non-primary
    op.execute("""
        ALTER TABLE source_normalization
        ADD COLUMN IF NOT EXISTS is_primary_funnel BOOLEAN NOT NULL DEFAULT FALSE
    """)

    # Flag all rows that resolve to "Webinar Live" as the primary funnel
    op.execute("""
        UPDATE source_normalization
        SET is_primary_funnel = TRUE
        WHERE canonical_channel = 'Webinar Live'
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE source_normalization
        DROP COLUMN IF EXISTS is_primary_funnel
    """)
