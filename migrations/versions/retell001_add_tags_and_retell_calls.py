"""add contact tags/email/phone to opportunities + retell_calls table

Part 2 of the Retell/Appointwise work:
- opportunities.contact_tags / contact_email / contact_phone — snapshot of the GHL contact
  (already fetched + cached per sales opp) to drive tag-first AI-channel classification,
  phone→Retell matching, and internal-domain exclusion.
- retell_calls — AI voice calls pulled from the Retell API (external to GHL), matched to a
  GHL contact by phone. Sole source of call volume/minutes/recordings.

Additive + reversible.

Revision ID: retell001
Revises: chcost001
Create Date: 2026-07-27

"""
from typing import Sequence, Union

from alembic import op


revision: str = 'retell001'
down_revision: Union[str, Sequence[str], None] = 'chcost001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS contact_tags TEXT[]")
    op.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS contact_email TEXT")
    op.execute("ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS contact_phone TEXT")
    op.execute("CREATE INDEX IF NOT EXISTS ix_opportunities_contact_phone ON opportunities (contact_phone)")

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS retell_calls (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            retell_call_id TEXT UNIQUE NOT NULL,
            agent_id TEXT,
            direction TEXT,
            from_number TEXT,
            to_number TEXT,
            started_at TIMESTAMPTZ,
            duration_sec INTEGER,
            call_status TEXT,
            disconnect_reason TEXT,
            recording_url TEXT,
            transcript TEXT,
            analysis JSONB,
            ghl_contact_id TEXT,
            ghl_contact_name TEXT,
            match_method TEXT,
            match_confidence TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_retell_calls_to_number ON retell_calls (to_number)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_retell_calls_ghl_contact_id ON retell_calls (ghl_contact_id)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_retell_calls_started_at ON retell_calls (started_at)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_retell_calls_agent_id ON retell_calls (agent_id)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS retell_calls")
    op.execute("DROP INDEX IF EXISTS ix_opportunities_contact_phone")
    op.execute("ALTER TABLE opportunities DROP COLUMN IF EXISTS contact_phone")
    op.execute("ALTER TABLE opportunities DROP COLUMN IF EXISTS contact_email")
    op.execute("ALTER TABLE opportunities DROP COLUMN IF EXISTS contact_tags")
