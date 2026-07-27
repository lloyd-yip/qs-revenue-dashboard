"""add appointwise_events — webhook events from Appointwise agents (per-agent attribution)

Appointwise has no read-API, so the only source of which-agent-handled-which-contact is a
Webhook Node pushing events to /api/webhooks/appointwise. Additive + reversible.

Revision ID: awh001
Revises: sms001
Create Date: 2026-07-27

"""
from typing import Sequence, Union

from alembic import op


revision: str = 'awh001'
down_revision: Union[str, Sequence[str], None] = 'sms001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS appointwise_events (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            event_type TEXT,
            agent_name TEXT,
            agent_id TEXT,
            ghl_contact_id TEXT,
            phone TEXT,
            email TEXT,
            raw JSONB
        );
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_appointwise_events_received ON appointwise_events (received_at)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_appointwise_events_agent ON appointwise_events (agent_name)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_appointwise_events_contact ON appointwise_events (ghl_contact_id)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS appointwise_events")
