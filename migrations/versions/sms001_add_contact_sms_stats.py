"""add contact_sms_stats — per-contact SMS engagement from GHL Conversations

Powers the Appointwise SMS funnel (messaged → replied → booked, avg msgs/lead,
reply-by-message-#, response timing, opt-outs). Computed during the main GHL sync for
Appointwise-tagged contacts. Additive + reversible.

Revision ID: sms001
Revises: retellcost001
Create Date: 2026-07-27

"""
from typing import Sequence, Union

from alembic import op


revision: str = 'sms001'
down_revision: Union[str, Sequence[str], None] = 'retellcost001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS contact_sms_stats (
            ghl_contact_id TEXT PRIMARY KEY,
            outbound_count INTEGER NOT NULL DEFAULT 0,
            inbound_count INTEGER NOT NULL DEFAULT 0,
            first_outbound_at TIMESTAMPTZ,
            first_inbound_at TIMESTAMPTZ,
            last_message_at TIMESTAMPTZ,
            reply_after_n INTEGER,
            opted_out BOOLEAN NOT NULL DEFAULT FALSE,
            opt_out_at TIMESTAMPTZ,
            opt_out_after_n INTEGER,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS ix_contact_sms_stats_first_inbound ON contact_sms_stats (first_inbound_at)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_contact_sms_stats_first_inbound")
    op.execute("DROP TABLE IF EXISTS contact_sms_stats")
