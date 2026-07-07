"""seed Referral channel + unambiguous source aliases

Adds the Referral channel (per Lloyd 2026-07-07) and the clearly-attributable raw
source aliases that were previously falling into 'Unknown' (audit F7). Ambiguous
values (email, call, fb, ig, calendar) are intentionally left out — they need a
paid-vs-organic naming decision before mapping.

Revision ID: f1funnel002
Revises: f1funnel001
Create Date: 2026-07-07

"""
from typing import Sequence, Union

from alembic import op


revision: str = 'f1funnel002'
down_revision: Union[str, Sequence[str], None] = 'f1funnel001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# (raw_value, canonical_channel) — raw_value is unique; existing rows are left untouched.
_SEED = [
    ("referral", "Referral"),
    ("li", "LinkedIn"),
    ("ai-caller", "AI Bot"),
    ("wa-bot", "AI Bot"),
    ("ai-chat", "AI Bot"),
    ("sms calendly", "Slack / WhatsApp / SMS"),
    ("slack calendly", "Slack / WhatsApp / SMS"),
]


def upgrade() -> None:
    values = ", ".join(
        f"('{raw}', '{chan}', FALSE)" for raw, chan in _SEED
    )
    op.execute(
        f"""
        INSERT INTO source_normalization (raw_value, canonical_channel, is_primary_funnel)
        VALUES {values}
        ON CONFLICT (raw_value) DO NOTHING;
        """
    )


def downgrade() -> None:
    raws = ", ".join(f"'{raw}'" for raw, _ in _SEED)
    op.execute(f"DELETE FROM source_normalization WHERE raw_value IN ({raws});")
