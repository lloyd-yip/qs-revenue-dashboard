"""Add outcome_unfilled and post_call_note_word_count columns

Revision ID: 002
Revises: 001
Create Date: 2026-03-20

outcome_unfilled: broader replacement signal for rep_compliance_failure.
  - Fires when appointment passed + 12h grace + status still Confirmed/NULL
  - No stage restriction (catches all stages, not just Upcoming)
  - Used as the show rate denominator exclusion

post_call_note_word_count: word count of rep's own post-call note, fetched
  from the GHL contacts notes API during sync.
  - NULL  = notes check not applicable (no-show / cancelled / appointment not yet past)
  - 0     = showed, no qualifying rep note found within 12h window
  - N     = word count of the longest qualifying rep note found
"""
from alembic import op
import sqlalchemy as sa


revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "opportunities",
        sa.Column(
            "outcome_unfilled",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
    )
    op.add_column(
        "opportunities",
        sa.Column(
            "post_call_note_word_count",
            sa.Integer(),
            nullable=True,
        ),
    )
    op.create_index(
        "ix_opportunities_outcome_unfilled",
        "opportunities",
        ["outcome_unfilled"],
    )


def downgrade() -> None:
    op.drop_index("ix_opportunities_outcome_unfilled", table_name="opportunities")
    op.drop_column("opportunities", "post_call_note_word_count")
    op.drop_column("opportunities", "outcome_unfilled")
