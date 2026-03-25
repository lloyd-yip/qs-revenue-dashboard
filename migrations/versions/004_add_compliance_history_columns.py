"""Add compliance history timestamp columns to opportunities

Revision ID: 004
Revises: 003
Create Date: 2026-03-25

outcome_unfilled_first_flagged_at: when this opp was first detected as outcome_unfilled=True.
  Never cleared — records that the rep was non-compliant at some point.

outcome_unfilled_resolved_at: when outcome_unfilled transitioned from True → False.
  Populated on the sync run that detects the rep fixed it.

Together these let us compute: how many hours after the appointment did the rep finally
log the outcome? Used for the "late rate" compliance KPI per rep.
"""
from alembic import op
import sqlalchemy as sa


revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "opportunities",
        sa.Column("outcome_unfilled_first_flagged_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "opportunities",
        sa.Column("outcome_unfilled_resolved_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("opportunities", "outcome_unfilled_resolved_at")
    op.drop_column("opportunities", "outcome_unfilled_first_flagged_at")
