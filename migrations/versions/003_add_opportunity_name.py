"""Add opportunity_name column to opportunities

Revision ID: 003
Revises: 002
Create Date: 2026-03-20

opportunity_name: the GHL opportunity name (usually the lead/contact name).
  Used for the channel closes drill-down popup.
"""
from alembic import op
import sqlalchemy as sa


revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "opportunities",
        sa.Column("opportunity_name", sa.String(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("opportunities", "opportunity_name")
