"""Add whop_stats_snapshots for stored MRR/ARR.

Revision ID: whopstats001
Revises: stripecharges001
Create Date: 2026-08-06

Plain English: MRR/ARR aren't in our DB (Whop computes them). This table stores a
dated snapshot each time we sync from Whop, so the Company dashboard can chart MRR/ARR
month by month without hammering the Whop API on every page load. One row per date.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "whopstats001"
down_revision = "stripecharges001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "whop_stats_snapshots",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("snapshot_date", sa.Date(), unique=True, nullable=False),
        sa.Column("mrr", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("arr", sa.Numeric(14, 2), nullable=False, server_default="0"),
        sa.Column("active_members", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("currency", sa.String(10), nullable=False, server_default="usd"),
        sa.Column("source", sa.String(40), nullable=False, server_default="whop_memberships"),
        sa.Column("synced_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )
    op.create_index("ix_whop_stats_snapshots_snapshot_date", "whop_stats_snapshots", ["snapshot_date"])


def downgrade() -> None:
    op.drop_table("whop_stats_snapshots")
