"""Add stripe_charges table for live Stripe cash inflow.

Revision ID: stripecharges001
Revises: whopprodmap001
Create Date: 2026-08-06

Plain English: stores every succeeded Stripe charge (no amount floor — unlike the
deal-matching pass, this INCLUDES the GHL sub-account subscriptions and commissions
the CEO wants counted). One row per charge, idempotent on stripe_charge_id. The
Company dashboard sums these per month into the live "Stripe" cash-inflow channel.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "stripecharges001"
down_revision = "whopprodmap001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "stripe_charges",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("stripe_charge_id", sa.String(100), unique=True, nullable=False),
        sa.Column("amount", sa.Numeric(12, 2), nullable=False, server_default="0"),           # gross, major units
        sa.Column("refunded_amount", sa.Numeric(12, 2), nullable=False, server_default="0"),   # major units
        sa.Column("currency", sa.String(10), nullable=False, server_default="usd"),
        sa.Column("created", sa.Date(), nullable=True),                                        # charge date
        sa.Column("status", sa.String(30), nullable=True),
        sa.Column("customer_id", sa.String(100), nullable=True),
        sa.Column("customer_email", sa.String(300), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("payment_intent", sa.String(100), nullable=True),
        sa.Column("synced_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )
    op.create_index("ix_stripe_charges_stripe_charge_id", "stripe_charges", ["stripe_charge_id"])
    op.create_index("ix_stripe_charges_created", "stripe_charges", ["created"])


def downgrade() -> None:
    op.drop_table("stripe_charges")
