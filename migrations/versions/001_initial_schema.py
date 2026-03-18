"""initial schema

Revision ID: 001
Revises:
Create Date: 2026-03-18

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSON

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- opportunities ---
    op.create_table(
        "opportunities",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("ghl_opportunity_id", sa.String(), nullable=False),
        sa.Column("ghl_contact_id", sa.String(), nullable=True),
        # Stage
        sa.Column("pipeline_stage_id", sa.String(), nullable=True),
        sa.Column("pipeline_stage_name", sa.String(), nullable=True),
        sa.Column("is_excluded", sa.Boolean(), nullable=False, server_default="false"),
        # Rep attribution
        sa.Column("opportunity_owner_id", sa.String(), nullable=True),
        sa.Column("opportunity_owner_name", sa.String(), nullable=True),
        # Deal value
        sa.Column("monetary_value", sa.Numeric(12, 2), nullable=True),
        # Per-call status
        sa.Column("call1_appointment_status", sa.String(), nullable=True),
        sa.Column("call2_appointment_status", sa.String(), nullable=True),
        sa.Column("call1_appointment_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("call2_appointment_date", sa.DateTime(timezone=True), nullable=True),
        # Qualification fields
        sa.Column("lead_quality", sa.String(), nullable=True),
        sa.Column("financial_qual", sa.String(), nullable=True),
        sa.Column("intent_to_transform", sa.String(), nullable=True),
        sa.Column("pre_call_indoctrination", sa.String(), nullable=True),
        sa.Column("business_fit", sa.String(), nullable=True),
        sa.Column("pain_goal_oriented", sa.String(), nullable=True),
        sa.Column("dq_reason", sa.String(), nullable=True),
        sa.Column("deal_lost_reasons", sa.String(), nullable=True),
        # Firmographic
        sa.Column("business_industry", sa.String(), nullable=True),
        sa.Column("current_revenue", sa.String(), nullable=True),
        # Attribution — first touch
        sa.Column("attr_first_utm_source", sa.String(), nullable=True),
        sa.Column("attr_first_utm_medium", sa.String(), nullable=True),
        sa.Column("attr_first_utm_campaign", sa.String(), nullable=True),
        # Attribution — last touch
        sa.Column("attr_last_utm_source", sa.String(), nullable=True),
        sa.Column("attr_last_utm_medium", sa.String(), nullable=True),
        sa.Column("attr_last_utm_campaign", sa.String(), nullable=True),
        # Booking-time attribution custom fields
        sa.Column("op_book_campaign_source", sa.String(), nullable=True),
        sa.Column("op_book_campaign_medium", sa.String(), nullable=True),
        sa.Column("op_book_campaign_name", sa.String(), nullable=True),
        # Normalized channel
        sa.Column("canonical_channel", sa.String(), nullable=True),
        # Compliance flag
        sa.Column("rep_compliance_failure", sa.Boolean(), nullable=False, server_default="false"),
        # GHL timestamps
        sa.Column("created_at_ghl", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at_ghl", sa.DateTime(timezone=True), nullable=False),
        # Sync metadata
        sa.Column("synced_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
    )
    op.create_index("ix_opportunities_ghl_opportunity_id", "opportunities", ["ghl_opportunity_id"], unique=True)
    op.create_index("ix_opportunities_opportunity_owner_id", "opportunities", ["opportunity_owner_id"])
    op.create_index("ix_opportunities_pipeline_stage_id", "opportunities", ["pipeline_stage_id"])
    op.create_index("ix_opportunities_call1_appointment_date", "opportunities", ["call1_appointment_date"])
    op.create_index("ix_opportunities_call2_appointment_date", "opportunities", ["call2_appointment_date"])
    op.create_index("ix_opportunities_created_at_ghl", "opportunities", ["created_at_ghl"])
    op.create_index("ix_opportunities_canonical_channel", "opportunities", ["canonical_channel"])
    op.create_index("ix_opportunities_is_excluded", "opportunities", ["is_excluded"])
    op.create_index("ix_opportunities_lead_quality", "opportunities", ["lead_quality"])
    op.create_index("ix_opportunities_rep_compliance_failure", "opportunities", ["rep_compliance_failure"])

    # --- sync_runs ---
    op.create_table(
        "sync_runs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("sync_type", sa.String(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("opportunities_synced", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("errors_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_details", JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
    )

    # --- source_normalization ---
    op.create_table(
        "source_normalization",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("raw_value", sa.String(), nullable=False),
        sa.Column("canonical_channel", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()"), nullable=False),
    )
    op.create_index("ix_source_normalization_raw_value", "source_normalization", ["raw_value"], unique=True)

    # Seed source_normalization from confirmed UTM Builder CSV mappings
    op.execute("""
        INSERT INTO source_normalization (raw_value, canonical_channel) VALUES
        -- Meta Ads
        ('meta', 'Meta Ads'),
        ('facebook', 'Meta Ads'),
        ('metaads', 'Meta Ads'),
        ('Facebook', 'Meta Ads'),
        ('Facebook Paid Ads', 'Meta Ads'),
        -- AI channels
        ('ai_bot', 'AI Bot'),
        ('ai_caller', 'AI Caller'),
        ('ai_chat', 'AI Chat (Vera)'),
        ('vera', 'AI Chat (Vera)'),
        -- Webinar
        ('webinar_email', 'Webinar Email Nurture'),
        ('email_nurture', 'Webinar Email Nurture'),
        ('webinar_live', 'Webinar Live'),
        ('webinar', 'Webinar Live'),
        ('Webinar Reg Form QS Rebranded', 'Webinar Live'),
        ('129', 'Webinar Live'),
        ('130', 'Webinar Live'),
        ('131', 'Webinar Live'),
        -- Cold Email
        ('cold_email', 'Cold Email'),
        -- LinkedIn
        ('linkedin', 'LinkedIn'),
        -- Newsletter
        ('newsletter', 'Newsletter'),
        -- Website Direct
        ('website', 'Website Direct'),
        ('direct', 'Website Direct'),
        -- Kajabi
        ('kajabi', 'Kajabi'),
        -- Slack / WhatsApp / SMS
        ('slack', 'Slack / WhatsApp / SMS'),
        ('whatsapp', 'Slack / WhatsApp / SMS'),
        ('sms', 'Slack / WhatsApp / SMS'),
        -- Other
        ('paypal', 'PayPal / GDoc'),
        ('gdoc', 'PayPal / GDoc'),
        ('reactivation', 'Opportunity Reactivation'),
        ('lead_magnet', 'Lead Magnet'),
        ('case_study', 'Case Study')
        ON CONFLICT (raw_value) DO NOTHING;
    """)


def downgrade() -> None:
    op.drop_table("source_normalization")
    op.drop_table("sync_runs")
    op.drop_table("opportunities")
