"""split AI channels into Retell (VERA) voice + AI Bot (Appointwise) SMS

Lloyd wants the two AI booking systems seen separately on Lead Quality by Channel:
  - Retell  — the AI *voice* call agent, historically named VERA  → "Retell (VERA)"
  - Appointwise — the AI *SMS* chatbot (was the generic "AI Bot")  → "AI Bot (Appointwise)"

Today every AI-booked opportunity lands in one bucket ("AI Bot", 42 opps) and two
now-empty legacy buckets ("AI Caller", "AI Chat (Vera)") also exist. This migration:
  1. Re-maps the raw source tokens in source_normalization to the two new channels
     (voice tokens → Retell, chat/SMS tokens → Appointwise) and seeds future
     retell* aliases.
  2. Re-resolves the denormalized opportunities.canonical_channel for the affected
     rows using the SAME precedence the sync normalizer uses (first-touch UTM source,
     then booking campaign source) so history reflects the split.

Lead-source semantics are preserved: opportunities whose first-touch UTM is a real
lead source (e.g. facebook → Meta Ads) but that were merely *booked* by the AI
caller/chat stay on their lead source — only opps actually attributed to the AI
tokens move. Verified on live data before writing: the 42 "AI Bot" opps split into
22 → Retell (VERA) and 20 → AI Bot (Appointwise); 10 Meta-Ads-first opps are
untouched.

Idempotent (WHERE/ON CONFLICT guarded) and reversible.

Revision ID: aichan001
Revises: rls001
Create Date: 2026-07-27

"""
from typing import Sequence, Union

from alembic import op


revision: str = 'aichan001'
down_revision: Union[str, Sequence[str], None] = 'rls001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


RETELL = "Retell (VERA)"
APPOINTWISE = "AI Bot (Appointwise)"

# Voice tokens → Retell (VERA); chat/SMS tokens → AI Bot (Appointwise).
_RETELL_TOKENS = ["ai-caller", "ai_caller", "vera"]
_APPOINTWISE_TOKENS = ["ai_bot", "ai-chat", "ai_chat", "wa-bot"]
# New aliases for future GHL/Retell attribution values (no rows today).
_RETELL_NEW_ALIASES = ["retell", "retell-ai", "retellai"]

# Old canonical buckets whose opportunities must be re-resolved after the remap.
_OLD_AI_CHANNELS = ["AI Bot", "AI Caller", "AI Chat (Vera)"]


def _sql_list(items: Sequence[str]) -> str:
    return ", ".join("'" + i.replace("'", "''") + "'" for i in items)


def _reresolve(channels: Sequence[str]) -> str:
    """Re-derive canonical_channel from the current source_normalization map, mirroring
    sync.normalizer.resolve_canonical_channel precedence: first-touch UTM source, then
    booking campaign source, then keep the existing value."""
    scope = _sql_list(channels)
    return f"""
        UPDATE opportunities o SET canonical_channel = COALESCE(
            (SELECT s.canonical_channel FROM source_normalization s
                WHERE lower(s.raw_value) = lower(o.attr_first_utm_source) LIMIT 1),
            (SELECT s.canonical_channel FROM source_normalization s
                WHERE lower(s.raw_value) = lower(o.op_book_campaign_source) LIMIT 1),
            o.canonical_channel
        )
        WHERE o.canonical_channel IN ({scope});
    """


def upgrade() -> None:
    # 1. Remap existing raw tokens to the two new channels.
    op.execute(
        f"UPDATE source_normalization SET canonical_channel = '{RETELL}' "
        f"WHERE raw_value IN ({_sql_list(_RETELL_TOKENS)});"
    )
    op.execute(
        f"UPDATE source_normalization SET canonical_channel = '{APPOINTWISE}' "
        f"WHERE raw_value IN ({_sql_list(_APPOINTWISE_TOKENS)});"
    )
    # 2. Seed future Retell aliases.
    values = ", ".join(f"('{a}', '{RETELL}', FALSE)" for a in _RETELL_NEW_ALIASES)
    op.execute(
        f"INSERT INTO source_normalization (raw_value, canonical_channel, is_primary_funnel) "
        f"VALUES {values} ON CONFLICT (raw_value) DO NOTHING;"
    )
    # 3. Re-resolve denormalized canonical_channel on affected opportunities.
    op.execute(_reresolve(_OLD_AI_CHANNELS))


def downgrade() -> None:
    # Restore the original raw-token → canonical map.
    op.execute(
        "UPDATE source_normalization SET canonical_channel = 'AI Bot' "
        "WHERE raw_value IN ('ai_bot', 'ai-caller', 'ai-chat', 'wa-bot');"
    )
    op.execute(
        "UPDATE source_normalization SET canonical_channel = 'AI Caller' "
        "WHERE raw_value IN ('ai_caller');"
    )
    op.execute(
        "UPDATE source_normalization SET canonical_channel = 'AI Chat (Vera)' "
        "WHERE raw_value IN ('ai_chat', 'vera');"
    )
    op.execute(
        f"DELETE FROM source_normalization WHERE raw_value IN ({_sql_list(_RETELL_NEW_ALIASES)});"
    )
    # Re-resolve the opps that were split back onto the restored map.
    op.execute(_reresolve([RETELL, APPOINTWISE]))
