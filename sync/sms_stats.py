"""Compute per-contact SMS engagement from GHL Conversation messages.

Appointwise sends SMS/WhatsApp through GHL, so GHL Conversations is the source of truth for
the Appointwise SMS funnel. This module classifies Appointwise contacts (by tag) and reduces
a contact's message list to the aggregates stored in contact_sms_stats.
"""

import re

from sync.normalizer import parse_ghl_datetime

# Appointwise worked-contact / audience tags (verified registry).
APPOINTWISE_TAGS = {"ai bot", "appointwise-webinar-lead"}

_OPT_OUT_RE = re.compile(r"\b(stop|unsubscribe|opt\s*out|remove me|do not (text|contact)|cancel)\b", re.I)
_SMS_TYPE_HINTS = ("SMS", "WHATSAPP")


def is_appointwise_contact(contact_tags: list[str] | None) -> bool:
    norm = {t.strip().lower() for t in (contact_tags or []) if t and t.strip()}
    return bool(norm & APPOINTWISE_TAGS)


def _is_sms(msg: dict) -> bool:
    mt = (msg.get("messageType") or "").upper()
    if mt:
        return any(h in mt for h in _SMS_TYPE_HINTS)
    return msg.get("type") in (1, 2)  # GHL numeric fallback: 1=SMS, 2=WhatsApp-ish


def compute_sms_stats(contact_id: str, messages: list[dict]) -> dict | None:
    """Reduce a contact's GHL messages to SMS engagement aggregates. None if no SMS."""
    sms = []
    for m in messages:
        if not _is_sms(m):
            continue
        dt = parse_ghl_datetime(m.get("dateAdded"))
        direction = (m.get("direction") or "").lower()
        if not dt or direction not in ("inbound", "outbound"):
            continue
        sms.append((dt, direction, m.get("body") or ""))
    if not sms:
        return None

    sms.sort(key=lambda x: x[0])
    outbound = [s for s in sms if s[1] == "outbound"]
    inbound = [s for s in sms if s[1] == "inbound"]
    first_in = inbound[0][0] if inbound else None

    reply_after_n = sum(1 for s in outbound if s[0] <= first_in) if first_in else None

    opted_out = False
    opt_out_at = None
    opt_out_after_n = None
    for dt, direction, body in sms:
        if direction == "inbound" and _OPT_OUT_RE.search(body):
            opted_out = True
            opt_out_at = dt
            opt_out_after_n = sum(1 for s in outbound if s[0] <= dt)
            break

    return {
        "ghl_contact_id": contact_id,
        "outbound_count": len(outbound),
        "inbound_count": len(inbound),
        "first_outbound_at": outbound[0][0] if outbound else None,
        "first_inbound_at": first_in,
        "last_message_at": sms[-1][0],
        "reply_after_n": reply_after_n,
        "opted_out": opted_out,
        "opt_out_at": opt_out_at,
        "opt_out_after_n": opt_out_after_n,
    }
