"""AI-channel (Retell / Appointwise) specific metrics + data-quality.

Scope for every query here (per the verified spec): Sales pipeline only, and exclude the
internal @quantum-scaling.com / @ig-institute.com contacts. Adds the 'held call' metric,
the attribution-confidence split (source-confirmed vs tag-inferred), the AI data-quality
tiles, and the Retell voice-call reconciliation list.
"""

from datetime import date, datetime, time, timedelta, timezone

from sqlalchemy import and_, case, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from config import SALES_PIPELINE_ID
from db.models import (
    AppointwiseEvent,
    ContactSmsStats,
    DealWhopMatch,
    ExpenseLineItem,
    Opportunity,
    RetellCall,
)
from db.queries.common import base_filter, has_1st_call, prorated_expense_amount
from sync.ghl_client import DEAL_WON_STAGE_ID, DISQUALIFIED_STAGE_ID

# "Held call" = the prospect actually turned up. Verified exact set (distinct from the
# generic 'showed' set — deliberately excludes FU Call Ghost / Deal Lost), OR call-1
# appointment status == 'Showed'.
HELD_STAGE_IDS = [
    "45a0608f-7648-4509-8f3a-d93b21cc9d41",  # 1st Call Done
    "10e6b1ef-0685-4f73-b3c7-b5006b7bc311",  # 2nd Call Done (In Prog)
    "d51c088d-1629-43f2-8ee8-c51bf74b8553",  # Warm List (1st or 2nd Call done)
    "8b0e8559-7665-4033-b762-23d94bfce90b",  # Hot List (Verbal Commit)
    "544b178f-d1f2-4186-a8c2-00c3b0eeefe8",  # Deal Won
    "dfb71208-834b-43e1-9777-5895d6dc8722",  # Long Term Nurture
    "bd0a2b3d-abcc-414d-8935-5a7d781e9727",  # Hot Long Term Nurture
]

_INTERNAL_DOMAINS = ("%@quantum-scaling.com", "%@ig-institute.com")
_AI_SOURCE_TOKENS = ("ai-caller", "ai-chat", "ai_bot")

# Retell/telephony disconnect reasons that mean the human never answered.
NOT_ANSWERED_REASONS = [
    "dial_no_answer", "dial_busy", "dial_failed", "no_answer", "dial_no_pickup",
    "voicemail_reached", "voicemail", "machine_detected", "registered_call_timeout",
    "user_declined",
]


def call_window(start: date, end: date):
    """SQL filter: retell_calls whose started_at falls inside [start, end] *inclusive of end*.

    started_at is a timestamptz while the API hands us plain dates, so a naive
    `started_at <= end` silently drops every call made on the last selected day
    (end resolves to midnight). Compare against a half-open UTC interval instead.
    """
    lo = datetime.combine(start, time.min, tzinfo=timezone.utc)
    hi = datetime.combine(end + timedelta(days=1), time.min, tzinfo=timezone.utc)
    return and_(RetellCall.started_at >= lo, RetellCall.started_at < hi)


def picked_up_expr():
    """SQL boolean: the call was answered by a human (had talk time + a non-no-answer reason)."""
    reason = func.lower(func.coalesce(RetellCall.disconnect_reason, ""))
    return and_(func.coalesce(RetellCall.duration_sec, 0) > 0, reason.notin_(NOT_ANSWERED_REASONS))


def is_picked_up(duration_sec: int | None, disconnect_reason: str | None) -> bool:
    """Python mirror of picked_up_expr for per-row labelling."""
    reason = (disconnect_reason or "").lower()
    return (duration_sec or 0) > 0 and reason not in NOT_ANSWERED_REASONS


def held_call_expr():
    """Boolean: the call was held (stage in the held set OR call-1 status 'Showed')."""
    return or_(
        Opportunity.pipeline_stage_id.in_(HELD_STAGE_IDS),
        Opportunity.call1_appointment_status == "Showed",
    )


def ai_scope_filter():
    """Sales pipeline + exclude internal-domain contacts (NULL email passes)."""
    email = func.lower(func.coalesce(Opportunity.contact_email, ""))
    return and_(
        Opportunity.pipeline_id == SALES_PIPELINE_ID,
        and_(*[email.notlike(d) for d in _INTERNAL_DOMAINS]),
    )


# Channel → Xero expense-vendor keywords (fallback cost source).
_CHANNEL_COST_VENDORS = {
    "Retell (VERA)": ["retell"],
    "AI Bot (Appointwise)": ["appointwise"],
}


def _months_in_range(start: date, end: date) -> list[tuple[date, date]]:
    """Every calendar month the range touches, as (first_day, last_day) pairs."""
    out, cur = [], start.replace(day=1)
    while cur <= end:
        last = (cur + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        out.append((cur, last))
        cur = last + timedelta(days=1)
    return out


async def _call_usage(session: AsyncSession, lo: date, hi: date) -> tuple[int, int]:
    """(dials, talk_seconds) for calls placed in [lo, hi]."""
    r = (await session.execute(
        select(
            func.count(RetellCall.id),
            func.coalesce(func.sum(RetellCall.duration_sec), 0),
        ).where(call_window(lo, hi))
    )).one()
    return r[0] or 0, r[1] or 0


async def get_retell_cost_for_range(session: AsyncSession, start: date, end: date) -> dict:
    """Allocate Retell's real monthly spend (Xero invoices) across [start, end].

    Retell bills by usage, not as a flat subscription, so splitting a month's invoice by
    calendar days misprices any range where volume was uneven — a 9-day window can hold
    half the month's dials. Each overlapping month is therefore allocated by the range's
    share of that month's talk minutes, falling back to dial count when nothing connected
    (August: every call declined, so minutes are 0 but the dials still cost money), and to
    days only when the month has no calls at all.

    Returns the allocated cost plus a per-month audit trail and the list of months in range
    that have calls but no reconciled invoice yet — those are excluded from the total rather
    than guessed at, so the figure is never quietly understated without saying so.
    """
    vendor_match = or_(*[
        ExpenseLineItem.vendor.ilike(f"%{k}%") for k in _CHANNEL_COST_VENDORS["Retell (VERA)"]
    ])
    invoices = (await session.execute(
        select(
            ExpenseLineItem.period_start, ExpenseLineItem.period_end, ExpenseLineItem.amount
        ).where(and_(
            vendor_match,
            ExpenseLineItem.period_start <= end,
            ExpenseLineItem.period_end >= start,
        ))
    )).all()

    total = 0.0
    months: list[dict] = []
    covered: set[tuple[int, int]] = set()
    for ps, pe, amount in invoices:
        amount = float(amount or 0)
        m_dials, m_secs = await _call_usage(session, ps, pe)
        olo, ohi = max(ps, start), min(pe, end)
        r_dials, r_secs = await _call_usage(session, olo, ohi)
        if m_secs:
            share, basis = r_secs / m_secs, "minutes"
        elif m_dials:
            share, basis = r_dials / m_dials, "dials"
        else:
            share, basis = ((ohi - olo).days + 1) / ((pe - ps).days + 1), "days"
        allocated = amount * share
        total += allocated
        covered.add((ps.year, ps.month))
        months.append({
            "month": ps.strftime("%Y-%m"),
            "invoiced": round(amount, 2),
            "allocated": round(allocated, 2),
            "share": round(share, 4),
            "basis": basis,
        })

    # Months the range covers that have calls but no invoice reconciled yet.
    unreconciled = []
    for ps, pe in _months_in_range(start, end):
        if (ps.year, ps.month) in covered:
            continue
        dials, _ = await _call_usage(session, max(ps, start), min(pe, end))
        if dials:
            unreconciled.append({"month": ps.strftime("%Y-%m"), "dials": dials})

    months.sort(key=lambda m: m["month"])
    return {
        "cost": round(total, 2) if months else None,
        "months": months,
        "unreconciled": unreconciled,
    }


async def get_auto_channel_cost(
    session: AsyncSession, channel: str, start: date, end: date
) -> tuple[float | None, str | None]:
    """Auto-resolve a channel's cost for the range. Returns (amount, source).

    Priority for Retell: the reconciled Xero invoices, usage-allocated across the range →
    Retell's own per-call cost → a flat day-prorated Xero line. The invoices come first
    because they are what was actually paid; per-call cost_cents is only populated on a
    minority of calls (Retell started returning it mid-July), so preferring it reported
    $23.75 against a range whose real invoiced spend was $191.58.
    """
    if channel == "Retell (VERA)":
        allocated = await get_retell_cost_for_range(session, start, end)
        if allocated["cost"] is not None:
            return allocated["cost"], "xero"

        cents = (await session.execute(
            select(func.coalesce(func.sum(RetellCall.cost_cents), 0)).where(call_window(start, end))
        )).scalar() or 0
        if cents and cents > 0:
            return round(cents / 100.0, 2), "retell"

    keywords = _CHANNEL_COST_VENDORS.get(channel, [])
    if keywords:
        vendor_match = or_(*[ExpenseLineItem.vendor.ilike(f"%{k}%") for k in keywords])
        amount = (await session.execute(
            select(func.coalesce(func.sum(prorated_expense_amount(start, end)), 0)).where(
                and_(
                    vendor_match,
                    ExpenseLineItem.period_start <= end,
                    ExpenseLineItem.period_end >= start,
                )
            )
        )).scalar() or 0
        if amount and float(amount) > 0:
            return round(float(amount), 2), "xero"

    return None, None


async def get_ai_channel_stats(
    session: AsyncSession, channel: str, start: date, end: date, date_by: str
) -> dict:
    """Retell/Appointwise metrics: booked, held (+rate), won, cash, and the attribution
    confidence split. AI-scoped. For Retell, also pull call volume/minutes from retell_calls."""
    bf = base_filter(start, end, date_by)
    source_confirmed = func.lower(func.coalesce(Opportunity.op_book_campaign_source, "")).in_(_AI_SOURCE_TOKENS)

    row = (await session.execute(
        select(
            func.count(Opportunity.id).label("booked"),
            func.count(case((held_call_expr(), 1))).label("held"),
            func.count(case((Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID, 1))).label("won"),
            func.count(case((Opportunity.pipeline_stage_id == DISQUALIFIED_STAGE_ID, 1))).label("dq"),
            func.coalesce(func.sum(
                case((Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID, DealWhopMatch.total_paid))
            ), 0).label("cash"),
            func.count(case((source_confirmed, 1))).label("source_confirmed"),
            func.count(case((~source_confirmed, 1))).label("tag_inferred"),
        )
        .select_from(Opportunity)
        .outerjoin(DealWhopMatch, Opportunity.ghl_opportunity_id == DealWhopMatch.ghl_opportunity_id)
        .where(and_(bf, ai_scope_filter(), Opportunity.canonical_channel == channel))
    )).one()

    booked = row.booked
    stats = {
        "channel": channel,
        "booked": booked,
        "held": row.held,
        "held_rate": round(row.held / booked, 4) if booked else None,
        "won": row.won,
        "dq": row.dq,
        "cash_collected": float(row.cash),
        "confidence": {"source_confirmed": row.source_confirmed, "tag_inferred": row.tag_inferred},
        "calls": None,  # populated below for Retell
    }

    if channel == "Retell (VERA)":
        # Retell call volume is dated by when the call was placed (started_at), independent
        # of the opportunity date_by dimension above. MUST be windowed — an unfiltered
        # aggregate here reports all-time dials against a range-scoped booked count.
        answered = picked_up_expr()
        cr = (await session.execute(
            select(
                func.count(RetellCall.id).label("n"),
                func.coalesce(func.sum(RetellCall.duration_sec), 0).label("secs"),
                func.count(case((RetellCall.ghl_contact_id.isnot(None), 1))).label("matched"),
                func.count(case((answered, 1))).label("picked_up"),
                func.count(func.distinct(RetellCall.to_number)).label("contacts"),
                func.count(func.distinct(case((answered, RetellCall.to_number)))).label("reached"),
                func.coalesce(func.sum(case((answered, RetellCall.duration_sec))), 0).label("talk_secs"),
            ).where(call_window(start, end))
        )).one()
        dialed = cr.n
        contacts = cr.contacts or 0
        picked_up = cr.picked_up or 0
        reached = cr.reached or 0
        stats["dialed"] = dialed
        stats["dialed_contacts"] = contacts
        stats["attempts_per_contact"] = round(dialed / contacts, 1) if contacts else None
        stats["picked_up"] = picked_up
        stats["pickup_rate"] = round(picked_up / dialed, 4) if dialed else None
        stats["contacts_reached"] = reached
        stats["reach_rate"] = round(reached / contacts, 4) if contacts else None
        # Avg talk time on answered calls only — averaging over unanswered dials would
        # drag it to ~0 and stop being a conversation-quality signal.
        stats["avg_talk_sec"] = round(cr.talk_secs / picked_up, 1) if picked_up else None
        # Booking conversion per unique contact dialed (matches the ~1.3% baseline; raw dials
        # include retries so booked ÷ contacts is the meaningful top-of-funnel rate).
        stats["conv_rate"] = round(booked / contacts, 4) if contacts else None
        # Step-to-step conversion for the funnel strip: each stage against the one before it,
        # which is what localises a leak (a low overall rate can't tell you *where* it leaks).
        stats["book_rate_of_reached"] = round(booked / reached, 4) if reached else None
        stats["win_rate_of_booked"] = round(row.won / booked, 4) if booked else None
        stats["calls"] = {
            "count": dialed,
            "minutes": round(cr.secs / 60.0, 1),
            "matched": cr.matched,
            "picked_up": picked_up,
        }
        # Data freshness — deliberately NOT windowed: these describe the feed itself, so the
        # page can say how current the numbers are regardless of which range is selected.
        fresh = (await session.execute(
            select(
                func.max(RetellCall.updated_at).label("synced"),
                func.max(RetellCall.started_at).label("latest_call"),
            )
        )).one()
        stats["last_synced_at"] = fresh.synced.isoformat() if fresh.synced else None
        stats["latest_call_at"] = fresh.latest_call.isoformat() if fresh.latest_call else None
    return stats


async def get_retell_calls(
    session: AsyncSession,
    start: date,
    end: date,
    filter_mode: str = "all",
    connection: str = "all",
    min_minutes: float = 0.0,
    limit: int = 500,
) -> list[dict]:
    """Retell voice calls (reconciliation list). Ordered newest first.

    filter_mode 'dq' → only calls whose matched opportunity is Disqualified.
    connection 'picked_up' | 'no_answer' → filter on whether a human answered.
    min_minutes → only calls at least this long.
    """
    dq_opps = (
        select(Opportunity.ghl_contact_id)
        .where(Opportunity.pipeline_stage_id == DISQUALIFIED_STAGE_ID)
        .scalar_subquery()
    )

    q = (
        select(RetellCall)
        .where(call_window(start, end))
        .order_by(RetellCall.started_at.desc())
        .limit(limit)
    )
    if filter_mode == "dq":
        q = q.where(RetellCall.ghl_contact_id.in_(dq_opps))
    if connection == "picked_up":
        q = q.where(picked_up_expr())
    elif connection == "no_answer":
        q = q.where(~picked_up_expr())
    if min_minutes and min_minutes > 0:
        q = q.where(func.coalesce(RetellCall.duration_sec, 0) >= int(min_minutes * 60))

    rows = (await session.execute(q)).scalars().all()
    return [
        {
            "retell_call_id": c.retell_call_id,
            "started_at": c.started_at.isoformat() if c.started_at else None,
            "duration_sec": c.duration_sec,
            "from_number": c.from_number,
            "to_number": c.to_number,
            "call_status": c.call_status,
            "disconnect_reason": c.disconnect_reason,
            "picked_up": is_picked_up(c.duration_sec, c.disconnect_reason),
            "has_recording": bool(c.recording_url),
            "sentiment": (c.analysis or {}).get("user_sentiment") if c.analysis else None,
            "successful": (c.analysis or {}).get("call_successful") if c.analysis else None,
            "ghl_contact_id": c.ghl_contact_id,
            "ghl_contact_name": c.ghl_contact_name,
            "match_method": c.match_method,
            "match_confidence": c.match_confidence,
        }
        for c in rows
    ]


async def get_retell_agent_breakdown(
    session: AsyncSession, start: date, end: date
) -> list[dict]:
    """Per-Retell-agent call performance in the range (for comparing agents/goals)."""
    rows = (await session.execute(
        select(
            RetellCall.agent_id,
            func.count(RetellCall.id).label("calls"),
            func.count(case((picked_up_expr(), 1))).label("picked_up"),
            func.coalesce(func.sum(RetellCall.duration_sec), 0).label("secs"),
            func.count(func.distinct(RetellCall.to_number)).label("contacts"),
            func.count(case((RetellCall.ghl_contact_id.isnot(None), 1))).label("matched"),
        )
        .where(call_window(start, end))
        .group_by(RetellCall.agent_id)
        .order_by(func.count(RetellCall.id).desc())
    )).all()
    return [
        {
            "agent_id": r.agent_id,
            "calls": r.calls,
            "contacts": r.contacts,
            "picked_up": r.picked_up,
            "pickup_rate": round(r.picked_up / r.calls, 4) if r.calls else None,
            "minutes": round(r.secs / 60.0, 1),
            "avg_sec": round(r.secs / r.calls, 1) if r.calls else None,
            "matched": r.matched,
        }
        for r in rows
    ]


async def get_appointwise_agent_breakdown(session: AsyncSession) -> list[dict]:
    """Per-Appointwise-agent performance, from webhook events (the only agent-identity source).
    Empty until the Appointwise Webhook Node is configured to push events."""
    pairs = (await session.execute(
        select(AppointwiseEvent.agent_name, AppointwiseEvent.ghl_contact_id)
        .where(and_(AppointwiseEvent.agent_name.isnot(None), AppointwiseEvent.ghl_contact_id.isnot(None)))
        .distinct()
    )).all()
    if not pairs:
        return []
    booked_ids = set((await session.execute(
        select(Opportunity.ghl_contact_id).where(and_(
            ai_scope_filter(),
            Opportunity.canonical_channel == "AI Bot (Appointwise)",
            Opportunity.ghl_contact_id.isnot(None),
        ))
    )).scalars().all())

    from collections import defaultdict
    agents: dict[str, set] = defaultdict(set)
    for name, cid in pairs:
        agents[name].add(cid)

    out = [
        {
            "agent_name": name,
            "contacts": len(cids),
            "booked": len(cids & booked_ids),
            "book_rate": round(len(cids & booked_ids) / len(cids), 4) if cids else None,
        }
        for name, cids in agents.items()
    ]
    out.sort(key=lambda x: x["contacts"], reverse=True)
    return out


async def get_appointwise_sms_stats(session: AsyncSession) -> dict:
    """Appointwise SMS funnel from GHL Conversations. contact_sms_stats holds only
    Appointwise-tagged contacts (both write paths are tag-gated), so this is the audience:
    messaged → replied → booked, plus avg msgs, reply timing, reply-#, and opt-outs.

    The main sync captures booked leads; the contact-by-tag sweep fills in the non-booked
    audience (run /api/sync/appointwise-sms). Booked = has an Appointwise-channel opp.
    """
    stats = (await session.execute(select(ContactSmsStats))).scalars().all()
    if not stats:
        return {"leads": 0, "messaged": 0, "replied": 0, "booked": 0}

    booked_ids = set((await session.execute(
        select(Opportunity.ghl_contact_id).where(and_(
            ai_scope_filter(),
            Opportunity.canonical_channel == "AI Bot (Appointwise)",
            Opportunity.ghl_contact_id.isnot(None),
        ))
    )).scalars().all())

    def _rate(n, d):
        return round(n / d, 4) if d else None

    messaged = [s for s in stats if s.outbound_count > 0]
    replied = [s for s in stats if s.inbound_count > 0]
    booked = [s for s in stats if s.ghl_contact_id in booked_ids]
    opted = [s for s in stats if s.opted_out]
    outbounds = [s.outbound_count for s in stats if s.outbound_count]

    # Reply-after-# histogram (on which outbound message they first replied): 1, 2, 3, 4, 5+
    reply_hist = {"1": 0, "2": 0, "3": 0, "4": 0, "5+": 0}
    for s in replied:
        n = s.reply_after_n
        if not n:
            continue
        reply_hist["5+" if n >= 5 else str(n)] += 1

    # Opt-out-after-# histogram (how many messages before they opted out).
    optout_hist = {"1-2": 0, "3-4": 0, "5-6": 0, "7+": 0}
    for s in opted:
        n = s.opt_out_after_n or 0
        key = "1-2" if n <= 2 else "3-4" if n <= 4 else "5-6" if n <= 6 else "7+"
        optout_hist[key] += 1

    # Median-ish first-reply delay (hours) and reply hour-of-day.
    delays, hours = [], {}
    for s in replied:
        if s.first_outbound_at and s.first_inbound_at:
            delays.append((s.first_inbound_at - s.first_outbound_at).total_seconds() / 3600.0)
        if s.first_inbound_at:
            h = s.first_inbound_at.hour
            hours[h] = hours.get(h, 0) + 1

    return {
        "leads": len(stats),
        "messaged": len(messaged),
        "replied": len(replied),
        "booked": len(booked),
        "reply_rate": _rate(len(replied), len(messaged)),
        "book_rate": _rate(len(booked), len(messaged)),
        "avg_msgs_per_lead": round(sum(outbounds) / len(outbounds), 1) if outbounds else None,
        "avg_reply_after_n": round(sum(s.reply_after_n for s in replied if s.reply_after_n) / len(replied), 1) if replied else None,
        "reply_by_message": reply_hist,
        "opted_out": len(opted),
        "opt_out_rate": _rate(len(opted), len(stats)),
        "opt_out_by_message": optout_hist,
        "avg_first_reply_hours": round(sum(delays) / len(delays), 1) if delays else None,
        "peak_reply_hour": max(hours, key=hours.get) if hours else None,
    }


async def get_ai_data_quality(session: AsyncSession) -> dict:
    """AI data-quality tiles (all-time, sales pipeline scope)."""
    # Same-contact ≥2 OPEN opps (not won/lost/DQ/cancelled/no-show) — leak alert, expect 0.
    closed_stages = (
        DEAL_WON_STAGE_ID, DISQUALIFIED_STAGE_ID,
        "80cba97d-2f60-4485-8953-4b9569b1ddc1",  # Deal Lost
        "1201d3c3-166e-4c01-90b5-7f02e02a77c4",  # No-Show
        "b9624f39-9697-418c-864b-bd28c1db6182",  # Cancelled
    )
    dup_open = (await session.execute(text(
        """
        SELECT count(*) FROM (
            SELECT ghl_contact_id FROM opportunities
            WHERE pipeline_id = :pid AND is_excluded = false
              AND ghl_contact_id IS NOT NULL
              AND coalesce(pipeline_stage_id,'') <> ALL(:closed)
            GROUP BY ghl_contact_id HAVING count(*) >= 2
        ) t
        """
    ), {"pid": SALES_PIPELINE_ID, "closed": list(closed_stages)})).scalar() or 0

    # $0 / owner-less won deals (3 known).
    zero_owner_wons = (await session.execute(
        select(func.count(Opportunity.id)).where(and_(
            Opportunity.pipeline_id == SALES_PIPELINE_ID,
            Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID,
            or_(Opportunity.opportunity_owner_name.is_(None), Opportunity.opportunity_owner_name == ""),
        ))
    )).scalar() or 0

    # AI method-token in source but NO corresponding tag (source-confirmed w/o tag backfill).
    method_no_tag = (await session.execute(text(
        """
        SELECT count(*) FROM opportunities
        WHERE pipeline_id = :pid
          AND lower(coalesce(op_book_campaign_source,'')) IN ('ai-caller','ai-chat','ai_bot')
          AND (contact_tags IS NULL OR cardinality(contact_tags) = 0)
        """
    ), {"pid": SALES_PIPELINE_ID})).scalar() or 0

    # source = 'call' opps pending definition (phone/manual, NOT AI) — 123 expected.
    source_call = (await session.execute(
        select(func.count(Opportunity.id)).where(and_(
            Opportunity.pipeline_id == SALES_PIPELINE_ID,
            func.lower(func.coalesce(Opportunity.op_book_campaign_source, "")) == "call",
        ))
    )).scalar() or 0

    # Sync freshness — most recent opportunity sync timestamp.
    last_sync = (await session.execute(select(func.max(Opportunity.synced_at)))).scalar()

    return {
        "same_contact_multi_open": dup_open,
        "zero_or_ownerless_wons": zero_owner_wons,
        "method_token_without_tag": method_no_tag,
        "source_call_pending": source_call,
        "last_synced_at": last_sync.isoformat() if last_sync else None,
    }


async def get_vera_chat_contacts(session: AsyncSession, limit: int = 5) -> list[dict]:
    """Opps classified as Vera *chat* (folded into Appointwise) — for fact-checking that
    they're really Appointwise. Chat signal = vera-chat-booked tag OR op_book source 'ai-chat'."""
    rows = (await session.execute(
        select(
            Opportunity.ghl_opportunity_id,
            Opportunity.ghl_contact_id,
            Opportunity.opportunity_name,
            Opportunity.opportunity_owner_name,
            Opportunity.contact_email,
            Opportunity.op_book_campaign_source,
            Opportunity.contact_tags,
        )
        .where(and_(
            Opportunity.pipeline_id == SALES_PIPELINE_ID,
            or_(
                func.lower(func.coalesce(Opportunity.op_book_campaign_source, "")) == "ai-chat",
                Opportunity.contact_tags.any("vera-chat-booked"),
            ),
        ))
        .limit(limit)
    )).all()
    return [
        {
            "ghl_opportunity_id": r.ghl_opportunity_id,
            "ghl_contact_id": r.ghl_contact_id,
            "name": r.opportunity_name,
            "rep": r.opportunity_owner_name,
            "email": r.contact_email,
            "op_book_campaign_source": r.op_book_campaign_source,
            "tags": r.contact_tags,
        }
        for r in rows
    ]
