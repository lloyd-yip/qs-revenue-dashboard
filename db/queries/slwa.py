"""Slack / WhatsApp / SMS weekly dashboard queries and manual inputs."""

from datetime import date, timedelta

from sqlalchemy import Date, and_, case, cast, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity, SLWAWeeklyInput
from db.queries.common import ALL_TEAM_SENTINEL, QUALIFIED_LEAD_QUALITY, sales_rep_filter
from sync.ghl_client import DEAL_WON_STAGE_ID, DISQUALIFIED_STAGE_ID

SLWA_SCOPE_LABELS = {
    "overall": "Overall",
    "slack": "Slack",
    "whatsapp": "WhatsApp",
    "sms": "SMS",
}

# Scopes that are channel-specific (i.e. require an op_book_campaign_source filter).
# "overall" is intentionally excluded — the workbook's Overall sheet has NO source
# filter; it counts all opps regardless of channel.
CHANNEL_SCOPES = ("slack", "whatsapp", "sms")

SLWA_SECTION_LABELS = {
    "ghl": "GHL",
    "calendly": "Calendly",
}

SLWA_CHANNEL_SECTION_SOURCES = {
    "slack": {
        "ghl": ("slack",),
        "calendly": ("slack calendly",),
    },
    "whatsapp": {
        "ghl": ("whatsapp",),
        "calendly": ("whatsapp calendly",),
    },
    "sms": {
        "ghl": ("sms-zach",),
        "calendly": ("sms calendly",),
    },
}


def get_slwa_scope_label(scope: str) -> str:
    return SLWA_SCOPE_LABELS.get(scope, "Overall")


def _lower_sources(sources: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(source.lower() for source in sources)


def get_slwa_section_sources(scope: str, section: str) -> tuple[str, ...]:
    """Return the op_book_campaign_source values that belong to a channel/section.

    For 'overall' scope: returns an empty tuple — Overall has no source filter.
    """
    if scope == "overall":
        return ()
    return SLWA_CHANNEL_SECTION_SOURCES.get(scope, {}).get(section, ())


def get_slwa_scope_sources(scope: str) -> tuple[str, ...]:
    """Return all op_book_campaign_source values for a scope.

    For 'overall' scope: returns an empty tuple — Overall has no source filter.
    """
    if scope == "overall":
        return ()
    merged = []
    for sources in SLWA_CHANNEL_SECTION_SOURCES.get(scope, {}).values():
        merged.extend(sources)
    return tuple(merged)


def get_slwa_sections(scope: str) -> list[dict[str, str]]:
    """Return the GHL/Calendly section list for a scope.

    Overall returns a single 'all' section (no GHL/Calendly split).
    Channel scopes return GHL + Calendly where data exists.
    """
    if scope == "overall":
        return [{"key": "all", "label": "All Sources"}]
    sections: list[dict[str, str]] = []
    for key, label in SLWA_SECTION_LABELS.items():
        if get_slwa_section_sources(scope, key):
            sections.append({"key": key, "label": label})
    return sections


def _scope_filter(
    start: date,
    end: date,
    rep_id: str | None,
    sources: tuple[str, ...],
):
    """Build the WHERE clause for a weekly query.

    When ``sources`` is empty (Overall scope), no source filter is applied —
    this matches the workbook's Overall sheet which counts ALL opps.
    """
    filters = [
        Opportunity.is_excluded.is_(False),
        Opportunity.call1_booking_date.isnot(None),
        func.date(Opportunity.call1_booking_date) >= start,
        func.date(Opportunity.call1_booking_date) <= end,
    ]
    # Channel-specific scopes filter on op_book_campaign_source (= Opps!AP).
    # Overall scope has NO source filter — it counts all opps globally.
    if sources:
        filters.append(
            func.lower(func.coalesce(Opportunity.op_book_campaign_source, "")).in_(_lower_sources(sources))
        )
    if rep_id == ALL_TEAM_SENTINEL:
        pass
    elif rep_id:
        filters.append(Opportunity.opportunity_owner_id == rep_id)
    else:
        filters.append(sales_rep_filter())
    return and_(*filters)


def _week_floor(value: date) -> date:
    return value - timedelta(days=value.weekday())


def _iter_week_starts(start: date, end: date) -> list[date]:
    current = _week_floor(start)
    last = _week_floor(end)
    weeks: list[date] = []
    while current <= last:
        weeks.append(current)
        current += timedelta(days=7)
    return weeks


async def _get_manual_inputs(
    session: AsyncSession,
    channel_key: str,
    section: str,
    start: date,
    end: date,
) -> dict[date, dict]:
    """Fetch manual weekly inputs for a scope/section.

    Each scope (including 'overall') stores its own manual rows —
    Overall is NOT an aggregation of channel values.
    """
    week_start_floor = _week_floor(start)
    week_end_floor = _week_floor(end)
    rows = (
        await session.execute(
            select(SLWAWeeklyInput)
            .where(
                and_(
                    SLWAWeeklyInput.channel_key == channel_key,
                    SLWAWeeklyInput.section == section,
                    SLWAWeeklyInput.week_start >= week_start_floor,
                    SLWAWeeklyInput.week_start <= week_end_floor,
                )
            )
            .order_by(SLWAWeeklyInput.week_start)
        )
    ).scalars().all()

    manual_by_week: dict[date, dict] = {}
    for row in rows:
        manual_by_week[row.week_start] = {
            "message_sent": float(row.message_sent) if row.message_sent is not None else None,
            "links_sent": float(row.links_sent) if row.links_sent is not None else None,
            "changes_to_funnel": row.changes_to_funnel,
            "copy": row.copy,
            "groups": row.groups,
        }

    return manual_by_week


def _summarize_week_rows(rows: list[dict]) -> dict:
    booked = sum(r["booked"] for r in rows)
    meeting_date_passed = sum(r["meeting_date_passed"] for r in rows)
    showed = sum(r["showed"] for r in rows)
    qualified_show = sum(r["qualified_show"] for r in rows)
    cancelled = sum(r["cancelled"] for r in rows)
    no_show = sum(r["no_show"] for r in rows)
    dq_count = sum(r["dq_count"] for r in rows)
    units_closed = sum(r["units_closed"] for r in rows)
    great_count = sum(r["great_count"] for r in rows)
    ok_count = sum(r["ok_count"] for r in rows)
    barely_passable_count = sum(r["barely_passable_count"] for r in rows)
    bad_dq_count = sum(r["bad_dq_count"] for r in rows)
    message_sent = sum(r["message_sent"] or 0 for r in rows) or None
    links_sent = sum(r["links_sent"] or 0 for r in rows) or None

    return {
        "booked": booked,
        "meeting_date_passed": meeting_date_passed,
        "showed": showed,
        "show_rate": round(showed / meeting_date_passed, 4) if meeting_date_passed else None,
        "qualified_show": qualified_show,
        "cancelled": cancelled,
        "no_show": no_show,
        "dq_count": dq_count,
        "units_closed": units_closed,
        "great_count": great_count,
        "ok_count": ok_count,
        "barely_passable_count": barely_passable_count,
        "bad_dq_count": bad_dq_count,
        "message_sent": message_sent,
        "links_sent": links_sent,
    }


async def _get_section_week_rows(
    session: AsyncSession,
    scope: str,
    section: str,
    start: date,
    end: date,
    rep_id: str | None,
) -> list[dict]:
    sources = get_slwa_section_sources(scope, section)
    # For channel scopes, no sources = this section doesn't exist for that channel.
    # For Overall scope, sources is intentionally empty (no source filter needed).
    if scope != "overall" and not sources:
        return []

    # Workbook uses < TODAY() (strictly before today) for "meeting date passed".
    # We compute cutoff as the day before today, then use <= cutoff in the query,
    # which is equivalent to < TODAY().
    cutoff = min(end, date.today() - timedelta(days=1))
    booking_week = cast(func.date_trunc("week", Opportunity.call1_booking_date), Date)
    row_filter = _scope_filter(start, end, rep_id, sources)
    appt_date = func.date(Opportunity.call1_appointment_date)

    result = await session.execute(
        select(
            booking_week.label("week_start"),
            func.count(Opportunity.id).label("booked"),
            func.count(
                case(
                    (
                        and_(
                            Opportunity.call1_appointment_date.isnot(None),
                            appt_date >= booking_week,
                            appt_date <= cutoff,
                        ),
                        1,
                    )
                )
            ).label("meeting_date_passed"),
            func.count(
                case(
                    (
                        and_(
                            Opportunity.call1_appointment_status == "Showed",
                            Opportunity.call1_appointment_date.isnot(None),
                            appt_date >= booking_week,
                            appt_date <= cutoff,
                        ),
                        1,
                    )
                )
            ).label("showed"),
            func.count(
                case(
                    (
                        and_(
                            Opportunity.call1_appointment_status == "Cancelled",
                            Opportunity.call1_appointment_date.isnot(None),
                            appt_date >= booking_week,
                            appt_date <= cutoff,
                        ),
                        1,
                    )
                )
            ).label("cancelled"),
            func.count(
                case(
                    (
                        and_(
                            Opportunity.call1_appointment_status == "No Show",
                            Opportunity.call1_appointment_date.isnot(None),
                            appt_date >= booking_week,
                            appt_date <= cutoff,
                        ),
                        1,
                    )
                )
            ).label("no_show"),
            func.count(
                case(
                    (
                        and_(
                            Opportunity.call1_appointment_status == "Showed",
                            Opportunity.call1_appointment_date.isnot(None),
                            appt_date >= booking_week,
                            appt_date <= cutoff,
                            Opportunity.lead_quality.in_(QUALIFIED_LEAD_QUALITY),
                        ),
                        1,
                    )
                )
            ).label("qualified_show"),
            func.count(
                case(
                    (
                        and_(
                            Opportunity.call1_appointment_date.isnot(None),
                            appt_date >= booking_week,
                            appt_date <= cutoff,
                            Opportunity.pipeline_stage_id == DISQUALIFIED_STAGE_ID,
                        ),
                        1,
                    )
                )
            ).label("dq_count"),
            func.count(
                case((Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID, 1))
            ).label("units_closed"),
            func.count(case((Opportunity.lead_quality == "Great", 1))).label("great_count"),
            func.count(case((Opportunity.lead_quality == "Ok", 1))).label("ok_count"),
            func.count(case((Opportunity.lead_quality == "Barely Passable", 1))).label("barely_passable_count"),
            func.count(
                case((Opportunity.lead_quality.in_(("Bad", "DQ", "Bad / DQ")), 1))
            ).label("bad_dq_count"),
        )
        .where(row_filter)
        .group_by(booking_week)
        .order_by(booking_week)
    )

    manual_map = await _get_manual_inputs(session, scope, section, start, end)
    rows_by_week: dict[date, dict] = {}
    for row in result.all():
        rows_by_week[row.week_start] = {
            "booked": row.booked,
            "meeting_date_passed": row.meeting_date_passed,
            "showed": row.showed,
            "cancelled": row.cancelled,
            "no_show": row.no_show,
            "show_rate": round(row.showed / row.meeting_date_passed, 4) if row.meeting_date_passed else None,
            "qualified_show": row.qualified_show,
            "dq_count": row.dq_count,
            "units_closed": row.units_closed,
            "great_count": row.great_count,
            "ok_count": row.ok_count,
            "barely_passable_count": row.barely_passable_count,
            "bad_dq_count": row.bad_dq_count,
        }

    rows: list[dict] = []
    for week_start in _iter_week_starts(start, end):
        metrics = rows_by_week.get(
            week_start,
            {
                "booked": 0,
                "meeting_date_passed": 0,
                "showed": 0,
                "cancelled": 0,
                "no_show": 0,
                "show_rate": None,
                "qualified_show": 0,
                "dq_count": 0,
                "units_closed": 0,
                "great_count": 0,
                "ok_count": 0,
                "barely_passable_count": 0,
                "bad_dq_count": 0,
            },
        )
        manual = manual_map.get(week_start, {})
        rows.append(
            {
                "week_start": week_start.isoformat(),
                **metrics,
                "message_sent": manual.get("message_sent"),
                "links_sent": manual.get("links_sent"),
                "changes_to_funnel": manual.get("changes_to_funnel"),
                "copy": manual.get("copy"),
                "groups": manual.get("groups"),
            }
        )
    return rows


async def _get_rep_totals(
    session: AsyncSession,
    scope: str,
    start: date,
    end: date,
    rep_id: str | None,
) -> list[dict]:
    sources = get_slwa_scope_sources(scope)
    row_filter = _scope_filter(start, end, rep_id, sources)

    result = await session.execute(
        select(
            Opportunity.opportunity_owner_id.label("rep_id"),
            Opportunity.opportunity_owner_name.label("rep_name"),
            func.count(Opportunity.id).label("booked"),
            func.count(case((Opportunity.call1_appointment_status == "Showed", 1))).label("showed"),
            func.count(
                case(
                    (
                        and_(
                            Opportunity.call1_appointment_status == "Showed",
                            Opportunity.lead_quality.in_(QUALIFIED_LEAD_QUALITY),
                        ),
                        1,
                    )
                )
            ).label("qualified_show"),
            func.count(case((Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID, 1))).label("units_closed"),
        )
        .where(row_filter)
        .group_by(Opportunity.opportunity_owner_id, Opportunity.opportunity_owner_name)
        .order_by(func.count(Opportunity.id).desc(), Opportunity.opportunity_owner_name.asc())
    )

    return [
        {
            "rep_id": row.rep_id,
            "rep_name": row.rep_name or "Unassigned",
            "booked": row.booked,
            "showed": row.showed,
            "qualified_show": row.qualified_show,
            "units_closed": row.units_closed,
        }
        for row in result.all()
    ]


async def _get_channel_totals(
    session: AsyncSession,
    start: date,
    end: date,
    rep_id: str | None,
) -> list[dict]:
    totals = []
    for channel_key in ("slack", "whatsapp", "sms"):
        sections = []
        for section in get_slwa_sections(channel_key):
            weeks = await _get_section_week_rows(session, channel_key, section["key"], start, end, rep_id)
            sections.append(_summarize_week_rows(weeks))

        summary = {
            "scope": channel_key,
            "label": get_slwa_scope_label(channel_key),
            "booked": sum(s["booked"] for s in sections),
            "meeting_date_passed": sum(s["meeting_date_passed"] for s in sections),
            "showed": sum(s["showed"] for s in sections),
            "qualified_show": sum(s["qualified_show"] for s in sections),
            "units_closed": sum(s["units_closed"] for s in sections),
        }
        summary["show_rate"] = (
            round(summary["showed"] / summary["meeting_date_passed"], 4)
            if summary["meeting_date_passed"]
            else None
        )
        totals.append(summary)
    return totals


async def get_slwa_weekly_dashboard(
    session: AsyncSession,
    scope: str,
    start: date,
    end: date,
    rep_id: str | None = None,
) -> dict:
    if scope not in SLWA_SCOPE_LABELS:
        scope = "overall"

    sections = []
    for section in get_slwa_sections(scope):
        weeks = await _get_section_week_rows(session, scope, section["key"], start, end, rep_id)
        sections.append(
            {
                "key": section["key"],
                "label": section["label"],
                "summary": _summarize_week_rows(weeks),
                "weeks": weeks,
            }
        )

    overall_summary = {
        "booked": sum(section["summary"]["booked"] for section in sections),
        "meeting_date_passed": sum(section["summary"]["meeting_date_passed"] for section in sections),
        "showed": sum(section["summary"]["showed"] for section in sections),
        "qualified_show": sum(section["summary"]["qualified_show"] for section in sections),
        "cancelled": sum(section["summary"]["cancelled"] for section in sections),
        "no_show": sum(section["summary"]["no_show"] for section in sections),
        "dq_count": sum(section["summary"]["dq_count"] for section in sections),
        "units_closed": sum(section["summary"]["units_closed"] for section in sections),
        "great_count": sum(section["summary"]["great_count"] for section in sections),
        "ok_count": sum(section["summary"]["ok_count"] for section in sections),
        "barely_passable_count": sum(section["summary"]["barely_passable_count"] for section in sections),
        "bad_dq_count": sum(section["summary"]["bad_dq_count"] for section in sections),
        "message_sent": sum(section["summary"]["message_sent"] or 0 for section in sections) or None,
        "links_sent": sum(section["summary"]["links_sent"] or 0 for section in sections) or None,
    }
    overall_summary["show_rate"] = (
        round(overall_summary["showed"] / overall_summary["meeting_date_passed"], 4)
        if overall_summary["meeting_date_passed"]
        else None
    )

    return {
        "scope": scope,
        "scope_label": get_slwa_scope_label(scope),
        "summary": overall_summary,
        "sections": sections,
        "rep_totals": await _get_rep_totals(session, scope, start, end, rep_id),
        "channel_totals": await _get_channel_totals(session, start, end, rep_id) if scope == "overall" else [],
    }


async def get_slwa_closes(
    session: AsyncSession,
    scope: str,
    start: date,
    end: date,
    rep_id: str | None = None,
) -> list[dict]:
    sources = get_slwa_scope_sources(scope if scope in SLWA_SCOPE_LABELS else "overall")
    row_filter = and_(
        _scope_filter(start, end, rep_id, sources),
        Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID,
    )

    result = await session.execute(
        select(
            Opportunity.opportunity_name,
            Opportunity.opportunity_owner_name,
            Opportunity.close_date,
            Opportunity.projected_deal_size,
        )
        .where(row_filter)
        .order_by(Opportunity.close_date.desc().nulls_last(), Opportunity.updated_at_ghl.desc())
    )

    return [
        {
            "name": row.opportunity_name or "—",
            "rep": row.opportunity_owner_name or "Unassigned",
            "close_date": row.close_date.strftime("%b %d, %Y") if row.close_date else "—",
            "value": float(row.projected_deal_size) if row.projected_deal_size is not None else None,
        }
        for row in result.all()
    ]


async def upsert_slwa_weekly_input(
    session: AsyncSession,
    channel_key: str,
    section: str,
    week_start: date,
    message_sent: float | None = None,
    links_sent: float | None = None,
    changes_to_funnel: str | None = None,
    copy: str | None = None,
    groups: str | None = None,
) -> None:
    stmt = pg_insert(SLWAWeeklyInput).values(
        channel_key=channel_key,
        section=section,
        week_start=week_start,
        message_sent=message_sent,
        links_sent=links_sent,
        changes_to_funnel=changes_to_funnel,
        copy=copy,
        groups=groups,
    ).on_conflict_do_update(
        index_elements=["channel_key", "section", "week_start"],
        set_={
            "message_sent": message_sent,
            "links_sent": links_sent,
            "changes_to_funnel": changes_to_funnel,
            "copy": copy,
            "groups": groups,
            "updated_at": func.now(),
        },
    )
    await session.execute(stmt)
    await session.commit()
