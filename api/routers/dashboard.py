"""Dashboard API endpoints — no auth required (browser-facing, read-only analytics).

These endpoints serve the static dashboard.html frontend. They wrap the same
underlying query functions as /api/metrics/* but are intentionally unauthenticated
so the browser can call them directly without embedding a token.

Protected endpoints (/api/metrics/*, /api/sync/*) remain unchanged.
"""

from datetime import date, datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas.responses import (
    ByRepResponse,
    ChannelClosesResponse,
    ChannelQualityResponse,
    ClosedDealRow,
    ComplianceResponse,
    DailyActivityResponse,
    FunnelInputsResponse,
    InsightsResponse,
    LateViolationResponse,
    LeadSourceResponse,
    MetaMixin,
    QualificationResponse,
    RepLateResponse,
    RepOppsResponse,
    RepsResponse,
    SaveCompRequest,
    SaveSLWAWeeklyInputRequest,
    SaveSpendRequest,
    SLWADashboardResponse,
    SummaryResponse,
    TimeSeriesResponse,
)
from db.queries.expenses import (
    get_available_periods,
    get_expenses_for_period,
    upsert_expense_line_items,
)
from db.queries.revenue import (
    get_available_revenue_periods,
    get_revenue_for_period,
    get_all_revenue_periods_summary,
    upsert_revenue_line_items,
)
from db.queries.compliance import (
    get_compliance_by_rep,
    get_compliance_failures,
    get_compliance_summary,
    get_rep_late_rates,
    get_rep_late_violations,
)
from db.queries.lead_source import (
    get_channel_closes,
    get_channel_quality_breakdown,
    get_lead_source_breakdown,
    get_qualification_breakdown,
)
from db.queries.data_quality import get_data_quality_issues
from db.queries.debug_drilldown import get_drilldown_opps
from db.queries.funnel_economics import get_auto_funnel_economics, get_period_inputs, upsert_marketing_spend, upsert_rep_compensations
from db.queries.upsell_metrics import get_upsell_summary, get_upsell_by_rep
from db.queries.metrics_by_rep import get_by_rep, get_daily_activity, get_rep_closes, get_rep_opps
from db.queries.metrics_summary import get_summary
from db.queries.pipeline_intelligence import get_pipeline_intelligence, get_segment_closes
from db.queries.dead_deals import get_dead_deals_data
from db.queries.stage_snapshot import get_stage_snapshot
from db.queries.reps import get_reps
from db.queries.deal_matches import (
    get_deal_matches,
    get_deal_match_summary,
    get_last_match_run,
)
from db.queries.wise_transfers import get_wise_transfers_for_deal, get_all_wise_transfers
from db.queries.slwa import get_slwa_closes, get_slwa_weekly_dashboard, upsert_slwa_weekly_input
from db.queries.sync_status import get_recent_sync_runs
from db.queries.time_series import get_time_series
from db.session import get_db

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


def _meta(start: date, end: date, date_by: str) -> MetaMixin:
    return MetaMixin(
        date_start=start,
        date_end=end,
        date_by=date_by,
        generated_at=datetime.now(timezone.utc),
    )


def _date_params(
    start: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end: date = Query(..., description="End date (YYYY-MM-DD)"),
    date_by: str = Query("appointment", description="Date dimension: 'appointment' or 'created'"),
) -> tuple[date, date, str]:
    if date_by not in ("appointment", "booked", "created"):
        date_by = "appointment"
    return start, end, date_by


@router.get("/reps", response_model=RepsResponse)
async def reps(db: AsyncSession = Depends(get_db)):
    """All reps — populates the rep filter dropdown."""
    data = await get_reps(db)
    return RepsResponse(data=data)


@router.get("/summary", response_model=SummaryResponse)
async def summary(
    rep_id: str | None = Query(None),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Team-level KPI summary cards."""
    start, end, date_by = params
    data = await get_summary(db, start, end, date_by, rep_id)
    return SummaryResponse(data=data, meta=_meta(start, end, date_by))


@router.get("/by-rep", response_model=ByRepResponse)
async def by_rep(
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """All KPIs per rep — for the rep breakdown table/chart."""
    start, end, date_by = params
    data = await get_by_rep(db, start, end, date_by)
    return ByRepResponse(data=data, meta=_meta(start, end, date_by))


@router.get("/time-series", response_model=TimeSeriesResponse)
async def time_series(
    granularity: str = Query("week", description="day | week | month"),
    rep_id: str | None = Query(None),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Show rate over time — for the line chart."""
    start, end, date_by = params
    data = await get_time_series(db, start, end, granularity, date_by, rep_id)
    return TimeSeriesResponse(data=data, meta=_meta(start, end, date_by))


@router.get("/channels", response_model=LeadSourceResponse)
async def channels(
    rep_id: str | None = Query(None),
    group_by: str = Query("channel", description="Group rows by 'channel' or 'funnel'"),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Channel distribution + conversion metrics — for the channel tab.

    group_by='funnel' splits by first-call funnel (webinar / outreach / referral).
    """
    start, end, date_by = params
    if group_by not in ("channel", "funnel"):
        group_by = "channel"
    data = await get_lead_source_breakdown(db, start, end, date_by, rep_id, group_by=group_by)
    return LeadSourceResponse(data=data, meta=_meta(start, end, date_by))


@router.get("/channel-quality", response_model=ChannelQualityResponse)
async def channel_quality(
    rep_id: str | None = Query(None),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Lead quality breakdown per channel — for the grouped bar chart."""
    start, end, date_by = params
    data = await get_channel_quality_breakdown(db, start, end, date_by, rep_id)
    return ChannelQualityResponse(data=data, meta=_meta(start, end, date_by))


@router.get("/qualification", response_model=QualificationResponse)
async def qualification(
    rep_id: str | None = Query(None),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Qual field breakdowns — for the lead quality tab detail section."""
    start, end, date_by = params
    data = await get_qualification_breakdown(db, start, end, date_by, rep_id)
    return QualificationResponse(data=data, meta=_meta(start, end, date_by))


@router.get("/channels/closes", response_model=ChannelClosesResponse)
async def channel_closes(
    channel: str = Query(..., description="Channel name (use 'Unknown' for null)"),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Closed deals for a specific channel — drill-down popup."""
    start, end, date_by = params
    data = await get_channel_closes(db, channel, start, end, date_by)
    return ChannelClosesResponse(data=data)


@router.get("/slwa/weekly", response_model=SLWADashboardResponse)
async def slwa_weekly(
    scope: str = Query("overall", description="overall | slack | whatsapp | sms"),
    rep_id: str | None = Query(None),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Slack / WhatsApp / SMS weekly dashboard data."""
    start, end, date_by = params
    data = await get_slwa_weekly_dashboard(db, scope, start, end, rep_id)
    return SLWADashboardResponse(data=data, meta=_meta(start, end, date_by))


@router.get("/slwa/closes", response_model=ChannelClosesResponse)
async def slwa_closes(
    scope: str = Query("overall", description="overall | slack | whatsapp | sms"),
    rep_id: str | None = Query(None),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Closed deals for a Slack / WhatsApp / SMS scope."""
    start, end, date_by = params
    data = await get_slwa_closes(db, scope, start, end, rep_id)
    return ChannelClosesResponse(data=[ClosedDealRow(**row) for row in data])


@router.post("/slwa/manual-entry")
async def save_slwa_manual_entry(
    body: SaveSLWAWeeklyInputRequest,
    db: AsyncSession = Depends(get_db),
):
    """Save or overwrite manual weekly Slack / WhatsApp / SMS dashboard inputs."""
    await upsert_slwa_weekly_input(
        db,
        channel_key=body.channel_key,
        section=body.section,
        week_start=body.week_start,
        message_sent=body.message_sent,
        links_sent=body.links_sent,
        changes_to_funnel=(body.changes_to_funnel.strip() if body.changes_to_funnel and body.changes_to_funnel.strip() else None),
        copy=(body.copy_text.strip() if body.copy_text and body.copy_text.strip() else None),
        groups=(body.groups.strip() if body.groups and body.groups.strip() else None),
    )
    return {"ok": True}


@router.get("/closes", response_model=ChannelClosesResponse)
async def closes(
    rep_id: str | None = Query(None, description="GHL opportunity owner ID — omit for all reps"),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Closed deals — all reps or a specific rep. Drill-down popup."""
    start, end, date_by = params
    data = await get_rep_closes(db, rep_id, start, end, date_by)
    return ChannelClosesResponse(data=data)


@router.get("/rep/opps", response_model=RepOppsResponse)
async def rep_opps(
    rep_id: str | None = Query(None, description="GHL opportunity owner ID"),
    opp_type: str = Query("booked", description="'booked' or 'showed'"),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Booked or showed 1st-call opps for a rep — drill-down modal."""
    start, end, date_by = params
    data = await get_rep_opps(db, rep_id, opp_type, start, end, date_by)
    return RepOppsResponse(data=data)


@router.get("/compliance", response_model=ComplianceResponse)
async def compliance(
    rep_id: str | None = Query(None),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Compliance — summary KPIs, per-rep bar chart data, and detail rows with GHL links."""
    start, end, date_by = params
    summary = await get_compliance_summary(db, start, end, rep_id, date_by)
    by_rep = await get_compliance_by_rep(db, start, end, date_by)
    failures = await get_compliance_failures(db, start, end, rep_id, date_by)
    return ComplianceResponse(
        summary=summary,
        by_rep=by_rep,
        failures=failures,
        meta=_meta(start, end, date_by),
    )


@router.get("/compliance/late-rates", response_model=RepLateResponse)
async def compliance_late_rates(
    db: AsyncSession = Depends(get_db),
):
    """Per-rep late-logging rates — how often reps take >12h to log call outcomes."""
    data = await get_rep_late_rates(db)
    return RepLateResponse(data=data)


@router.get("/compliance/late-violations", response_model=LateViolationResponse)
async def compliance_late_violations(
    rep_name: str | None = Query(None, description="Filter to a specific rep by name"),
    db: AsyncSession = Depends(get_db),
):
    """Individual opp rows for the late-violation drill-down modal."""
    data = await get_rep_late_violations(db, rep_name)
    return LateViolationResponse(data=data)


@router.get("/daily-activity", response_model=DailyActivityResponse)
async def daily_activity(
    rep_id: str | None = Query(None, description="GHL opportunity owner ID — omit for team total"),
    start_date: date | None = Query(None, description="ISO date — start of 7-day window (defaults to 6 days ago)"),
    end_date: date | None = Query(None, description="ISO date — end of 7-day window (defaults to today)"),
    db: AsyncSession = Depends(get_db),
):
    """Day-by-day booked / showed / qual for a 7-day window."""
    data = await get_daily_activity(db, rep_id, start_date, end_date)
    return DailyActivityResponse(data=data)


# ── Tier 2: Insight Endpoints ────────────────────────────────────────────────

from db.queries.insights import (
    get_rep_trend_insights,
    get_anomaly_insights,
    get_team_summary_insights,
    get_channel_insights,
    get_rep_ranking_insights,
)


@router.get("/insights/rep-trends", response_model=InsightsResponse)
async def rep_trends(
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Rep performance trends — which reps improved or declined vs. prior period."""
    start, end, date_by = params
    data = await get_rep_trend_insights(db, start, end, date_by)
    return InsightsResponse(data=data, meta=_meta(start, end, date_by))


@router.get("/insights/anomalies", response_model=InsightsResponse)
async def anomalies(
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Anomaly detection — reps performing significantly below team average."""
    start, end, date_by = params
    data = await get_anomaly_insights(db, start, end, date_by)
    return InsightsResponse(data=data, meta=_meta(start, end, date_by))


@router.get("/insights/team-summary", response_model=InsightsResponse)
async def team_summary_insights(
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Team week-to-date summary — how is the team pacing vs. prior period."""
    start, end, date_by = params
    data = await get_team_summary_insights(db, start, end, date_by)
    return InsightsResponse(data=data, meta=_meta(start, end, date_by))


@router.get("/insights/channels", response_model=InsightsResponse)
async def channel_insights(
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Channel performance shifts — which channels improved or declined."""
    start, end, date_by = params
    data = await get_channel_insights(db, start, end, date_by)
    return InsightsResponse(data=data, meta=_meta(start, end, date_by))


@router.get("/insights/rankings", response_model=InsightsResponse)
async def rep_rankings(
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Rep rankings — top performer, bottom performer, biggest improver."""
    start, end, date_by = params
    data = await get_rep_ranking_insights(db, start, end, date_by)
    return InsightsResponse(data=data, meta=_meta(start, end, date_by))



# ── Pipeline Intelligence ─────────────────────────────────────────────────────

VALID_GROUP_BY = {"rep", "channel", "funnel", "lead_quality", "intent", "indoctrination",
                  "business_fit", "pain_goal", "industry", "current_revenue"}


@router.get("/pipeline-intelligence/closes")
async def pi_segment_closes(
    group_by: str = Query("rep", description="Same values as /pipeline-intelligence"),
    segment: str = Query(..., description="Segment value to drill into (e.g. 'Consulting & Advisory')"),
    rep_id: str | None = Query(None),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Won deal details for a specific Pipeline Intelligence segment."""
    start, end, date_by = params
    if group_by not in VALID_GROUP_BY:
        group_by = "rep"
    data = await get_segment_closes(db, group_by, segment, start, end, date_by, rep_id)
    return {"segment": segment, "dimension": group_by, "count": len(data), "data": data}


@router.get("/dead-deals")
async def dead_deals(
    rep_id: str | None = Query(None),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Dead Deals tab — DQ'd and Lost opportunities with reasons, by-rep, by-channel breakdowns."""
    start, end, date_by = params
    data = await get_dead_deals_data(db, start, end, date_by, rep_id)
    return {"data": data, "meta": _meta(start, end, date_by).__dict__}


@router.get("/pipeline-intelligence")
async def pipeline_intelligence(
    group_by: str = Query("rep", description="rep | channel | lead_quality | intent | indoctrination"),
    rep_id: str | None = Query(None, description="Filter to a specific rep (only applies when group_by != 'rep')"),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Segment all metrics by any dimension — for the Pipeline Intelligence page."""
    start, end, date_by = params
    if group_by not in VALID_GROUP_BY:
        group_by = "rep"
    data = await get_pipeline_intelligence(db, group_by, start, end, date_by, rep_id)
    return {"data": data, "meta": _meta(start, end, date_by).__dict__}


# ── Follow-up call show rate by lead quality ─────────────────────────────────

from db.queries.followup_quality import get_followup_show_rate_by_quality


@router.get("/followup-by-quality")
async def followup_by_quality(
    rep_id: str | None = Query(None),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Follow-up (2nd) call show rate broken down by lead quality."""
    start, end, date_by = params
    data = await get_followup_show_rate_by_quality(db, start, end, date_by, rep_id)
    return {"data": data, "meta": _meta(start, end, date_by).__dict__}


# ── Debug drill-down ─────────────────────────────────────────────────────────


@router.get("/debug")
async def debug_drilldown(
    metric: str = Query(..., description="Metric key, e.g. 'calls_booked_1st', 'shows_1st', 'units_closed'"),
    rep_id: str | None = Query(None),
    channel: str | None = Query(None, description="Restrict to one canonical channel (Lead Quality by Channel drill-downs)"),
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Debug drill-down — returns every opportunity behind a dashboard KPI number."""
    start, end, date_by = params
    data = await get_drilldown_opps(db, metric, start, end, date_by, rep_id, channel)
    return {"metric": metric, "count": len(data), "data": data, "meta": _meta(start, end, date_by).__dict__}


# ── Data Quality Audit ────────────────────────────────────────────────────────

@router.get("/data-quality")
async def data_quality(
    rep_id: str | None = Query(None),
    start: date | None = Query(None),
    end: date | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Data quality audit — non-excluded opps with GHL inconsistencies, optionally filtered by created date."""
    return await get_data_quality_issues(db, rep_id, start, end)


# ── Lookups (calendar names, pipeline stages) ─────────────────────────────────

from db.queries.debug_drilldown import STAGE_NAMES as FALLBACK_STAGE_NAMES
from sync.ghl_client import GHLClient

# Simple in-memory cache — refreshed on first request and on manual trigger
_lookup_cache: dict[str, dict] = {}


@router.get("/lookups")
async def get_lookups():
    """Return calendar names and pipeline stage names.

    Fetches dynamically from GHL API on first call, then caches.
    Falls back to hardcoded map if the API call fails (e.g. PIT scope missing).
    """
    if _lookup_cache:
        return _lookup_cache

    ghl = GHLClient()

    # Fetch calendars
    calendars = await ghl.get_calendars()

    # Fetch pipeline stages
    stages = await ghl.get_pipeline_stages()
    # Merge fallback stage names for any IDs not returned by API
    merged_stages = {**FALLBACK_STAGE_NAMES, **stages}

    _lookup_cache["calendars"] = calendars
    _lookup_cache["stages"] = merged_stages

    return _lookup_cache


@router.post("/lookups/refresh")
async def refresh_lookups():
    """Force-refresh the calendar/stage lookup cache."""
    _lookup_cache.clear()
    return await get_lookups()


# ── Sync History ──────────────────────────────────────────────────────────────

@router.get("/sync-history")
async def sync_history(
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    """Recent sync runs — for the sync history page."""
    data = await get_recent_sync_runs(db, limit)
    return {"data": data}


# ── Funnel Economics Inputs ───────────────────────────────────────────────────

@router.get("/funnel-inputs", response_model=FunnelInputsResponse)
async def funnel_inputs(
    start: date = Query(...),
    end: date = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Return saved marketing spend + rep comp for the exact period.

    Returns data=null if nothing has been saved for this period yet.
    """
    data = await get_period_inputs(db, start, end)
    if data is None:
        return FunnelInputsResponse(data=None)
    return FunnelInputsResponse(data=data)


@router.post("/funnel-inputs/spend")
async def save_spend(body: SaveSpendRequest, db: AsyncSession = Depends(get_db)):
    """Save or overwrite total marketing spend for a period."""
    await upsert_marketing_spend(db, body.start, body.end, body.amount)
    return {"ok": True}


@router.post("/funnel-inputs/comp")
async def save_comp(body: SaveCompRequest, db: AsyncSession = Depends(get_db)):
    """Save or overwrite rep compensation for a period."""
    reps = [r.model_dump() for r in body.reps]
    await upsert_rep_compensations(db, body.start, body.end, reps)
    return {"ok": True}


@router.get("/funnel-economics")
async def funnel_economics(
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Auto-computed cost card metrics for the primary webinar invite funnel.

    Pulls marketing spend + sales comp from Xero expense data and GHL opportunity
    metrics (calls booked / shows / qual shows / closed) from the database.

    Only counts opportunities whose canonical_channel is flagged is_primary_funnel=True
    in source_normalization (currently "Webinar Live").

    Returns null for cost fields when no expense data exists for the period —
    this is expected until Xero sync has been run for the selected month.

    VERIFICATION: GET /api/dashboard/funnel-economics?start=2026-05-01&end=2026-05-31
    Response should include marketing_spend, sales_comp, and the 4 cost card values
    once Xero has been synced for that month.
    """
    start, end, date_by = params
    data = await get_auto_funnel_economics(db, start, end, date_by)
    return {"data": data, "meta": _meta(start, end, date_by).model_dump()}


# ── Upsells (Client Delivery Revenue Pipeline) ────────────────────────────────

@router.get("/upsells")
async def upsells(
    start: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end: date = Query(..., description="End date (YYYY-MM-DD)"),
    db: AsyncSession = Depends(get_db),
):
    """Client Delivery Revenue Pipeline — outreach → call → close funnel metrics.

    Date anchor is opportunity created_at_ghl (not appointment date).
    Returns summary KPIs + per-rep breakdown.
    """
    summary = await get_upsell_summary(db, start, end)
    by_rep  = await get_upsell_by_rep(db, start, end)
    meta = {
        "date_start": start.isoformat(),
        "date_end":   end.isoformat(),
        "date_by":    "created",
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    return {"data": {"summary": summary, "by_rep": by_rep}, "meta": meta}


# ── Stage Snapshot (Hot / Warm List) ──────────────────────────────────────────

@router.get("/stage-snapshot")
async def stage_snapshot(db: AsyncSession = Depends(get_db)):
    """Current-state Hot List + Warm List by rep with deal values.

    No date params — reflects the live GHL pipeline right now.
    Used by the weekly report for projected pipeline value calculations.

    Discount rates applied by the consumer (weekly report):
      Hot List  (Verbal Commit)         → 50% of deal value
      Warm List (1st/2nd Call done)     → 10% of deal value

    Also surfaces missing_value_count per rep/bucket so the report can flag
    reps who haven't filled in deal value on their Hot/Warm opps.
    """
    data = await get_stage_snapshot(db)
    return {
        "data": data,
        "meta": {"generated_at": datetime.now(timezone.utc).isoformat()},
    }


# ── Expenses ──────────────────────────────────────────────────────────────────

@router.get("/expenses/periods")
async def expense_periods(db: AsyncSession = Depends(get_db)):
    """Return all periods that have expense data loaded — drives the month dropdown."""
    periods = await get_available_periods(db)
    return {"data": periods}


@router.get("/expenses")
async def expenses(
    period_start: date = Query(..., description="Period start (YYYY-MM-DD)"),
    period_end: date = Query(..., description="Period end (YYYY-MM-DD)"),
    db: AsyncSession = Depends(get_db),
):
    """Return classified expense line items grouped by bucket for a given period."""
    data = await get_expenses_for_period(db, period_start, period_end)
    return {
        "data": data,
        "meta": {"generated_at": datetime.now(timezone.utc).isoformat()},
    }


class ExpenseItemInput(BaseModel):
    bucket: str
    vendor: str
    amount: float
    is_approximate: bool = False
    notes: str | None = None


class UpsertExpensesRequest(BaseModel):
    period_start: date
    period_end: date
    items: list[ExpenseItemInput]
    replace: bool = False


@router.post("/expenses/upsert")
async def upsert_expenses(body: UpsertExpensesRequest, db: AsyncSession = Depends(get_db)):
    """Load or overwrite expense line items for a period. Used during monthly Xero pull.

    Set replace=true to wipe the period clean before inserting (safe monthly refresh).
    """
    count = await upsert_expense_line_items(
        db, body.period_start, body.period_end,
        [i.model_dump() for i in body.items],
        replace=body.replace,
    )
    return {"ok": True, "rows_upserted": count}


# ── Revenue ────────────────────────────────────────────────────────────────────

@router.get("/revenue/periods")
async def revenue_periods(db: AsyncSession = Depends(get_db)):
    """Return all periods that have revenue data loaded — drives the month dropdown."""
    periods = await get_available_revenue_periods(db)
    return {"data": periods}


@router.get("/revenue/summary")
async def revenue_summary(db: AsyncSession = Depends(get_db)):
    """Return cash collected + splitit AR totals for all months — drives the P&L multi-month view."""
    data = await get_all_revenue_periods_summary(db)
    return {
        "data": data,
        "meta": {"generated_at": datetime.now(timezone.utc).isoformat()},
    }


@router.get("/revenue")
async def revenue(
    period_start: date = Query(..., description="Period start (YYYY-MM-DD)"),
    period_end: date = Query(..., description="Period end (YYYY-MM-DD)"),
    db: AsyncSession = Depends(get_db),
):
    """Return revenue line items grouped by category for a given period."""
    data = await get_revenue_for_period(db, period_start, period_end)
    return {
        "data": data,
        "meta": {"generated_at": datetime.now(timezone.utc).isoformat()},
    }


class RevenueItemInput(BaseModel):
    source: str
    category: str
    product_type: str
    amount: float
    payment_count: int = 0
    notes: str | None = None


class UpsertRevenueRequest(BaseModel):
    period_start: date
    period_end: date
    items: list[RevenueItemInput]
    replace: bool = False


@router.post("/revenue/upsert")
async def upsert_revenue(body: UpsertRevenueRequest, db: AsyncSession = Depends(get_db)):
    """Load or overwrite revenue line items for a period. Used during monthly Whop sync.

    Set replace=true to wipe the period clean before inserting (safe monthly refresh).
    """
    count = await upsert_revenue_line_items(
        db, body.period_start, body.period_end,
        [i.model_dump() for i in body.items],
        replace=body.replace,
    )
    return {"ok": True, "rows_upserted": count}


# ── Deal ↔ Whop Reconciliation ──────────────────────────────────────────────

@router.get("/deals/matches")
async def deal_matches(
    month_start: date | None = Query(None, description="Filter by close date from (YYYY-MM-DD)"),
    month_end: date | None = Query(None, description="Filter by close date to (YYYY-MM-DD)"),
    owner_name: str | None = Query(None, description="Filter by rep name"),
    confidence: str | None = Query(None, description="Filter: high | medium | low | unmatched"),
    db: AsyncSession = Depends(get_db),
):
    """Return deal-Whop match rows for the Deals dashboard.

    Verify: GET /api/dashboard/deals/matches → should return array of deal objects
    with fields: ghl_opportunity_name, match_confidence, total_paid, remaining_ar.
    Silent failure: empty array when won deals exist → run-match hasn't been triggered yet.
    """
    rows = await get_deal_matches(db, month_start, month_end, owner_name, confidence)
    return {
        "data": rows,
        "meta": {"generated_at": datetime.now(timezone.utc).isoformat(), "count": len(rows)},
    }


@router.get("/deals/summary")
async def deal_summary(db: AsyncSession = Depends(get_db)):
    """Aggregate stats for the deals page header cards.

    Verify: GET /api/dashboard/deals/summary → returns total_deals, match_rate_pct,
    total_contract_value, total_remaining_ar. If total_deals=0, run-match first.
    """
    summary = await get_deal_match_summary(db)
    last_run = await get_last_match_run(db)
    return {
        "data": summary,
        "last_match_run": last_run,
        "meta": {"generated_at": datetime.now(timezone.utc).isoformat()},
    }


@router.post("/deals/run-match")
async def run_deal_match(db: AsyncSession = Depends(get_db)):
    """Trigger GHL↔Whop matching engine. Runs synchronously (may take 2-4 min).

    Requires WHOP_API_KEY set in Railway env vars. Idempotent — safe to run
    multiple times. Confirmed matches (is_confirmed=True) are never overwritten.

    Verify: POST /api/dashboard/deals/run-match → {"ok": true, "stats": {...}}
    stats.matched_high should be > 0 if deals exist and WHOP_API_KEY is correct.
    Silent failure: stats.errors > 0 → check Railway logs for the traceback.
    """
    import logging
    from config import settings

    logger = logging.getLogger(__name__)

    if not settings.whop_api_key:
        return {
            "ok": False,
            "error": "WHOP_API_KEY not set in Railway env vars. Add it and redeploy.",
        }

    from sync.match_deals_whop import run_matching
    try:
        stats = await run_matching()
        return {"ok": True, "stats": stats}
    except Exception as exc:
        logger.error(f"Deal matching failed: {exc}", exc_info=True)
        return {"ok": False, "error": str(exc), "stats": {}}


# ── Wise / Xero bank transfer endpoints ──────────────────────────────────────

@router.get("/deals/wise-transfers")
async def get_deals_wise_transfers(
    ghl_opportunity_id: str | None = Query(default=None, description="Filter to one deal"),
    db: AsyncSession = Depends(get_db),
):
    """Return Wise bank transfers from xero_bank_transfers table.

    If ghl_opportunity_id is provided, returns only transfers linked to that deal.
    Otherwise returns all transfers (most recent first, limit 500).

    Verification: after running POST /xero/sync-wise-transfers, call this endpoint.
    If you see the transfers you expect (e.g. FRANCHISE PIPELINE SOLUTIONS LLC $17,995),
    the sync worked. If the list is empty, the sync hasn't run or Xero auth is missing.
    """
    if ghl_opportunity_id:
        rows = await get_wise_transfers_for_deal(db, ghl_opportunity_id)
    else:
        rows = await get_all_wise_transfers(db)
    return {"transfers": rows, "count": len(rows)}


# ── Manual Deal Match (Whop / Stripe) ────────────────────────────────────────

from db.models import DealWhopMatch
from db.session import AsyncSessionLocal


class ManualDealMatchInput(BaseModel):
    """Manually link a GHL deal to a Whop membership or Stripe customer."""
    ghl_opportunity_id: str
    whop_membership_id: Optional[str] = None
    whop_email: Optional[str] = None
    stripe_customer_email: Optional[str] = None
    match_method: str = "manual"
    notes: Optional[str] = None
    # Optional payment metrics — fill these to populate numbers directly
    upfront_cash: Optional[float] = None
    total_paid: Optional[float] = None
    total_contract_value: Optional[float] = None
    remaining_ar: Optional[float] = None
    payment_count: Optional[int] = None
    is_financing: Optional[bool] = None
    first_payment_date: Optional[str] = None


class ManualDealMatchResult(BaseModel):
    updated: int
    ghl_opportunity_id: str
    ghl_opportunity_name: Optional[str] = None
    match_confidence: str = "high"
    whop_membership_id: Optional[str] = None
    whop_email: Optional[str] = None
    error: Optional[str] = None


@router.post("/deals/manual-match")
async def manual_deal_match(body: ManualDealMatchInput):
    """Manually link a GHL deal to a Whop membership or Stripe customer.

    Updates the existing deal_whop_matches row (created by the auto-matcher)
    with match_confidence='high', is_confirmed=True so the auto-matcher
    never overwrites it.

    Verification: POST with a known ghl_opportunity_id → response.updated=1.
    Then GET /deals/matches?confidence=high → the deal should appear.
    """
    async with AsyncSessionLocal() as session:
        row = (await session.execute(
            select(DealWhopMatch)
            .where(DealWhopMatch.ghl_opportunity_id == body.ghl_opportunity_id)
        )).scalar_one_or_none()

        if not row:
            raise HTTPException(404, f"Deal {body.ghl_opportunity_id} not found in deal_whop_matches")

        row.match_confidence = "high"
        row.match_method = body.match_method
        row.is_confirmed = True
        row.confirmed_by = "claude-manual"
        row.confirmed_at = datetime.now(timezone.utc)

        if body.whop_membership_id:
            row.whop_membership_id = body.whop_membership_id
        if body.whop_email:
            row.whop_email = body.whop_email
        if body.stripe_customer_email:
            # Store Stripe email in whop_email field (shared payment email field)
            row.whop_email = body.stripe_customer_email
            row.match_method = body.match_method or "manual_stripe"

        # Payment metrics (optional — fill directly without waiting for matcher)
        if body.upfront_cash is not None:
            row.upfront_cash = body.upfront_cash
        if body.total_paid is not None:
            row.total_paid = body.total_paid
        if body.total_contract_value is not None:
            row.total_contract_value = body.total_contract_value
        if body.remaining_ar is not None:
            row.remaining_ar = body.remaining_ar
        if body.payment_count is not None:
            row.payment_count = body.payment_count
        if body.is_financing is not None:
            row.is_financing = body.is_financing
        if body.first_payment_date:
            row.first_payment_date = date.fromisoformat(body.first_payment_date)
        row.metrics_updated_at = datetime.now(timezone.utc)

        await session.commit()

        return ManualDealMatchResult(
            updated=1,
            ghl_opportunity_id=body.ghl_opportunity_id,
            ghl_opportunity_name=row.ghl_opportunity_name,
            match_confidence="high",
            whop_membership_id=row.whop_membership_id,
            whop_email=row.whop_email,
        )


@router.post("/deals/manual-match-batch")
async def manual_deal_match_batch(links: list[ManualDealMatchInput]):
    """Batch-link multiple GHL deals to Whop/Stripe. Same logic as /deals/manual-match."""
    results = []
    async with AsyncSessionLocal() as session:
        for body in links:
            row = (await session.execute(
                select(DealWhopMatch)
                .where(DealWhopMatch.ghl_opportunity_id == body.ghl_opportunity_id)
            )).scalar_one_or_none()

            if not row:
                results.append(ManualDealMatchResult(
                    updated=0,
                    ghl_opportunity_id=body.ghl_opportunity_id,
                    error=f"Deal {body.ghl_opportunity_id} not found",
                ))
                continue

            row.match_confidence = "high"
            row.match_method = body.match_method
            row.is_confirmed = True
            row.confirmed_by = "claude-manual"
            row.confirmed_at = datetime.now(timezone.utc)

            if body.whop_membership_id:
                row.whop_membership_id = body.whop_membership_id
            if body.whop_email:
                row.whop_email = body.whop_email
            if body.stripe_customer_email:
                row.whop_email = body.stripe_customer_email
                if body.match_method == "manual":
                    row.match_method = "manual_stripe"

            # Payment metrics
            if body.upfront_cash is not None:
                row.upfront_cash = body.upfront_cash
            if body.total_paid is not None:
                row.total_paid = body.total_paid
            if body.total_contract_value is not None:
                row.total_contract_value = body.total_contract_value
            if body.remaining_ar is not None:
                row.remaining_ar = body.remaining_ar
            if body.payment_count is not None:
                row.payment_count = body.payment_count
            if body.is_financing is not None:
                row.is_financing = body.is_financing
            if body.first_payment_date:
                row.first_payment_date = date.fromisoformat(body.first_payment_date)
            row.metrics_updated_at = datetime.now(timezone.utc)

            results.append(ManualDealMatchResult(
                updated=1,
                ghl_opportunity_id=body.ghl_opportunity_id,
                ghl_opportunity_name=row.ghl_opportunity_name,
                match_confidence="high",
                whop_membership_id=row.whop_membership_id,
                whop_email=row.whop_email,
            ))

        await session.commit()

    return {"results": results, "total_updated": sum(r.updated for r in results)}


@router.delete("/deals/match/{ghl_opportunity_id}")
async def delete_deal_match(ghl_opportunity_id: str):
    """Delete a duplicate deal_whop_matches row. Use only for cleaning up duplicates."""
    async with AsyncSessionLocal() as session:
        row = (await session.execute(
            select(DealWhopMatch)
            .where(DealWhopMatch.ghl_opportunity_id == ghl_opportunity_id)
        )).scalar_one_or_none()
        if not row:
            raise HTTPException(404, f"Deal {ghl_opportunity_id} not found")
        name = row.ghl_opportunity_name
        await session.delete(row)
        await session.commit()
        return {"deleted": True, "ghl_opportunity_id": ghl_opportunity_id, "name": name}
