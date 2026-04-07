"""Dashboard API endpoints — no auth required (browser-facing, read-only analytics).

These endpoints serve the static dashboard.html frontend. They wrap the same
underlying query functions as /api/metrics/* but are intentionally unauthenticated
so the browser can call them directly without embedding a token.

Protected endpoints (/api/metrics/*, /api/sync/*) remain unchanged.
"""

from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas.responses import (
    ByRepResponse,
    ChannelClosesResponse,
    ChannelQualityResponse,
    ComplianceResponse,
    DailyActivityResponse,
    FunnelInputsResponse,
    SaveCompRequest,
    SaveSpendRequest,
    InsightsResponse,
    LateViolationResponse,
    LeadSourceResponse,
    MetaMixin,
    QualificationResponse,
    RepLateResponse,
    RepOppsResponse,
    RepsResponse,
    SummaryResponse,
    TimeSeriesResponse,
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
from db.queries.metrics_by_rep import get_by_rep, get_daily_activity, get_rep_closes, get_rep_opps
from db.queries.pipeline_intelligence import get_pipeline_intelligence
from db.queries.metrics_summary import get_summary
from db.queries.reps import get_reps
from db.queries.sync_status import get_recent_sync_runs
from db.queries.time_series import get_time_series
from db.queries.funnel_economics import get_period_inputs, upsert_marketing_spend, upsert_rep_compensations
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
    if date_by not in ("appointment", "created"):
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
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Channel distribution + conversion metrics — for the channel tab."""
    start, end, date_by = params
    data = await get_lead_source_breakdown(db, start, end, date_by, rep_id)
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
    summary = await get_compliance_summary(db, start, end, rep_id)
    by_rep = await get_compliance_by_rep(db, start, end)
    failures = await get_compliance_failures(db, start, end, rep_id)
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

VALID_GROUP_BY = {"rep", "channel", "lead_quality", "intent", "indoctrination"}


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
    params: tuple = Depends(_date_params),
    db: AsyncSession = Depends(get_db),
):
    """Debug drill-down — returns every opportunity behind a dashboard KPI number."""
    start, end, date_by = params
    data = await get_drilldown_opps(db, metric, start, end, date_by, rep_id)
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

