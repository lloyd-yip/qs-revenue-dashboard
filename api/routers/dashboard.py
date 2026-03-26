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
from db.queries.metrics_by_rep import get_by_rep, get_daily_activity, get_rep_closes, get_rep_opps
from db.queries.metrics_summary import get_summary
from db.queries.reps import get_reps
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
