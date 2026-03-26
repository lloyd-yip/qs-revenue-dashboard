"""Tier 2 insight queries — pre-computed operational intelligence.

Returns structured insight objects that any consumer (dashboard, orchestration
agent, WhatsApp bot) can format however it wants. The app owns the calculation;
the consumer owns the presentation.
"""

from datetime import date, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from db.queries.metrics_summary import get_summary
from db.queries.metrics_by_rep import get_by_rep
from db.queries.lead_source import get_lead_source_breakdown


def _safe_change_pct(current: float | None, previous: float | None) -> float | None:
    """Calculate percentage change between two values."""
    if current is None or previous is None or previous == 0:
        return None
    return round((current - previous) / previous * 100, 1)


def _flag(current: float | None, previous: float | None, benchmark: float | None) -> str:
    """Generate a machine-readable flag based on trend + benchmark comparison."""
    if current is None:
        return "no_data"

    trending = "stable"
    if previous is not None and previous != 0:
        change = (current - previous) / previous
        if change >= 0.05:
            trending = "improving"
        elif change <= -0.05:
            trending = "declining"

    vs_benchmark = ""
    if benchmark is not None and benchmark != 0:
        diff = (current - benchmark) / benchmark
        if diff >= 0.05:
            vs_benchmark = "above_average"
        elif diff <= -0.05:
            vs_benchmark = "below_average"
        else:
            vs_benchmark = "at_average"

    if vs_benchmark:
        return f"{vs_benchmark}_and_{trending}"
    return trending


async def get_rep_trend_insights(
    session: AsyncSession,
    start: date,
    end: date,
    date_by: str = "appointment",
) -> list[dict]:
    """Compare each rep's current period metrics to their prior period.

    Returns one insight object per rep per key metric that changed significantly.
    """
    period_days = (end - start).days + 1
    prev_start = start - timedelta(days=period_days)
    prev_end = start - timedelta(days=1)

    current_reps = await get_by_rep(session, start, end, date_by)
    previous_reps = await get_by_rep(session, prev_start, prev_end, date_by)

    # Build lookup by rep_id for previous period
    prev_by_id = {r["rep_id"]: r for r in previous_reps}

    # Team averages for benchmark
    team_summary = await get_summary(session, start, end, date_by)

    metrics_to_track = [
        ("show_rate_1st", "1st Call Show Rate", team_summary.get("show_rate_1st")),
        ("show_rate_2nd", "2nd Call Show Rate", team_summary.get("show_rate_2nd")),
        ("close_rate", "Close Rate", team_summary.get("close_rate")),
        ("qualification_rate", "Qualification Rate", team_summary.get("qualification_rate")),
    ]

    insights = []
    for rep in current_reps:
        prev = prev_by_id.get(rep["rep_id"], {})
        for metric_key, metric_label, team_avg in metrics_to_track:
            current_val = rep.get(metric_key)
            prev_val = prev.get(metric_key)
            change_pct = _safe_change_pct(current_val, prev_val)

            # Only surface insights where something meaningful changed (>5%)
            if change_pct is not None and abs(change_pct) >= 5:
                insights.append({
                    "type": "rep_trend",
                    "metric": metric_key,
                    "metric_label": metric_label,
                    "entity": rep["rep_name"],
                    "entity_id": rep["rep_id"],
                    "current_value": current_val,
                    "comparison_value": prev_val,
                    "comparison_period": "previous_period",
                    "change_pct": change_pct,
                    "benchmark": team_avg,
                    "benchmark_label": "team_average",
                    "flag": _flag(current_val, prev_val, team_avg),
                })

    # Sort by absolute change magnitude — biggest movers first
    insights.sort(key=lambda x: abs(x.get("change_pct") or 0), reverse=True)
    return insights


async def get_anomaly_insights(
    session: AsyncSession,
    start: date,
    end: date,
    date_by: str = "appointment",
) -> list[dict]:
    """Flag reps performing significantly below team average on key metrics.

    Uses >1 standard deviation below mean as the threshold.
    """
    reps = await get_by_rep(session, start, end, date_by)
    if len(reps) < 2:
        return []

    metrics_to_check = [
        ("show_rate_1st", "1st Call Show Rate"),
        ("close_rate", "Close Rate"),
        ("qualification_rate", "Qualification Rate"),
    ]

    insights = []
    for metric_key, metric_label in metrics_to_check:
        values = [r[metric_key] for r in reps if r[metric_key] is not None]
        if len(values) < 2:
            continue

        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        std_dev = variance ** 0.5

        if std_dev == 0:
            continue

        threshold = mean - std_dev

        for rep in reps:
            val = rep.get(metric_key)
            if val is not None and val < threshold:
                z_score = round((val - mean) / std_dev, 2)
                insights.append({
                    "type": "anomaly",
                    "metric": metric_key,
                    "metric_label": metric_label,
                    "entity": rep["rep_name"],
                    "entity_id": rep["rep_id"],
                    "current_value": val,
                    "team_average": round(mean, 4),
                    "std_devs_below": round(abs(z_score), 2),
                    "flag": "significantly_below_average",
                })

    insights.sort(key=lambda x: x.get("std_devs_below", 0), reverse=True)
    return insights


async def get_team_summary_insights(
    session: AsyncSession,
    start: date,
    end: date,
    date_by: str = "appointment",
) -> list[dict]:
    """Compare team-level metrics between current and previous period.

    Returns insight objects for every key metric that changed.
    """
    period_days = (end - start).days + 1
    prev_start = start - timedelta(days=period_days)
    prev_end = start - timedelta(days=1)

    current = await get_summary(session, start, end, date_by)
    previous = await get_summary(session, prev_start, prev_end, date_by)

    metrics_to_track = [
        ("show_rate_1st", "1st Call Show Rate"),
        ("show_rate_2nd", "2nd Call Show Rate"),
        ("close_rate", "Close Rate"),
        ("qualification_rate", "Qualification Rate"),
        ("dq_rate", "DQ Rate"),
        ("units_closed", "Units Closed"),
    ]

    insights = []
    for metric_key, metric_label in metrics_to_track:
        curr_val = current.get(metric_key)
        prev_val = previous.get(metric_key)
        change_pct = _safe_change_pct(curr_val, prev_val)

        if curr_val is not None:
            insights.append({
                "type": "team_summary",
                "metric": metric_key,
                "metric_label": metric_label,
                "entity": "team",
                "current_value": curr_val,
                "comparison_value": prev_val,
                "comparison_period": "previous_period",
                "change_pct": change_pct,
                "flag": _flag(curr_val, prev_val, None),
            })

    return insights


async def get_channel_insights(
    session: AsyncSession,
    start: date,
    end: date,
    date_by: str = "appointment",
) -> list[dict]:
    """Compare channel performance between current and previous period.

    Surfaces channels with significant show rate or qual rate shifts.
    """
    period_days = (end - start).days + 1
    prev_start = start - timedelta(days=period_days)
    prev_end = start - timedelta(days=1)

    current_channels = await get_lead_source_breakdown(session, start, end, date_by)
    previous_channels = await get_lead_source_breakdown(session, prev_start, prev_end, date_by)

    prev_by_name = {c["channel"]: c for c in previous_channels}

    insights = []
    for ch in current_channels:
        prev = prev_by_name.get(ch["channel"], {})

        # Show rate insight (using shows/total_ops as proxy)
        curr_show_rate = round(ch["shows"] / ch["total_ops"], 4) if ch["total_ops"] > 0 else None
        prev_show_rate = round(prev["shows"] / prev["total_ops"], 4) if prev.get("total_ops", 0) > 0 else None
        change = _safe_change_pct(curr_show_rate, prev_show_rate)

        if change is not None and abs(change) >= 5:
            insights.append({
                "type": "channel_shift",
                "metric": "show_rate",
                "metric_label": "Show Rate",
                "entity": ch["channel"],
                "current_value": curr_show_rate,
                "comparison_value": prev_show_rate,
                "comparison_period": "previous_period",
                "change_pct": change,
                "volume_current": ch["total_ops"],
                "volume_previous": prev.get("total_ops", 0),
                "flag": _flag(curr_show_rate, prev_show_rate, None),
            })

        # Qual rate insight
        curr_qual = ch.get("qual_rate")
        prev_qual = prev.get("qual_rate")
        qual_change = _safe_change_pct(curr_qual, prev_qual)

        if qual_change is not None and abs(qual_change) >= 5:
            insights.append({
                "type": "channel_shift",
                "metric": "qual_rate",
                "metric_label": "Qualification Rate",
                "entity": ch["channel"],
                "current_value": curr_qual,
                "comparison_value": prev_qual,
                "comparison_period": "previous_period",
                "change_pct": qual_change,
                "flag": _flag(curr_qual, prev_qual, None),
            })

    insights.sort(key=lambda x: abs(x.get("change_pct") or 0), reverse=True)
    return insights


async def get_rep_ranking_insights(
    session: AsyncSession,
    start: date,
    end: date,
    date_by: str = "appointment",
) -> list[dict]:
    """Rank reps by key metrics. Surface top, bottom, and biggest improver.

    Returns structured ranking objects.
    """
    period_days = (end - start).days + 1
    prev_start = start - timedelta(days=period_days)
    prev_end = start - timedelta(days=1)

    current_reps = await get_by_rep(session, start, end, date_by)
    previous_reps = await get_by_rep(session, prev_start, prev_end, date_by)
    prev_by_id = {r["rep_id"]: r for r in previous_reps}

    if not current_reps:
        return []

    insights = []

    # Close rate ranking
    reps_with_close = [r for r in current_reps if r["close_rate"] is not None and r["total_shows"] >= 3]
    if reps_with_close:
        reps_with_close.sort(key=lambda r: r["close_rate"], reverse=True)
        top = reps_with_close[0]
        bottom = reps_with_close[-1]

        insights.append({
            "type": "ranking",
            "metric": "close_rate",
            "metric_label": "Close Rate",
            "rank": "top",
            "entity": top["rep_name"],
            "entity_id": top["rep_id"],
            "current_value": top["close_rate"],
            "units_closed": top["units_closed"],
        })

        if len(reps_with_close) > 1:
            insights.append({
                "type": "ranking",
                "metric": "close_rate",
                "metric_label": "Close Rate",
                "rank": "bottom",
                "entity": bottom["rep_name"],
                "entity_id": bottom["rep_id"],
                "current_value": bottom["close_rate"],
                "units_closed": bottom["units_closed"],
            })

    # Biggest improver (close rate change)
    improvements = []
    for rep in current_reps:
        prev = prev_by_id.get(rep["rep_id"])
        if prev and rep["close_rate"] is not None and prev.get("close_rate") is not None:
            change = _safe_change_pct(rep["close_rate"], prev["close_rate"])
            if change is not None:
                improvements.append({
                    "rep": rep,
                    "change": change,
                })

    if improvements:
        improvements.sort(key=lambda x: x["change"], reverse=True)
        best = improvements[0]
        if best["change"] > 0:
            insights.append({
                "type": "ranking",
                "metric": "close_rate",
                "metric_label": "Close Rate",
                "rank": "most_improved",
                "entity": best["rep"]["rep_name"],
                "entity_id": best["rep"]["rep_id"],
                "current_value": best["rep"]["close_rate"],
                "improvement_pct": best["change"],
            })

    # Total units ranking
    reps_by_units = sorted(current_reps, key=lambda r: r["units_closed"], reverse=True)
    if reps_by_units and reps_by_units[0]["units_closed"] > 0:
        top_units = reps_by_units[0]
        insights.append({
            "type": "ranking",
            "metric": "units_closed",
            "metric_label": "Units Closed",
            "rank": "top",
            "entity": top_units["rep_name"],
            "entity_id": top_units["rep_id"],
            "current_value": top_units["units_closed"],
            "projected_contract_value": top_units["projected_contract_value"],
        })

    return insights
