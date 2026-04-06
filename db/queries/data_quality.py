"""Data quality audit — scans non-excluded opportunities for GHL inconsistencies.

Optionally filters by opportunity created date range.
Reuses the anomaly detection logic from debug_drilldown.py.
"""

from datetime import date

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity
from db.queries.debug_drilldown import (
    _DRILLDOWN_COLUMNS,
    _detect_anomalies,
    _row_to_dict,
)


async def get_data_quality_issues(
    session: AsyncSession,
    rep_id: str | None = None,
    start: date | None = None,
    end: date | None = None,
) -> dict:
    """Return all opportunities with data quality issues.

    Optionally filters by created_at_ghl date range.
    Returns {issues: [...], summary: {anomaly_id: count}, total: int}.
    """
    filters = [Opportunity.is_excluded.is_(False)]
    if rep_id:
        filters.append(Opportunity.opportunity_owner_id == rep_id)
    if start:
        filters.append(func.date(Opportunity.created_at_ghl) >= start)
    if end:
        filters.append(func.date(Opportunity.created_at_ghl) <= end)

    result = await session.execute(
        select(*_DRILLDOWN_COLUMNS)
        .where(and_(*filters))
        .order_by(Opportunity.call1_appointment_date.desc().nulls_last())
    )

    issues = []
    summary: dict[str, int] = {}

    for r in result.all():
        row = _row_to_dict(r)
        anomalies = _detect_anomalies(row)
        if anomalies:
            row["anomalies"] = anomalies
            issues.append(row)
            for a in anomalies:
                summary[a["id"]] = summary.get(a["id"], 0) + 1

    return {"issues": issues, "summary": summary, "total": len(issues)}
