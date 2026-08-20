"""Shared range/meta plumbing for the browser-facing dashboard routers.

Lives here rather than in dashboard.py so sibling routers (ai_channels, …) can reuse the
exact same date contract instead of re-declaring it and drifting.
"""

from datetime import date, datetime, timezone

from fastapi import Query

from api.schemas.responses import MetaMixin

DATE_DIMENSIONS = ("appointment", "booked", "created")


def meta(start: date, end: date, date_by: str) -> MetaMixin:
    """Standard response envelope describing the range the payload was built for."""
    return MetaMixin(
        date_start=start,
        date_end=end,
        date_by=date_by,
        generated_at=datetime.now(timezone.utc),
    )


def date_params(
    start: date = Query(..., description="Start date (YYYY-MM-DD)"),
    end: date = Query(..., description="End date (YYYY-MM-DD)"),
    date_by: str = Query("appointment", description="Date dimension: 'appointment' or 'created'"),
) -> tuple[date, date, str]:
    """Range dependency shared by every dashboard endpoint. Unknown dimensions fall back
    to 'appointment' rather than 422-ing, so a stale bookmark still renders."""
    if date_by not in DATE_DIMENSIONS:
        date_by = "appointment"
    return start, end, date_by
