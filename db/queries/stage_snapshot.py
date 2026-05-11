"""Current-state pipeline snapshot for Hot List and Warm List stages.

Unlike all other dashboard queries, this is NOT date-filtered — it reflects the
live state of GHL right now. Used in the weekly report for projected pipeline value.

Hot List  (Verbal Commit)   → 50% close probability discount applied by report
Warm List (1st/2nd Call done) → 10% close probability discount applied by report
"""

from sqlalchemy import and_, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity
from config import ACTIVE_REP_NAMES, _normalize_name

# Normalized active rep names for post-query filtering (handles double-space variants)
_ACTIVE_NORMALIZED = frozenset(_normalize_name(n) for n in ACTIVE_REP_NAMES)

# ── Stage ID constants ────────────────────────────────────────────────────────
# Only the two canonical stages we care about — no temp/legacy stages.
HOT_STAGE_IDS = frozenset({
    "8b0e8559-7665-4033-b762-23d94bfce90b",  # Hot List (Verbal Commit)
})

WARM_STAGE_IDS = frozenset({
    "d51c088d-1629-43f2-8ee8-c51bf74b8553",  # Warm List (1st or 2nd Call done)
})

ALL_TRACKED_STAGE_IDS = HOT_STAGE_IDS | WARM_STAGE_IDS

# Human-readable label for each stage bucket
STAGE_BUCKET: dict[str, str] = {
    "8b0e8559-7665-4033-b762-23d94bfce90b": "hot",
    "d51c088d-1629-43f2-8ee8-c51bf74b8553": "warm",
}


async def get_stage_snapshot(session: AsyncSession) -> dict:
    """Return current Hot/Warm list breakdown by rep.

    Response shape:
    {
        "by_rep": [
            {
                "rep_name": "Ryan Matsumori",
                "rep_id": "...",
                "hot_count": 3,
                "hot_value": 60000.0,
                "hot_missing_value": 1,
                "warm_count": 5,
                "warm_value": 80000.0,
                "warm_missing_value": 2,
            },
            ...
        ],
        "team": {
            "hot_count": ..., "hot_value": ..., "hot_missing_value": ...,
            "warm_count": ..., "warm_value": ..., "warm_missing_value": ...,
        }
    }
    """
    result = await session.execute(
        select(
            Opportunity.opportunity_owner_name.label("rep_name"),
            Opportunity.opportunity_owner_id.label("rep_id"),
            Opportunity.pipeline_stage_id.label("stage_id"),
            func.count().label("count"),
            func.coalesce(func.sum(Opportunity.monetary_value), 0).label("total_value"),
            func.count(
                case(
                    (
                        (Opportunity.monetary_value.is_(None))
                        | (Opportunity.monetary_value == 0),
                        1,
                    ),
                    else_=None,
                )
            ).label("missing_value_count"),
        )
        .where(
            and_(
                Opportunity.is_excluded.is_(False),
                Opportunity.pipeline_stage_id.in_(list(ALL_TRACKED_STAGE_IDS)),
            )
        )
        .group_by(
            Opportunity.opportunity_owner_name,
            Opportunity.opportunity_owner_id,
            Opportunity.pipeline_stage_id,
        )
        .order_by(Opportunity.opportunity_owner_name)
    )

    rows = result.all()

    # Aggregate into per-rep buckets — active reps only
    rep_data: dict[str, dict] = {}
    for row in rows:
        name    = row.rep_name or "Unknown"
        # Skip inactive/other reps — only surface active sales reps
        if _normalize_name(name) not in _ACTIVE_NORMALIZED:
            continue
        rep_id  = row.rep_id or ""
        bucket  = STAGE_BUCKET.get(row.stage_id, "warm")
        count   = row.count or 0
        value   = float(row.total_value or 0)
        missing = row.missing_value_count or 0

        if rep_id not in rep_data:
            rep_data[rep_id] = {
                "rep_name":          name,
                "rep_id":            rep_id,
                "hot_count":         0,
                "hot_value":         0.0,
                "hot_missing_value": 0,
                "warm_count":        0,
                "warm_value":        0.0,
                "warm_missing_value": 0,
            }

        rep_data[rep_id][f"{bucket}_count"]         += count
        rep_data[rep_id][f"{bucket}_value"]         += value
        rep_data[rep_id][f"{bucket}_missing_value"] += missing

    by_rep = list(rep_data.values())

    # Team totals
    team = {
        "hot_count":          sum(r["hot_count"]          for r in by_rep),
        "hot_value":          sum(r["hot_value"]           for r in by_rep),
        "hot_missing_value":  sum(r["hot_missing_value"]   for r in by_rep),
        "warm_count":         sum(r["warm_count"]          for r in by_rep),
        "warm_value":         sum(r["warm_value"]          for r in by_rep),
        "warm_missing_value": sum(r["warm_missing_value"]  for r in by_rep),
    }

    return {"by_rep": by_rep, "team": team}
