"""Follow-up call show rate by lead quality."""

from datetime import date

from sqlalchemy import and_, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Opportunity
from db.queries.common import base_filter, bookable_2nd_call_expr, has_2nd_call, showed_2nd_call_expr


async def get_followup_show_rate_by_quality(
    session: AsyncSession,
    start: date,
    end: date,
    date_by: str,
    rep_id: str | None = None,
) -> list[dict]:
    """Return 2nd-call show rate grouped by lead quality.

    Returns one row per lead quality value (Great, Ok, Barely Passable, Bad, DQ)
    plus a row for NULL (not yet filled).
    """
    bf = base_filter(start, end, date_by, rep_id)
    is_2nd = has_2nd_call(start, end, date_by)
    showed_2nd = showed_2nd_call_expr()

    # COALESCE NULL lead quality to "(Not Set)"
    quality_col = func.coalesce(Opportunity.lead_quality, "(Not Set)")

    result = await session.execute(
        select(
            quality_col.label("lead_quality"),
            func.count().label("booked"),
            func.count(case((bookable_2nd_call_expr(), 1))).label("bookable"),
            func.count(case((showed_2nd, 1))).label("showed"),
            func.count(
                case(
                    (
                        Opportunity.call2_appointment_status == "No Show",
                        1,
                    )
                )
            ).label("no_show"),
            func.count(
                case(
                    (
                        Opportunity.call2_appointment_status == "Cancelled",
                        1,
                    )
                )
            ).label("cancelled"),
        )
        .where(and_(bf, is_2nd))
        .group_by(quality_col)
        .order_by(
            # Fixed display order matching the qualification scale
            case(
                (quality_col == "Great", 0),
                (quality_col == "Ok", 1),
                (quality_col == "Barely Passable", 2),
                (quality_col == "Bad / DQ", 3),
                else_=4,  # (Not Set) at the end
            )
        )
    )

    rows = []
    for r in result:
        booked = r.booked or 0
        bookable = r.bookable or 0
        showed = r.showed or 0
        no_show = r.no_show or 0
        cancelled = r.cancelled or 0
        show_rate = round(showed / bookable * 100, 1) if bookable > 0 else 0.0
        rows.append(
            {
                "lead_quality": r.lead_quality,
                "booked": booked,
                "showed": showed,
                "no_show": no_show,
                "cancelled": cancelled,
                "show_rate": show_rate,
            }
        )

    return rows
