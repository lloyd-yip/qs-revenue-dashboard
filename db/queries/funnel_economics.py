"""Funnel Economics — period marketing spend, rep compensation, and auto-cost calculations.

Manual inputs (PeriodMarketingSpend, RepCompensation) power the RORI table.
Auto-cost calculations (get_auto_funnel_economics) pull from Xero expense data
and GHL opportunity data to populate the 4 cost cards automatically.
"""

from datetime import date

from sqlalchemy import and_, case, delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.models import DealWhopMatch, ExpenseLineItem, Opportunity, PeriodMarketingSpend, RepCompensation, SourceNormalization
from db.queries.common import (
    QUALIFIED_LEAD_QUALITY,
    base_filter,
    has_1st_call,
    showed_1st_call_expr,
    ALL_TEAM_SENTINEL,
)
from sync.ghl_client import DEAL_WON_STAGE_ID

# Vendors in the 'sales' bucket that count as sales compensation for Cost/Acquisition.
# Excludes CRM tools and payment processing fees which are not rep comp.
SALES_COMP_VENDORS = ("Salaries – Sales", "Commissions")


async def get_period_inputs(
    session: AsyncSession,
    start: date,
    end: date,
) -> dict | None:
    """Return saved marketing spend + rep comps for the exact period.

    Returns None if no data has been saved for this period yet.
    Returns a dict with keys: marketing_spend (float|None), rep_comps (list[dict]).
    """
    spend_row = await session.scalar(
        select(PeriodMarketingSpend).where(
            and_(
                PeriodMarketingSpend.period_start == start,
                PeriodMarketingSpend.period_end == end,
            )
        )
    )

    comp_rows = (await session.execute(
        select(RepCompensation).where(
            and_(
                RepCompensation.period_start == start,
                RepCompensation.period_end == end,
            )
        ).order_by(RepCompensation.rep_name)
    )).scalars().all()

    if spend_row is None and not comp_rows:
        return None

    return {
        "marketing_spend": float(spend_row.amount) if spend_row else None,
        "rep_comps": [
            {
                "rep_id": r.rep_id,
                "rep_name": r.rep_name,
                "total_comp": float(r.total_comp),
            }
            for r in comp_rows
        ],
    }


async def upsert_marketing_spend(
    session: AsyncSession,
    start: date,
    end: date,
    amount: float,
) -> None:
    """Save or overwrite total marketing spend for the exact period.

    Upsert = insert if new period, update if it already exists.
    """
    stmt = pg_insert(PeriodMarketingSpend).values(
        period_start=start,
        period_end=end,
        amount=amount,
    ).on_conflict_do_update(
        index_elements=["period_start", "period_end"],
        set_={"amount": amount, "updated_at": "now()"},
    )
    await session.execute(stmt)
    await session.commit()


async def get_auto_funnel_economics(
    session: AsyncSession,
    start: date,
    end: date,
    date_by: str = "appointment",
) -> dict:
    """Auto-compute funnel economics for the primary webinar invite funnel.

    Marketing spend = sum of marketing_salaries + tech_tools expense buckets
    for periods fully contained within [start, end].

    Sales comp = sum of 'Salaries – Sales' + 'Commissions' vendors from the
    sales bucket for those same periods.

    GHL metrics (calls booked / shows / qual shows / closed / avg deal values)
    are filtered to opportunities whose canonical_channel appears in
    source_normalization with is_primary_funnel = TRUE (currently "Webinar Live").

    All four cost cards derive from these two inputs:
      Cost/Call Booked  = marketing_spend ÷ calls_booked
      Cost/Show         = marketing_spend ÷ shows
      Cost/Qual Show    = marketing_spend ÷ qual_shows
      Cost/Acquisition  = (marketing_spend + sales_comp) ÷ closed

    Returns None for a cost card if the required inputs aren't available
    (e.g. no expense data loaded for this period yet).

    VERIFICATION: Hit GET /api/dashboard/funnel-economics?start=YYYY-MM-01&end=YYYY-MM-DD
    and confirm the response has non-null cost fields when expense data exists for that month.
    If all costs return null, check that expense line items exist for the period:
        SELECT bucket, SUM(amount) FROM expense_line_items
        WHERE period_start >= 'YYYY-MM-01' GROUP BY bucket;

    SILENT FAILURE SIGNAL: Cost cards show "—" on the dashboard even though you've
    synced Xero data. Root cause is usually that the expense period dates don't fall
    within the dashboard date range — expense periods must be *fully inside* the range.
    """
    # ── 1. Expense data ───────────────────────────────────────────────────────
    # Overlap semantics: include any expense period that touches [start, end].
    # A period overlaps if it starts before end AND ends after start.
    # This handles multi-month ranges correctly and works with month-picker inputs.
    expense_filter = and_(
        ExpenseLineItem.period_start <= end,
        ExpenseLineItem.period_end >= start,
    )

    # Marketing spend: marketing_salaries + tech_tools combined
    mktg_row = await session.execute(
        select(func.sum(ExpenseLineItem.amount))
        .where(expense_filter, ExpenseLineItem.bucket.in_(["marketing_salaries", "tech_tools"]))
    )
    marketing_spend = mktg_row.scalar()
    marketing_spend = float(marketing_spend) if marketing_spend is not None else None

    # Sales comp: only Salaries–Sales and Commissions from the sales bucket
    comp_row = await session.execute(
        select(func.sum(ExpenseLineItem.amount))
        .where(
            expense_filter,
            ExpenseLineItem.bucket == "sales",
            ExpenseLineItem.vendor.in_(list(SALES_COMP_VENDORS)),
        )
    )
    sales_comp = comp_row.scalar()
    sales_comp = float(sales_comp) if sales_comp is not None else None

    # ── 2. Primary funnel channels ────────────────────────────────────────────
    # Fetch channel names flagged is_primary_funnel=True in source_normalization.
    # These are the channels whose opportunities count toward cost card denominators.
    ch_result = await session.execute(
        select(SourceNormalization.canonical_channel)
        .where(SourceNormalization.is_primary_funnel.is_(True))
        .distinct()
    )
    primary_channels = [r[0] for r in ch_result.all()]

    if not primary_channels:
        # Migration ran but no channels flagged — return zeros so the UI doesn't error
        return {
            "marketing_spend": marketing_spend,
            "sales_comp": sales_comp,
            "calls_booked": 0,
            "shows": 0,
            "qual_shows": 0,
            "closed": 0,
            "avg_contract_value": None,
            "avg_cash_collected": None,
            "avg_pct_cash_upfront": None,
            "cost_per_call_booked": None,
            "cost_per_show": None,
            "cost_per_qual_show": None,
            "cost_per_acquisition": None,
            "primary_channels": [],
        }

    # ── 3. GHL metrics, primary funnel only ───────────────────────────────────
    # base_filter with ALL_TEAM_SENTINEL = no rep restriction (cost analysis is whole-team)
    ghl_filter = and_(
        base_filter(start, end, date_by, rep_id=ALL_TEAM_SENTINEL),
        Opportunity.canonical_channel.in_(primary_channels),
    )

    showed = showed_1st_call_expr()
    has_call = has_1st_call(start, end, date_by)
    closed_won = Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID

    # Qualified show: showed on 1st call AND lead quality is Great/Ok/Barely Passable
    qual_show = and_(showed, Opportunity.lead_quality.in_(list(QUALIFIED_LEAD_QUALITY)))

    # Single aggregation query — one DB round trip for all 7 metrics
    # Payment data (avg_contract_value, avg_cash_collected) comes from
    # deal_whop_matches (Whop/Stripe/Wise reconciled) via LEFT JOIN.
    agg = await session.execute(
        select(
            func.sum(case((has_call, 1), else_=0)).label("calls_booked"),
            func.sum(case((showed, 1), else_=0)).label("shows"),
            func.sum(case((qual_show, 1), else_=0)).label("qual_shows"),
            func.sum(case((closed_won, 1), else_=0)).label("closed"),
            # avg from deal_whop_matches — only over closed-won rows with matched payment data
            func.avg(case((closed_won, DealWhopMatch.total_contract_value))).label("avg_contract_value"),
            func.avg(case((closed_won, DealWhopMatch.upfront_cash))).label("avg_cash_collected"),
            func.avg(
                case(
                    (
                        and_(
                            closed_won,
                            DealWhopMatch.total_contract_value > 0,
                            DealWhopMatch.upfront_cash.isnot(None),
                            DealWhopMatch.upfront_cash > 0,
                        ),
                        DealWhopMatch.upfront_cash / DealWhopMatch.total_contract_value * 100,
                    )
                )
            ).label("avg_pct_cash_upfront"),
        )
        .outerjoin(
            DealWhopMatch,
            Opportunity.ghl_opportunity_id == DealWhopMatch.ghl_opportunity_id,
        )
        .where(ghl_filter)
    )
    row = agg.one()

    calls_booked      = int(row.calls_booked or 0)
    shows             = int(row.shows or 0)
    qual_shows        = int(row.qual_shows or 0)
    avg_contract_value  = float(row.avg_contract_value)  if row.avg_contract_value  is not None else None
    avg_cash_collected  = float(row.avg_cash_collected)   if row.avg_cash_collected   is not None else None
    avg_pct_cash_upfront = float(row.avg_pct_cash_upfront) if row.avg_pct_cash_upfront is not None else None

    # Closes counted by CLOSE DATE — not appointment date — so deals that
    # closed in this period are counted even if their first call was earlier.
    close_date_filter = and_(
        Opportunity.is_excluded.is_(False),
        Opportunity.close_date.isnot(None),
        func.date(Opportunity.close_date) >= start,
        func.date(Opportunity.close_date) <= end,
        Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID,
        Opportunity.canonical_channel.in_(primary_channels),
    )
    close_agg = await session.execute(
        select(
            func.count().label("closed"),
            func.avg(DealWhopMatch.total_contract_value).label("avg_cv"),
            func.avg(DealWhopMatch.upfront_cash).label("avg_cc"),
            func.avg(
                case(
                    (
                        and_(
                            DealWhopMatch.total_contract_value > 0,
                            DealWhopMatch.upfront_cash.isnot(None),
                            DealWhopMatch.upfront_cash > 0,
                        ),
                        DealWhopMatch.upfront_cash / DealWhopMatch.total_contract_value * 100,
                    )
                )
            ).label("avg_pct"),
        )
        .outerjoin(
            DealWhopMatch,
            Opportunity.ghl_opportunity_id == DealWhopMatch.ghl_opportunity_id,
        )
        .where(close_date_filter)
    )
    close_row = close_agg.one()
    closed = int(close_row.closed or 0)
    # Override payment averages with close-date-filtered values if available
    if close_row.avg_cv is not None:
        avg_contract_value = float(close_row.avg_cv)
    if close_row.avg_cc is not None:
        avg_cash_collected = float(close_row.avg_cc)
    if close_row.avg_pct is not None:
        avg_pct_cash_upfront = float(close_row.avg_pct)

    # ── 4. Derive cost cards ──────────────────────────────────────────────────
    # Each card returns None if its required inputs are missing (no expense data loaded)
    cost_per_call_booked = (
        round(marketing_spend / calls_booked, 2)
        if marketing_spend is not None and calls_booked > 0 else None
    )
    cost_per_show = (
        round(marketing_spend / shows, 2)
        if marketing_spend is not None and shows > 0 else None
    )
    cost_per_qual_show = (
        round(marketing_spend / qual_shows, 2)
        if marketing_spend is not None and qual_shows > 0 else None
    )
    # Cost/Acquisition uses marketing + sales comp combined.
    # sales_comp is treated as $0 if absent — shows a floor number rather than blanking out.
    # (Sales comp apportionment per channel deferred until Whop is configured.)
    cost_per_acquisition = (
        round(((marketing_spend or 0) + (sales_comp or 0)) / closed, 2)
        if marketing_spend is not None and closed > 0 else None
    )

    return {
        "marketing_spend": marketing_spend,
        "sales_comp": sales_comp,
        "calls_booked": calls_booked,
        "shows": shows,
        "qual_shows": qual_shows,
        "closed": closed,
        "avg_contract_value": avg_contract_value,
        "avg_cash_collected": avg_cash_collected,
        "avg_pct_cash_upfront": avg_pct_cash_upfront,
        "cost_per_call_booked": cost_per_call_booked,
        "cost_per_show": cost_per_show,
        "cost_per_qual_show": cost_per_qual_show,
        "cost_per_acquisition": cost_per_acquisition,
        "primary_channels": primary_channels,
    }


async def upsert_rep_compensations(
    session: AsyncSession,
    start: date,
    end: date,
    reps: list[dict],
) -> None:
    """Save or overwrite comp for a list of reps for the exact period.

    Each dict in reps must have: rep_id, rep_name, total_comp.
    Existing reps not in the list are left unchanged.
    """
    for rep in reps:
        stmt = pg_insert(RepCompensation).values(
            rep_id=rep["rep_id"],
            rep_name=rep["rep_name"],
            period_start=start,
            period_end=end,
            total_comp=rep["total_comp"],
        ).on_conflict_do_update(
            index_elements=["rep_id", "period_start", "period_end"],
            set_={"total_comp": rep["total_comp"], "rep_name": rep["rep_name"], "updated_at": "now()"},
        )
        await session.execute(stmt)
    await session.commit()
