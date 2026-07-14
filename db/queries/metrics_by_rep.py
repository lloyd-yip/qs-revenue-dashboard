"""Per-rep metric queries — returns all reps in a single query."""

from datetime import date, timedelta

from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Appointment, DealWhopMatch, ExpenseLineItem, Opportunity
from db.queries.rep_comp import (
    DEFAULT_BASE_SALARY_MONTHLY,
    DEFAULT_COMMISSION_PCT,
    get_rep_comp_settings_map,
)
from db.queries.common import (
    ALL_TEAM_SENTINEL,
    QUALIFIED_LEAD_QUALITY,
    base_filter,
    bookable_1st_call_expr,
    bookable_2nd_call_expr,
    has_1st_call,
    has_2nd_call,
    prorated_expense_amount,
    sales_cycle_days_expr,
    sales_rep_filter,
    showed_1st_call_expr,
    showed_2nd_call_expr,
    whop_projected_total_expr,
)
from sync.ghl_client import DEAL_WON_STAGE_ID, DISQUALIFIED_STAGE_ID


async def get_rep_closes(
    session: AsyncSession,
    rep_id: str | None,
    start: date,
    end: date,
    date_by: str,
) -> list[dict]:
    """Closed deals for a specific rep (or all reps when rep_id is None).

    Filters by close_date (when the deal was won), not appointment date —
    so the drilldown matches the CLOSED column in the rep table, which also
    counts by close date via the close_date_where sub-query in get_by_rep.

    Returns opportunity name, closer, close date, and deal value.
    Ordered by close date descending.
    """
    conditions = [
        Opportunity.is_excluded.is_(False),
        Opportunity.close_date.isnot(None),
        func.date(Opportunity.close_date) >= start,
        func.date(Opportunity.close_date) <= end,
        Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID,
        sales_rep_filter(),
    ]
    if rep_id is not None:
        conditions.append(Opportunity.opportunity_owner_id == rep_id)

    result = await session.execute(
        select(
            Opportunity.opportunity_name,
            Opportunity.opportunity_owner_name,
            Opportunity.close_date,
            Opportunity.projected_deal_size,
            DealWhopMatch.total_paid,
            whop_projected_total_expr().label("whop_projected"),
            DealWhopMatch.payment_count,
            DealWhopMatch.total_installments,
            DealWhopMatch.is_splitit,
            DealWhopMatch.is_claritypay,
        )
        .outerjoin(
            DealWhopMatch,
            Opportunity.ghl_opportunity_id == DealWhopMatch.ghl_opportunity_id,
        )
        .where(and_(*conditions))
        .order_by(Opportunity.close_date.desc())
    )

    return [
        {
            "name": row.opportunity_name or "—",
            "rep": row.opportunity_owner_name or "Unassigned",
            "close_date": row.close_date.strftime("%b %d, %Y") if row.close_date else "—",
            "value": float(row.projected_deal_size) if row.projected_deal_size else None,
            "cash_paid": float(row.total_paid) if row.total_paid is not None else None,
            "whop_projected": round(float(row.whop_projected), 2) if row.whop_projected is not None else None,
            "payment_count": row.payment_count,
            "total_installments": row.total_installments,
            "is_splitit": row.is_splitit,
            "is_claritypay": row.is_claritypay,
        }
        for row in result.all()
    ]


async def get_rep_opps(
    session: AsyncSession,
    rep_id: str | None,
    opp_type: str,  # "booked" | "showed" | "not_logged"
    start: date,
    end: date,
    date_by: str,
) -> list[dict]:
    """Booked, showed, or outcome-unfilled 1st-call opps for a rep (drill-down modal).

    opp_type='booked'     → all opps with a 1st call in range
    opp_type='showed'     → subset that actually showed
    opp_type='not_logged' → opps where rep never logged the call outcome
    """
    bf = base_filter(start, end, date_by, rep_id)
    is_1st = has_1st_call(start, end, date_by)

    if opp_type == "showed":
        showed_1st = showed_1st_call_expr()
        row_filter = and_(bf, is_1st, showed_1st)
    elif opp_type == "not_logged":
        row_filter = and_(bf, is_1st, Opportunity.outcome_unfilled.is_(True))
    else:
        row_filter = and_(bf, is_1st)

    result = await session.execute(
        select(
            Opportunity.opportunity_name,
            Opportunity.ghl_opportunity_id,
            Opportunity.call1_appointment_date,
            Opportunity.pipeline_stage_name,
            Opportunity.opportunity_owner_name,
            Opportunity.call1_appointment_status,
        )
        .where(row_filter)
        .order_by(Opportunity.call1_appointment_date.desc())
    )

    return [
        {
            "name": row.opportunity_name or "—",
            "ghl_opportunity_id": row.ghl_opportunity_id,
            "appt_date": row.call1_appointment_date.strftime("%b %d, %Y") if row.call1_appointment_date else "—",
            "stage": row.pipeline_stage_name or "—",
            "rep": row.opportunity_owner_name or "Unassigned",
            "status": row.call1_appointment_status or "—",
        }
        for row in result.all()
    ]


async def get_by_rep(
    session: AsyncSession,
    start: date,
    end: date,
    date_by: str,
) -> list[dict]:
    """All KPIs broken down per rep in a single aggregated query.

    Payment data (contract_value, cash_collected) comes from deal_whop_matches
    (Whop/Stripe/Wise reconciled) rather than GHL's monetary_value/cash_collected.

    Cost metrics are cohort-aligned to match the revenue side:
    - Lead cost: expense_line_items marketing spend in the window, allocated
      proportionally by calls booked.
    - Rep comp: DERIVED from rep_comp_settings (prorated base salary +
      commission % × cohort cash collected) — NOT read from Xero payouts,
      which are cash-basis and lag split-payment deals by months.
    """

    bf = base_filter(start, end, date_by)
    is_1st = has_1st_call(start, end, date_by)
    is_2nd = has_2nd_call(start, end, date_by)
    showed_1st = showed_1st_call_expr()
    showed_2nd = showed_2nd_call_expr()

    # Won-deal condition for payment aggregation via deal_matches
    is_won = Opportunity.pipeline_stage_id == DEAL_WON_STAGE_ID

    result = await session.execute(
        select(
            Opportunity.opportunity_owner_id.label("rep_id"),
            Opportunity.opportunity_owner_name.label("rep_name"),
            func.count(case((is_1st, 1))).label("calls_booked_1st"),
            func.count(
                case((and_(is_1st, showed_1st), 1))
            ).label("shows_1st"),
            func.count(
                case((and_(is_1st, bookable_1st_call_expr()), 1))
            ).label("bookable_1st"),
            func.count(case((is_2nd, 1))).label("calls_booked_2nd"),
            func.count(case((and_(is_2nd, showed_2nd), 1))).label("shows_2nd"),
            func.count(
                case((and_(is_2nd, bookable_2nd_call_expr()), 1))
            ).label("bookable_2nd"),
            func.count(
                case((
                    and_(is_1st, showed_1st, Opportunity.lead_quality.in_(QUALIFIED_LEAD_QUALITY)),
                    1,
                ))
            ).label("qualified_shows"),
            func.count(
                case((and_(is_1st, showed_1st, Opportunity.lead_quality.isnot(None)), 1))
            ).label("shows_with_quality_filled"),
            func.count(
                case((
                    and_(
                        is_1st,
                        showed_1st,
                        Opportunity.lead_quality == "DQ",
                    ),
                    1,
                ))
            ).label("dq_count"),
            func.count(case((and_(is_1st, showed_1st, Opportunity.lead_quality == "Great"), 1))).label("lq_great"),
            func.count(case((and_(is_1st, showed_1st, Opportunity.lead_quality == "Ok"), 1))).label("lq_ok"),
            func.count(case((and_(is_1st, showed_1st, Opportunity.lead_quality == "Barely Passable"), 1))).label("lq_barely"),
            func.count(case((and_(is_1st, showed_1st, Opportunity.lead_quality == "Bad"), 1))).label("lq_bad"),
            func.count(case((and_(is_1st, showed_1st, Opportunity.lead_quality.is_(None)), 1))).label("lq_missing"),
            func.count(
                case((and_(is_1st, is_won), 1))
            ).label("units_closed"),
            func.coalesce(
                func.sum(
                    case((and_(is_1st, is_won), Opportunity.projected_deal_size))
                ),
                0,
            ).label("projected_contract_value"),
            func.count(
                case((
                    or_(
                        and_(is_1st, showed_1st),
                        and_(is_2nd, showed_2nd),
                    ),
                    1,
                ))
            ).label("total_shows"),
            func.count(
                case((Opportunity.rep_compliance_failure.is_(True), 1))
            ).label("compliance_failures"),
            # DQ'd after 2nd call booked (passed call 1 screen but still didn't qualify)
            func.count(
                case((
                    and_(
                        is_1st,
                        showed_1st,
                        Opportunity.lead_quality == "DQ",
                        Opportunity.call2_appointment_date.isnot(None),
                    ),
                    1,
                ))
            ).label("dq_after_call2_count"),
            # Call outcome not logged (show/no-show never marked by rep)
            func.count(
                case((and_(is_1st, Opportunity.outcome_unfilled.is_(True)), 1))
            ).label("outcome_not_logged_count"),
            # Avg deal cycle: first SHOWED call → first payment, cohort won deals only.
            func.avg(
                case((
                    and_(
                        is_1st,
                        is_won,
                        Opportunity.call1_appointment_date.isnot(None),
                    ),
                    sales_cycle_days_expr(DealWhopMatch.first_payment_date),
                ))
            ).label("avg_cycle_days"),
            # Payment data from deal_whop_matches (Whop/Stripe/Wise reconciled)
            func.coalesce(
                func.sum(case((and_(is_1st, is_won), DealWhopMatch.total_contract_value))),
                0,
            ).label("contract_value"),
            # Cash collected = total paid to date, so monthly installments stack
            # up as they land (upfront_cash kept separately for the % upfront metric)
            func.coalesce(
                func.sum(case((and_(is_1st, is_won), DealWhopMatch.total_paid))),
                0,
            ).label("cash_collected_sum"),
            func.coalesce(
                func.sum(case((and_(is_1st, is_won), DealWhopMatch.upfront_cash))),
                0,
            ).label("upfront_cash_sum"),
            # Payment-verified projected full contract: financed (Splitit/ClarityPay)
            # settles 100% upfront so total_paid IS the contract; internal plans
            # project avg installment × plan length (split_pay_required_payments).
            func.coalesce(
                func.sum(case((and_(is_1st, is_won), whop_projected_total_expr()))),
                0,
            ).label("whop_projected_total"),
        )
        .outerjoin(
            DealWhopMatch,
            Opportunity.ghl_opportunity_id == DealWhopMatch.ghl_opportunity_id,
        )
        .where(bf)
        .group_by(Opportunity.opportunity_owner_id, Opportunity.opportunity_owner_name)
        .order_by(Opportunity.opportunity_owner_name)
    )

    rows = result.all()

    # ── Reschedules per rep ───────────────────────────────────────────────
    # Restricted to the booked 1st-call cohort (base_filter + has_1st_call), so it matches
    # calls_booked_1st. Rescheduled = cohort opps with >1 call_1 appointment (moved >=1x).
    _per_opp_resched = (
        select(
            Opportunity.opportunity_owner_id.label("owner"),
            Opportunity.ghl_opportunity_id.label("oid"),
            func.count().filter(Appointment.appointment_type == "call_1").label("total"),
        )
        .select_from(Opportunity)
        .join(Appointment, Appointment.ghl_contact_id == Opportunity.ghl_contact_id)
        .where(and_(base_filter(start, end, date_by), has_1st_call(start, end, date_by)))
        .group_by(Opportunity.opportunity_owner_id, Opportunity.ghl_opportunity_id)
        .subquery()
    )
    _resched_rows = await session.execute(
        select(
            _per_opp_resched.c.owner,
            func.count().filter(_per_opp_resched.c.total > 1).label("rescheduled"),
        ).group_by(_per_opp_resched.c.owner)
    )
    reschedule_by_rep: dict[str, int] = {r.owner: r.rescheduled for r in _resched_rows.all()}

    # ── Expense-based cost data ───────────────────────────────────────────
    # Total lead gen spend for the period, allocated proportionally by calls
    # booked per rep. Rep comp is derived from rep_comp_settings below.
    expense_overlap = and_(
        ExpenseLineItem.period_start <= end,
        ExpenseLineItem.period_end >= start,
    )
    _prorated = prorated_expense_amount(start, end)  # prorate partial-overlap periods by days

    # Per-rep comp model (base salary + commission %). Missing reps fall back
    # to base $0 / commission 10%.
    comp_settings = await get_rep_comp_settings_map(session)
    window_months = ((end - start).days + 1) / 30.4375  # avg days per month

    # Total lead gen spend (marketing_salaries + tech_tools + paid_ads)
    lead_spend_result = await session.execute(
        select(func.sum(_prorated))
        .where(
            expense_overlap,
            ExpenseLineItem.bucket.in_(["marketing_salaries", "tech_tools", "paid_ads"]),
        )
    )
    total_lead_spend = lead_spend_result.scalar()
    total_lead_spend = float(total_lead_spend) if total_lead_spend is not None else None

    # Total calls booked across all reps (for proportional lead cost allocation)
    total_calls_all_reps = sum(r.calls_booked_1st for r in rows)

    def safe_rate(num: int, den: int) -> float | None:
        return round(num / den, 4) if den else None  # type: ignore[call-overload]

    def safe_div(num: float | None, den: float | int | None) -> float | None:
        if num is None or den is None or den == 0:
            return None
        return round(num / den, 2)

    rep_list = []
    for row in rows:
        # Financial metrics are COHORT-based (1st call in window + now Won), matching the
        # Units Closed card and the units_closed drill-down — not close-date. This keeps the
        # CLOSED column, its ↗ drill-down, and the top card in agreement.
        units = row.units_closed
        contract_val = float(row.contract_value)
        cash_val = float(row.cash_collected_sum)
        upfront_val = float(row.upfront_cash_sum)
        whop_projected = float(row.whop_projected_total)
        avg_cycle = round(float(row.avg_cycle_days), 1) if row.avg_cycle_days is not None else None

        # Allocate lead spend proportional to calls booked
        if total_lead_spend is not None and total_calls_all_reps > 0 and row.calls_booked_1st > 0:
            lead_cost_alloc = round(total_lead_spend * row.calls_booked_1st / total_calls_all_reps, 2)
        else:
            lead_cost_alloc = None

        # Rep comp = prorated base salary + commission on cohort cash collected.
        # Commission accrues only when deals close and cash lands, so it stays
        # in the same cohort window as the revenue it produced.
        setting = comp_settings.get(row.rep_id) if row.rep_id else None
        base_salary_monthly = setting["base_salary_monthly"] if setting else DEFAULT_BASE_SALARY_MONTHLY
        commission_pct = setting["commission_pct"] if setting else DEFAULT_COMMISSION_PCT
        base_salary_alloc = round(base_salary_monthly * window_months, 2)
        commission_amount = round(cash_val * commission_pct / 100.0, 2) if cash_val > 0 else 0.0
        rep_comp = round(base_salary_alloc + commission_amount, 2)

        # Total invested = derived rep comp + allocated lead cost
        if rep_comp > 0 and lead_cost_alloc is not None:
            total_invested = round(rep_comp + lead_cost_alloc, 2)
        elif rep_comp > 0:
            total_invested = rep_comp
        elif lead_cost_alloc is not None:
            total_invested = lead_cost_alloc
        else:
            total_invested = None

        resc = reschedule_by_rep.get(row.rep_id, 0)
        rep_list.append({
            "rep_id": row.rep_id,
            "rep_name": row.rep_name or "Unassigned",
            "rescheduled_1st": resc,
            "reschedule_rate_1st": safe_rate(resc, row.calls_booked_1st),
            "calls_booked_1st": row.calls_booked_1st,
            "shows_1st": row.shows_1st,
            # occurred = show-rate denominator (calls with a determinate outcome) —
            # exposed so table totals can recompute Show Rate consistently
            "occurred_1st": row.bookable_1st,
            "show_rate_1st": safe_rate(row.shows_1st, row.bookable_1st),
            "no_show_rate_1st": safe_rate(row.bookable_1st - row.shows_1st, row.bookable_1st),
            "calls_booked_2nd": row.calls_booked_2nd,
            "shows_2nd": row.shows_2nd,
            "show_rate_2nd": safe_rate(row.shows_2nd, row.bookable_2nd),
            "qualification_rate": safe_rate(row.qualified_shows, row.shows_1st),
            "dq_rate": safe_rate(row.dq_count, row.shows_1st),
            "dq_after_call2_rate": safe_rate(row.dq_after_call2_count, row.shows_1st),
            "close_rate": safe_rate(units, row.shows_1st),
            "close_rate_qual": safe_rate(units, row.qualified_shows),
            "units_closed": units,
            "projected_contract_value": float(row.projected_contract_value),
            "contract_value": contract_val,
            "cash_collected": cash_val,
            "upfront_cash": upfront_val,
            "whop_projected_total": round(whop_projected, 2),
            "total_shows": row.total_shows,
            "compliance_failures": row.compliance_failures,
            "outcome_not_logged_count": row.outcome_not_logged_count,
            "avg_cycle_days": avg_cycle,
            "lq_great": row.lq_great,
            "lq_ok": row.lq_ok,
            "lq_barely": row.lq_barely,
            "lq_bad": row.lq_bad,
            "lq_missing": row.lq_missing,
            # New: averages per close — based on the payment-verified projected
            # total, not the rep-entered GHL contract value
            "avg_contract_value": safe_div(whop_projected, units) if whop_projected > 0 else None,
            "avg_cash_collected": safe_div(cash_val, units) if cash_val > 0 else None,
            "avg_cash_pct_upfront": round(upfront_val / whop_projected * 100, 1) if whop_projected > 0 and upfront_val > 0 else None,
            # New: cost & RORI (rep comp derived from rep_comp_settings)
            "rep_comp": rep_comp,
            "base_salary_monthly": base_salary_monthly,
            "base_salary_alloc": base_salary_alloc,
            "commission_pct": commission_pct,
            "commission_amount": commission_amount,
            "lead_cost_alloc": lead_cost_alloc,
            "total_invested": total_invested,
            "cost_per_close": safe_div(total_invested, units),
            "cash_rori": safe_div(cash_val, total_invested) if cash_val > 0 else None,
            # Contract RORI on the payment-verified projected total, not the
            # rep-entered GHL value — GHL overstates 40-80% on recent deals
            "contract_rori": safe_div(whop_projected, total_invested) if whop_projected > 0 else None,
        })

    return rep_list


async def get_daily_activity(
    session: AsyncSession,
    rep_id: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict]:
    """Day-by-day booked / showed / qual counts for a 7-day window.

    Defaults to the rolling last 7 days when start_date/end_date are omitted.
    Accepts an optional rep_id to scope to a single rep.
    """
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=6)  # 7 days inclusive

    showed_1st = showed_1st_call_expr()

    conditions = [
        Opportunity.is_excluded.is_(False),
        Opportunity.call1_appointment_date.isnot(None),
        func.date(Opportunity.call1_appointment_date) >= start_date,
        func.date(Opportunity.call1_appointment_date) <= end_date,
    ]
    if rep_id == ALL_TEAM_SENTINEL:
        pass  # no rep filter — show everything
    elif rep_id:
        conditions.append(Opportunity.opportunity_owner_id == rep_id)
    else:
        conditions.append(sales_rep_filter())

    result = await session.execute(
        select(
            func.date(Opportunity.call1_appointment_date).label("day"),
            func.count(Opportunity.id).label("booked"),
            func.count(case((showed_1st, 1))).label("showed"),
            func.count(case((bookable_1st_call_expr(), 1))).label("occurred"),
            func.count(
                case((
                    and_(
                        showed_1st,
                        Opportunity.lead_quality.in_(QUALIFIED_LEAD_QUALITY),
                    ),
                    1,
                ))
            ).label("qual"),
        )
        .where(and_(*conditions))
        .group_by(func.date(Opportunity.call1_appointment_date))
        .order_by(func.date(Opportunity.call1_appointment_date))
    )

    def safe_rate(num: int, den: int) -> float | None:
        return round(float(num) / den, 4) if den else None  # type: ignore[call-overload]

    return [
        {
            "day": row.day.isoformat() if hasattr(row.day, "isoformat") else str(row.day),
            "booked": row.booked,
            "occurred": row.occurred,
            "showed": row.showed,
            # Show rate = shows / occurred (excludes still-upcoming Confirmed calls), consistent
            # with the KPI cards and time-series — not shows / booked.
            "show_rate": safe_rate(row.showed, row.occurred),
            "qual": row.qual,
            "qual_rate": safe_rate(row.qual, row.showed),
        }
        for row in result.all()
    ]
