"""Read queries for xero_pnl_lines — the faithful company-wide Xero P&L.

Powers the Company dashboard's Revenue / Expenses / Net Profit + cost breakdown, so
they match Xero exactly. Expense accounts are rolled into a handful of readable groups
for the stacked cost chart; income + totals come straight off the stored lines.
"""

from datetime import date

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import XeroPnlLine

# Display grouping for the company cost chart — maps a Xero expense account name to a
# readable bucket. Substring match on the lowercased account; first hit wins.
EXPENSE_GROUP_ORDER = [
    "Sales", "Client Delivery", "Marketing & Mgmt Salaries", "Advertising",
    "Tech & Tools", "Payment & Bank Fees", "Currency / FX", "General & Admin", "Other",
]

_GROUP_RULES = [
    ("salaries - sales", "Sales"),
    ("salaries - operations", "Client Delivery"),
    ("va pre training", "Client Delivery"),
    ("salaries - management", "Marketing & Mgmt Salaries"),
    ("salaries - marketing", "Marketing & Mgmt Salaries"),
    ("salaries - assistant", "Marketing & Mgmt Salaries"),
    ("paid ads", "Advertising"),
    ("salaries - hermes", "Tech & Tools"),
    ("tool", "Tech & Tools"),      # "Tool - CRM", "Tools - Automation", …
    ("whop", "Payment & Bank Fees"),
    ("splitit", "Payment & Bank Fees"),
    ("stripe", "Payment & Bank Fees"),
    ("bank fee", "Payment & Bank Fees"),
    ("payoneer", "Payment & Bank Fees"),
    ("currency", "Currency / FX"),
    ("revaluation", "Currency / FX"),
    ("general expense", "General & Admin"),
    ("consulting", "General & Admin"),
    ("accounting", "General & Admin"),
]


def expense_group(account: str) -> str:
    n = (account or "").lower()
    for needle, group in _GROUP_RULES:
        if needle in n:
            return group
    return "Other"


async def get_xero_pnl_for_period(session: AsyncSession, period_start: date) -> dict:
    """Company-wide P&L for ONE month (matched on period_start), shaped for the P&L
    page's bucket renderer: income accounts + expense groups (each with account items)
    + totals + net. Matches Xero. Returns has_data=False when the month isn't synced.
    """
    rows = (await session.execute(
        select(XeroPnlLine.section, XeroPnlLine.account, XeroPnlLine.is_income, XeroPnlLine.amount_usd)
        .where(XeroPnlLine.period_start == period_start)
        .order_by(XeroPnlLine.amount_usd.desc())
    )).all()

    income_items: list[dict] = []
    groups: dict[str, list] = {}
    total_income = total_expense = 0.0
    for section, account, is_income, amount_usd in rows:
        amt = float(amount_usd or 0)
        if is_income:
            income_items.append({"name": account, "amount": amt})
            total_income += amt
        else:
            groups.setdefault(expense_group(account), []).append({"name": account, "amount": amt})
            total_expense += amt

    expense_groups = []
    for g in EXPENSE_GROUP_ORDER:
        items = groups.get(g)
        if not items:
            continue
        items.sort(key=lambda x: x["amount"], reverse=True)
        expense_groups.append({"label": g, "total": round(sum(i["amount"] for i in items), 2), "items": items})

    income_items.sort(key=lambda x: x["amount"], reverse=True)
    return {
        "has_data": bool(rows),
        "period_start": str(period_start),
        "total_income": round(total_income, 2),
        "total_expenses": round(total_expense, 2),
        "net_profit": round(total_income - total_expense, 2),
        "income_items": income_items,
        "expense_groups": expense_groups,
    }


async def get_xero_pnl_by_month(session: AsyncSession, start: date, end: date) -> dict:
    """Company-wide P&L per month from xero_pnl_lines.

    Returns:
      {
        has_data: bool,
        income:   {mk: total_income_usd},
        expenses: {mk: total_expense_usd},
        net:      {mk: income - expense},
        expense_groups: {group_label: {mk: usd}},   # for the stacked cost chart
        groups_present: [ordered group labels that have any spend],
      }
    Months with no stored lines are simply absent (caller leaves them blank).
    """
    rows = (await session.execute(
        select(XeroPnlLine.period_start, XeroPnlLine.account,
               XeroPnlLine.is_income, XeroPnlLine.amount_usd)
        .where(XeroPnlLine.period_start >= start)
        .where(XeroPnlLine.period_start <= end)
    )).all()

    income: dict[str, float] = {}
    expenses: dict[str, float] = {}
    expense_groups: dict[str, dict[str, float]] = {}
    for period_start, account, is_income, amount_usd in rows:
        mk = f"{period_start.year:04d}-{period_start.month:02d}"
        amt = float(amount_usd or 0)
        if is_income:
            income[mk] = round(income.get(mk, 0.0) + amt, 2)
        else:
            expenses[mk] = round(expenses.get(mk, 0.0) + amt, 2)
            g = expense_group(account)
            expense_groups.setdefault(g, {})[mk] = round(expense_groups.setdefault(g, {}).get(mk, 0.0) + amt, 2)

    net = {mk: round(income.get(mk, 0.0) - expenses.get(mk, 0.0), 2)
           for mk in set(income) | set(expenses)}
    groups_present = [g for g in EXPENSE_GROUP_ORDER if g in expense_groups]

    return {
        "has_data": bool(rows),
        "income": income,
        "expenses": expenses,
        "net": net,
        "expense_groups": expense_groups,
        "groups_present": groups_present,
    }
