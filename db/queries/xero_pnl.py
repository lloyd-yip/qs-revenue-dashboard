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
