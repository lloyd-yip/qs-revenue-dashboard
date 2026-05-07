#!/usr/bin/env python3
"""One-time seed script — loads Oct 2025–May 2026 aggregate expense data into Supabase.

Run from the qs-dashboard directory with DATABASE_URL set:
    DATABASE_URL=<url> python sync/seed_expenses.py

Data source: qs-analytics/financial-analysis/qs-unit-economics.md
Period: 2025-10-01 to 2026-05-07 (7.2 months)
"""

import asyncio
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from db.session import AsyncSessionLocal
from db.queries.expenses import upsert_expense_line_items

PERIOD_START = date(2025, 10, 1)
PERIOD_END   = date(2026, 5, 7)

# All amounts are period totals (7.2 months), sourced from Xero P&L
# is_approximate=True means the amount is from memory/estimate, not a confirmed transaction line
ITEMS = [

    # ── Sales ──────────────────────────────────────────────────────────────────
    {
        "bucket": "sales",
        "vendor": "Salaries – Sales (Armando, Melissa, Ryan)",
        "amount": 101864.30,
        "notes": "Armando, Melissa Fredericks, Ryan Matsumori",
    },
    {
        "bucket": "sales",
        "vendor": "Commissions",
        "amount": 2162.18,
    },
    {
        "bucket": "sales",
        "vendor": "Tool – CRM (GoHighLevel)",
        "amount": 5174.67,
    },
    {
        "bucket": "sales",
        "vendor": "Stripe Collection Fees",
        "amount": 16113.45,
        "notes": "Payment processing fees on collected revenue",
    },
    {
        "bucket": "sales",
        "vendor": "Stripe Billing Usage Fee",
        "amount": 5130.58,
    },
    {
        "bucket": "sales",
        "vendor": "Whop Fees",
        "amount": 4290.29,
        "notes": "Platform fees on low-ticket / community revenue",
    },

    # ── Marketing Salaries ─────────────────────────────────────────────────────
    {
        "bucket": "marketing_salaries",
        "vendor": "Salaries – Marketing (Angel, Lloyd, Geri, Santiago, Gonzalo, Tati)",
        "amount": 158761.40,
        "notes": "Angel, Lloyd, Geri, Santiago, Gonzalo (pre-mid-Apr), Tati (left Mar 2026)",
    },

    # ── Tech & Tools ───────────────────────────────────────────────────────────
    {
        "bucket": "tech_tools",
        "vendor": "Tools – Funnel",
        "amount": 57374.01,
        "notes": "Primary funnel tools (webinar, landing pages, booking)",
    },
    {
        "bucket": "tech_tools",
        "vendor": "Tool – Marketing",
        "amount": 23434.79,
        "notes": "Includes Nick + Juan retainer — extract transaction-level for clean paid ads total",
    },
    {
        "bucket": "tech_tools",
        "vendor": "Tools – Automation",
        "amount": 3951.83,
    },
    {
        "bucket": "tech_tools",
        "vendor": "Tool – AI",
        "amount": 254.32,
    },
    {
        "bucket": "tech_tools",
        "vendor": "Tool – Project Management (revenue team 50%)",
        "amount": 3839.32,
        "notes": "50% of project management cost allocated to revenue team",
    },

    # ── Paid Ads ───────────────────────────────────────────────────────────────
    {
        "bucket": "paid_ads",
        "vendor": "Paid Ads – Direct Spend",
        "amount": 17533.03,
        "notes": "Confirmed from Xero. Nick + Juan retainers still embedded in Tool-Marketing.",
    },
    {
        "bucket": "paid_ads",
        "vendor": "Nick Otero – Paid Ads Manager (est.)",
        "amount": 3500.00,
        "is_approximate": True,
        "notes": "Retainer embedded in Tool-Marketing. Exact amount TBD — pull Xero transactions.",
    },
    {
        "bucket": "paid_ads",
        "vendor": "Juan – Paid Ads Manager (est.)",
        "amount": 2000.00,
        "is_approximate": True,
        "notes": "Retainer embedded in Tool-Marketing. Exact amount TBD — pull Xero transactions.",
    },

    # ── Experiments ────────────────────────────────────────────────────────────
    {
        "bucket": "experiments",
        "vendor": "Offer Workshop Exp",
        "amount": 10043.78,
        "notes": "Unclassified — possible one-time event/workshop. Classify with Lloyd.",
    },
]


async def main():
    print(f"Seeding {len(ITEMS)} expense line items for {PERIOD_START} → {PERIOD_END}…")
    async with AsyncSessionLocal() as session:
        count = await upsert_expense_line_items(session, PERIOD_START, PERIOD_END, ITEMS)
    print(f"Done — {count} rows upserted.")
    total = sum(i["amount"] for i in ITEMS)
    print(f"Grand total in seed: ${total:,.2f}")


if __name__ == "__main__":
    asyncio.run(main())
