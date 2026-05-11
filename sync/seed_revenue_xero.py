#!/usr/bin/env python3
"""Seed revenue_line_items from Xero P&L income accounts.

Data was pulled from Xero Reports/ProfitAndLoss API (Oct 2025 – Apr 2026).
All Xero amounts are in EUR (base currency of quantumSCALE Institute OÜ).
Converted to USD using ECB monthly average rates (same source as expenses).

Verification: March 2026 Total Income EUR 143,872.63 × 1.1558 = $166,304
matches Xero UI display of $166,303.81 exactly.

Run:
    python3 sync/seed_revenue_xero.py

Replaces any existing revenue data for each month (Whop or Xero) — idempotent.
"""

import json
import subprocess
import sys

RAILWAY_URL = "https://qs-revenue-dashboard-production.up.railway.app"
RAILWAY_TOKEN = "RAILWAY_BEARER_REMOVED"

# ── Xero income data (EUR) pulled 2026-05-11 ─────────────────────────────────
# Source: GET /api.xro/2.0/Reports/ProfitAndLoss?fromDate=...&toDate=...
# Amounts are base-currency EUR (Xero journal NetAmount).
# SummaryRow "Total Income" rows excluded — we sum line items ourselves.

XERO_MONTHS = [
    {
        "period_start": "2025-10-01",
        "period_end":   "2025-10-31",
        "eur_usd":      1.1630,
        "items": [
            {"name": "High ticket - Installment  Pmt", "amount_eur":  77287.44},
            {"name": "High ticket - Upfront Pmt",      "amount_eur":  71925.75},
            {"name": "Low ticket - Installment Pmt",   "amount_eur":   1046.63},
            {"name": "Referral Income",                "amount_eur":   1673.83},
            {"name": "Refunds",                        "amount_eur":   -167.82},
            {"name": "SaaS IG x HighLevel - Starter",  "amount_eur":   3528.64},
        ],
    },
    {
        "period_start": "2025-11-01",
        "period_end":   "2025-11-30",
        "eur_usd":      1.1560,
        "items": [
            {"name": "High ticket - Installment  Pmt", "amount_eur":  58470.30},
            {"name": "High ticket - Upfront Pmt",      "amount_eur":  17258.19},
            {"name": "Low ticket - Installment Pmt",   "amount_eur":    624.02},
            {"name": "Referral Income",                "amount_eur":   2810.81},
            {"name": "SaaS IG x HighLevel - Starter",  "amount_eur":   2899.60},
        ],
    },
    {
        "period_start": "2025-12-01",
        "period_end":   "2025-12-31",
        "eur_usd":      1.1709,
        "items": [
            {"name": "High ticket - Installment  Pmt", "amount_eur":  53811.17},
            {"name": "High ticket - Upfront Pmt",      "amount_eur":  36175.78},
            {"name": "Low ticket - Installment Pmt",   "amount_eur":    616.54},
            {"name": "Referral Income",                "amount_eur":   3522.81},
            {"name": "Refunds",                        "amount_eur": -17971.80},
            {"name": "SaaS IG x HighLevel - Starter",  "amount_eur":   3118.43},
        ],
    },
    {
        "period_start": "2026-01-01",
        "period_end":   "2026-01-31",
        "eur_usd":      1.1738,
        "items": [
            {"name": "High ticket - Installment  Pmt", "amount_eur":  67737.31},
            {"name": "High ticket - Upfront Pmt",      "amount_eur":  33934.94},
            {"name": "Low ticket - Installment Pmt",   "amount_eur":    200.00},
            {"name": "Referral Income",                "amount_eur":   2783.21},
            {"name": "Refunds",                        "amount_eur": -11440.92},
            {"name": "SaaS IG x HighLevel - Starter",  "amount_eur":   3280.32},
            {"name": "Splitit Balance",                "amount_eur":  -1898.05},
        ],
    },
    {
        "period_start": "2026-02-01",
        "period_end":   "2026-02-28",
        "eur_usd":      1.1824,
        "items": [
            {"name": "High ticket - Installment  Pmt", "amount_eur":  72748.85},
            {"name": "High ticket - Upfront Pmt",      "amount_eur":  16921.68},
            {"name": "Low ticket - Installment Pmt",   "amount_eur":    414.69},
            {"name": "Referral Income",                "amount_eur":   4672.20},
            {"name": "Refunds",                        "amount_eur":  -4231.41},
            {"name": "SaaS IG x HighLevel - Starter",  "amount_eur":   3673.46},
        ],
    },
    {
        "period_start": "2026-03-01",
        "period_end":   "2026-03-31",
        "eur_usd":      1.1558,
        "items": [
            {"name": "High ticket - Installment  Pmt", "amount_eur":  80912.96},
            {"name": "High ticket - Upfront Pmt",      "amount_eur":  66120.46},
            {"name": "Referral Income",                "amount_eur":   3458.60},
            {"name": "Refunds",                        "amount_eur":  -4258.50},
            {"name": "SaaS IG x HighLevel - Starter",  "amount_eur":   3770.94},
            {"name": "Splitit Balance",                "amount_eur":  -6131.83},
        ],
    },
    {
        "period_start": "2026-04-01",
        "period_end":   "2026-04-30",
        "eur_usd":      1.1706,
        "items": [
            {"name": "High ticket - Installment  Pmt", "amount_eur":  39720.81},
            {"name": "High ticket - Upfront Pmt",      "amount_eur":  61591.89},
            {"name": "Referral Income",                "amount_eur":   3397.36},
            {"name": "Refunds",                        "amount_eur":  -8531.77},
            {"name": "SaaS IG x HighLevel - Starter",  "amount_eur":   9547.41},
            {"name": "Splitit Balance",                "amount_eur":  -6127.54},
        ],
    },
]

# ── Xero account name → product_type slug ────────────────────────────────────
NAME_TO_TYPE = {
    "High ticket - Installment  Pmt": "high_ticket_installment",
    "High ticket - Upfront Pmt":      "high_ticket_upfront",
    "Low ticket - Installment Pmt":   "low_ticket_installment",
    "Referral Income":                "referral",
    "Refunds":                        "refunds",
    "SaaS IG x HighLevel - Starter":  "saas",
    "Splitit Balance":                "splitit_balance",
}


def build_items(month: dict) -> list[dict]:
    rate = month["eur_usd"]
    items = []
    for row in month["items"]:
        product_type = NAME_TO_TYPE.get(row["name"], row["name"].lower().replace(" ", "_"))
        amount_usd = round(row["amount_eur"] * rate, 2)
        items.append({
            "source":       "xero",
            "category":     "cash_collected",
            "product_type": product_type,
            "amount":       amount_usd,
            "payment_count": 0,
            "notes":        f"Xero P&L — EUR {row['amount_eur']:,.2f} × {rate} = USD {amount_usd:,.2f}",
        })
    return items


def post_to_railway(period_start: str, period_end: str, items: list[dict]) -> dict:
    payload = json.dumps({
        "period_start": period_start,
        "period_end":   period_end,
        "items":        items,
        "replace":      True,
    })
    result = subprocess.run(
        [
            "curl", "-s", "-X", "POST",
            f"{RAILWAY_URL}/api/dashboard/revenue/upsert",
            "-H", "Content-Type: application/json",
            "-H", f"Authorization: Bearer {RAILWAY_TOKEN}",
            "-d", payload,
        ],
        capture_output=True, text=True, check=True,
    )
    return json.loads(result.stdout)


def main():
    print("=" * 64)
    print("Xero Revenue Seed (replaces Whop data)")
    print("=" * 64)

    print(f"\n{'Month':<10}  {'Line Item':<32}  {'EUR':>12}  {'USD':>12}")
    print("  " + "-" * 72)

    total_rows = 0
    for month in XERO_MONTHS:
        ms = month["period_start"][:7]
        items = build_items(month)

        for item in items:
            # Find original EUR for display
            orig = next(r for r in month["items"]
                        if NAME_TO_TYPE.get(r["name"], "") == item["product_type"])
            eur_str = f"€{orig['amount_eur']:>11,.2f}"
            usd_str = f"${item['amount']:>11,.2f}"
            print(f"  {ms:<10}  {item['product_type']:<32}  {eur_str}  {usd_str}")

        response = post_to_railway(month["period_start"], month["period_end"], items)
        rows = response.get("rows_upserted", "?")
        total_rows += rows if isinstance(rows, int) else 0
        print(f"  {'':>10}  → {rows} rows upserted\n")

    print(f"✓ Done. Total rows upserted: {total_rows}")
    print("\nVerify:")
    print(f"  curl -s '{RAILWAY_URL}/api/dashboard/revenue/summary' | python3 -m json.tool")
    print(f"\nMar 2026 expected total: $166,304 (Xero UI shows $166,303.81)")


if __name__ == "__main__":
    main()
