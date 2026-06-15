#!/usr/bin/env python3
"""Seed monthly expense data (Oct 2025 – May 2026) pulled from Xero P&L via API.

Run:
    python sync/seed_expenses_monthly.py

Posts directly to Railway — no DATABASE_URL needed.

--- CATEGORIZATION RULES (applied on every sync) ---

EXCLUDED vendors (not revenue-team costs):
  - James Walter / James Wolter  (delivery team, miscoded in Xero)
  - Doug Rich                    (delivery team)
  - Xero                         (accounting software, not revenue team)
  - BitWarden                    (password manager, delivery team)
  - Profitable By Design         (delivery project, not revenue experiment)
  - Tailscale                    (infra/delivery, not revenue team)

BUCKET OVERRIDES (move from Xero account default):
  - Go High Level / Highlevel Inc  → tech_tools  (CRM billed to various accounts)
  - CLIENTACQUISITIONIO            → tech_tools
  - Fiverr / Upwork                → tech_tools
  - Render.com                     → tech_tools
  - Twilio                         → tech_tools
  - LightxEditor                   → tech_tools
  - CrowdTamers                    → experiments

50% ALLOCATION (shared with delivery team):
  - ClickUp   (project mgmt used by both teams)
  - Fireflies (meeting recorder used by both teams)
"""

import json
import os
import urllib.error
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

RAILWAY_URL = "https://qs-revenue-dashboard-production.up.railway.app/api/dashboard/expenses/upsert"
BEARER = os.environ["API_BEARER_TOKEN"]

# ── Categorization rules ──────────────────────────────────────────────────────

EXCLUDE_VENDORS = {
    "JAMES WALTER", "JAMES WOLTER",
    "DOUG RICH",
    "XERO",
    "BITWARDEN",
    "PROFITABLE BY DESIGN",
    "TAILSCALE",
}

TECH_TOOLS_OVERRIDE = {
    "GO HIGH LEVEL", "HIGHLEVEL INC",
    "CLIENTACQUISITIONIO",
    "FIVERR", "FIVER",
    "UPWORK",
    "RENDER.COM",
    "TWILIO",
    "LIGHTXEDITOR",
}

EXPERIMENTS_OVERRIDE = {
    "CROWDTAMERS",
}

# Vendors whose costs are split 50/50 with delivery team
HALF_VENDORS = {
    "CLICKUP",
    "FIREFLIES",
}

# Xero org base currency is EUR (quantumSCALE Institute OÜ, Estonia).
# All Xero amounts are in EUR — convert to USD using historical monthly averages
# from Frankfurter (api.frankfurter.dev). Update when adding new months.
EUR_USD_RATES = {
    "2025-10": 1.1630,
    "2025-11": 1.1560,
    "2025-12": 1.1709,
    "2026-01": 1.1738,
    "2026-02": 1.1824,
    "2026-03": 1.1558,
    "2026-04": 1.1706,
    "2026-05": 1.1729,
}

# ── Monthly data (replace with Xero API pull on next sync) ───────────────────

MONTHS = [
    {
        "period_start": "2025-10-01",
        "period_end": "2025-10-31",
        "replace": True,
        "items": [
            {"bucket": "sales", "vendor": "Salaries – Sales", "amount": 22918.20},
            {"bucket": "sales", "vendor": "GoHighLevel (CRM)", "amount": 1081.96},
            {"bucket": "sales", "vendor": "Stripe Collection Fees", "amount": 4324.86, "notes": "Payment processing on collected revenue"},
            {"bucket": "sales", "vendor": "Stripe Billing Usage Fee", "amount": 1772.67},
            {"bucket": "marketing_salaries", "vendor": "Salaries – Marketing", "amount": 23493.82},
            {"bucket": "tech_tools", "vendor": "Tools – Funnel", "amount": 10415.98, "notes": "Webinar, landing pages, booking"},
            {"bucket": "tech_tools", "vendor": "Tool – Marketing", "amount": 194.67, "notes": "Includes paid ads mgr retainers (Nick, Juan)"},
            {"bucket": "tech_tools", "vendor": "Tools – Automation", "amount": 1237.62},
            {"bucket": "tech_tools", "vendor": "Tool – AI", "amount": 33.93},
            {"bucket": "tech_tools", "vendor": "Tool – Project Mgmt (50%)", "amount": 431.71, "notes": "50% allocated to revenue team"},
            {"bucket": "tech_tools", "vendor": "Tool – CyberSecurity", "amount": 64.63},
            {"bucket": "experiments", "vendor": "VA Pre-Training", "amount": 1475.71},
        ],
    },
    {
        "period_start": "2025-11-01",
        "period_end": "2025-11-30",
        "replace": True,
        "items": [
            {"bucket": "sales", "vendor": "Salaries – Sales", "amount": 18477.95},
            {"bucket": "sales", "vendor": "Commissions", "amount": 2162.18},
            {"bucket": "sales", "vendor": "GoHighLevel (CRM)", "amount": 1087.31},
            {"bucket": "sales", "vendor": "Stripe Collection Fees", "amount": 2427.76, "notes": "Payment processing on collected revenue"},
            {"bucket": "sales", "vendor": "Stripe Billing Usage Fee", "amount": 1095.04},
            {"bucket": "marketing_salaries", "vendor": "Salaries – Marketing", "amount": 28815.16},
            {"bucket": "tech_tools", "vendor": "Tools – Funnel", "amount": 6069.67, "notes": "Webinar, landing pages, booking"},
            {"bucket": "tech_tools", "vendor": "Tool – Marketing", "amount": 2013.77, "notes": "Includes paid ads mgr retainers (Nick, Juan)"},
            {"bucket": "tech_tools", "vendor": "Tools – Automation", "amount": 635.36},
            {"bucket": "tech_tools", "vendor": "Tool – AI", "amount": 34.00},
            {"bucket": "tech_tools", "vendor": "Tool – Project Mgmt (50%)", "amount": 781.33, "notes": "50% allocated to revenue team"},
            {"bucket": "tech_tools", "vendor": "Tool – CyberSecurity", "amount": 77.87},
            {"bucket": "experiments", "vendor": "VA Pre-Training", "amount": 274.35},
        ],
    },
    {
        "period_start": "2025-12-01",
        "period_end": "2025-12-31",
        "replace": True,
        "items": [
            {"bucket": "sales", "vendor": "Salaries – Sales", "amount": 8442.07},
            {"bucket": "sales", "vendor": "GoHighLevel (CRM)", "amount": 640.47},
            {"bucket": "sales", "vendor": "Stripe Collection Fees", "amount": 2871.86, "notes": "Payment processing on collected revenue"},
            {"bucket": "sales", "vendor": "Stripe Billing Usage Fee", "amount": 844.27},
            {"bucket": "marketing_salaries", "vendor": "Salaries – Marketing", "amount": 27603.88},
            {"bucket": "tech_tools", "vendor": "Tools – Funnel", "amount": 4716.32, "notes": "Webinar, landing pages, booking"},
            {"bucket": "tech_tools", "vendor": "Tool – Marketing", "amount": 4720.79, "notes": "Includes paid ads mgr retainers (Nick, Juan)"},
            {"bucket": "tech_tools", "vendor": "Tools – Automation", "amount": 417.70},
            {"bucket": "tech_tools", "vendor": "Tool – AI", "amount": 33.53},
            {"bucket": "tech_tools", "vendor": "Tool – Project Mgmt (50%)", "amount": 472.15, "notes": "50% allocated to revenue team"},
            {"bucket": "tech_tools", "vendor": "Tool – CyberSecurity", "amount": 65.16},
            {"bucket": "experiments", "vendor": "VA Pre-Training", "amount": 2707.14},
        ],
    },
    {
        "period_start": "2026-01-01",
        "period_end": "2026-01-31",
        "replace": True,
        "items": [
            {"bucket": "sales", "vendor": "Salaries – Sales", "amount": 12020.53},
            {"bucket": "sales", "vendor": "GoHighLevel (CRM)", "amount": 808.40},
            {"bucket": "sales", "vendor": "Stripe Collection Fees", "amount": 1960.66, "notes": "Payment processing on collected revenue"},
            {"bucket": "sales", "vendor": "Stripe Billing Usage Fee", "amount": 1089.61},
            {"bucket": "sales", "vendor": "Whop Fees", "amount": 274.47, "notes": "Platform fees on low-ticket / community revenue"},
            {"bucket": "marketing_salaries", "vendor": "Salaries – Marketing", "amount": 19437.91},
            {"bucket": "tech_tools", "vendor": "Tools – Funnel", "amount": 4634.29, "notes": "Webinar, landing pages, booking"},
            {"bucket": "tech_tools", "vendor": "Tool – Marketing", "amount": 4012.64, "notes": "Includes paid ads mgr retainers (Nick, Juan)"},
            {"bucket": "tech_tools", "vendor": "Tools – Automation", "amount": 420.05},
            {"bucket": "tech_tools", "vendor": "Tool – AI", "amount": 25.17},
            {"bucket": "tech_tools", "vendor": "Tool – Project Mgmt (50%)", "amount": 538.76, "notes": "50% allocated to revenue team"},
            {"bucket": "tech_tools", "vendor": "Tool – CyberSecurity", "amount": 64.65},
            {"bucket": "experiments", "vendor": "Offer Workshop Exp", "amount": 1067.84, "notes": "Classify with Lloyd"},
            {"bucket": "experiments", "vendor": "VA Pre-Training", "amount": 915.05},
        ],
    },
    {
        "period_start": "2026-02-01",
        "period_end": "2026-02-28",
        "replace": True,
        "items": [
            {"bucket": "sales", "vendor": "Salaries – Sales", "amount": 11974.50},
            {"bucket": "sales", "vendor": "Stripe Collection Fees", "amount": 1981.82, "notes": "Payment processing on collected revenue"},
            {"bucket": "sales", "vendor": "Stripe Billing Usage Fee", "amount": 122.89},
            {"bucket": "sales", "vendor": "Whop Fees", "amount": 547.42, "notes": "Platform fees on low-ticket / community revenue"},
            {"bucket": "marketing_salaries", "vendor": "Salaries – Marketing", "amount": 26010.66},
            {"bucket": "tech_tools", "vendor": "Tools – Funnel", "amount": 4901.65, "notes": "Webinar, landing pages, booking"},
            {"bucket": "tech_tools", "vendor": "Tool – Marketing", "amount": 2303.28, "notes": "Includes paid ads mgr retainers (Nick, Juan)"},
            {"bucket": "tech_tools", "vendor": "Tools – Automation", "amount": 479.86},
            {"bucket": "tech_tools", "vendor": "Tool – AI", "amount": 127.69},
            {"bucket": "tech_tools", "vendor": "Tool – Project Mgmt (50%)", "amount": 32.44, "notes": "50% allocated to revenue team"},
            {"bucket": "tech_tools", "vendor": "Tool – CyberSecurity", "amount": 83.25},
            {"bucket": "experiments", "vendor": "Offer Workshop Exp", "amount": 6356.61, "notes": "Classify with Lloyd"},
            {"bucket": "experiments", "vendor": "VA Pre-Training", "amount": 344.58},
        ],
    },
    {
        "period_start": "2026-03-01",
        "period_end": "2026-03-31",
        "replace": True,
        "items": [
            {"bucket": "sales", "vendor": "Salaries – Sales", "amount": 7989.63},
            {"bucket": "sales", "vendor": "GoHighLevel (CRM)", "amount": 1039.87},
            {"bucket": "sales", "vendor": "Stripe Collection Fees", "amount": 1731.78, "notes": "Payment processing on collected revenue"},
            {"bucket": "sales", "vendor": "Stripe Billing Usage Fee", "amount": 5.01},
            {"bucket": "sales", "vendor": "Whop Fees", "amount": 1885.52, "notes": "Platform fees on low-ticket / community revenue"},
            {"bucket": "marketing_salaries", "vendor": "Salaries – Marketing", "amount": 21083.01},
            {"bucket": "tech_tools", "vendor": "Tools – Funnel", "amount": 10629.42, "notes": "Webinar, landing pages, booking"},
            {"bucket": "tech_tools", "vendor": "Tool – Marketing", "amount": 3630.31, "notes": "Includes paid ads mgr retainers (Nick, Juan)"},
            {"bucket": "tech_tools", "vendor": "Tools – Automation", "amount": 385.67},
            {"bucket": "tech_tools", "vendor": "Tool – Project Mgmt (50%)", "amount": 926.07, "notes": "50% allocated to revenue team"},
            {"bucket": "tech_tools", "vendor": "Tool – CyberSecurity", "amount": 69.11},
            {"bucket": "experiments", "vendor": "Offer Workshop Exp", "amount": 2619.33, "notes": "Classify with Lloyd"},
            {"bucket": "experiments", "vendor": "VA Pre-Training", "amount": 1060.81},
        ],
    },
    {
        "period_start": "2026-04-01",
        "period_end": "2026-04-30",
        "replace": True,
        "items": [
            {"bucket": "sales", "vendor": "Salaries – Sales", "amount": 20041.42},
            {"bucket": "sales", "vendor": "GoHighLevel (CRM)", "amount": 516.66},
            {"bucket": "sales", "vendor": "Stripe Collection Fees", "amount": 837.45, "notes": "Payment processing on collected revenue"},
            {"bucket": "sales", "vendor": "Stripe Billing Usage Fee", "amount": 201.09},
            {"bucket": "sales", "vendor": "Whop Fees", "amount": 1582.88, "notes": "Platform fees on low-ticket / community revenue"},
            {"bucket": "marketing_salaries", "vendor": "Salaries – Marketing", "amount": 12082.31},
            {"bucket": "tech_tools", "vendor": "Tools – Funnel", "amount": 16006.68, "notes": "Webinar, landing pages, booking"},
            {"bucket": "tech_tools", "vendor": "Tool – Marketing", "amount": 6559.33, "notes": "Includes paid ads mgr retainers (Nick, Juan)"},
            {"bucket": "tech_tools", "vendor": "Tools – Automation", "amount": 375.57},
            {"bucket": "tech_tools", "vendor": "Tool – Project Mgmt (50%)", "amount": 656.88, "notes": "50% allocated to revenue team"},
            {"bucket": "tech_tools", "vendor": "Tool – CyberSecurity", "amount": 88.16},
            {"bucket": "experiments", "vendor": "VA Pre-Training", "amount": 349.48},
        ],
    },
    {
        "period_start": "2026-05-01",
        "period_end": "2026-05-07",
        "replace": True,
        "items": [
            {"bucket": "marketing_salaries", "vendor": "Salaries – Marketing", "amount": 234.65},
        ],
    },
]


def apply_categorization_rules(items: list) -> list:
    """Apply Lloyd's categorization corrections to a list of expense items.

    Rules are maintained at the top of this file. Update those sets when
    new vendors are identified, not here.
    """
    result = []
    for item in items:
        key = item.get("vendor", "").upper()

        if key in EXCLUDE_VENDORS:
            continue

        corrected = dict(item)

        if key in TECH_TOOLS_OVERRIDE:
            corrected["bucket"] = "tech_tools"
        elif key in EXPERIMENTS_OVERRIDE:
            corrected["bucket"] = "experiments"

        if key in HALF_VENDORS:
            corrected["amount"] = round(corrected["amount"] / 2, 2)
            corrected["vendor"] = corrected["vendor"] + " (50%)"
            corrected.setdefault("notes", "")
            corrected["notes"] = (corrected["notes"] + " (50% revenue-team allocation)").strip()

        result.append(corrected)

    return result


def convert_to_usd(items: list, period_start: str) -> list:
    """Convert EUR amounts to USD using the historical monthly average rate."""
    month = period_start[:7]
    rate = EUR_USD_RATES.get(month)
    if rate is None:
        raise ValueError(f"No EUR/USD rate for month {month}. Add it to EUR_USD_RATES.")
    return [{**item, "amount": round(item["amount"] * rate, 2)} for item in items]


def post_month(payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        RAILWAY_URL,
        data=data,
        headers={
            "Authorization": f"Bearer {BEARER}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def main():
    grand_total = 0.0
    for m in MONTHS:
        items = apply_categorization_rules(m["items"])
        items = convert_to_usd(items, m["period_start"])
        month_total = sum(i["amount"] for i in items)
        grand_total += month_total
        payload = {
            "period_start": m["period_start"],
            "period_end": m["period_end"],
            "replace": m.get("replace", True),
            "items": items,
        }
        print(
            f"Posting {m['period_start']} → {m['period_end']}  "
            f"({len(items)} items, ${month_total:,.2f})...",
            end=" ",
            flush=True,
        )
        result = post_month(payload)
        print(f"OK — {result.get('rows_upserted', result)}")
    print(f"\nDone. Grand total seeded: ${grand_total:,.2f}")


if __name__ == "__main__":
    main()
