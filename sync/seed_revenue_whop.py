#!/usr/bin/env python3
"""Seed revenue_line_items from Whop payments API.

Pulls all Whop payments (153 across 16 pages), classifies by product type,
groups by month using paid_at (for collected cash) or created_at (for open AR),
and upserts to the Railway backend.

Run:
    python3 sync/seed_revenue_whop.py

What it does:
    1. Fetches all payments from Whop API (paginated)
    2. Classifies each payment as cash_collected (status=paid) or splitit_ar (status=open)
    3. Groups by calendar month and product type (high_ticket | saas)
    4. POSTs to POST /api/dashboard/revenue/upsert with replace=true per month

Idempotency: replace=true on each period → re-running produces the same result.
"""

import json
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────

WHOP_API_KEY = "WHOP_KEY_REMOVED"
RAILWAY_URL = "https://qs-revenue-dashboard-production.up.railway.app"
RAILWAY_TOKEN = "RAILWAY_BEARER_REMOVED"

# Only seed these months (Oct 2025 → Apr 2026) — May 2026 excluded (unreconciled)
INCLUDE_MONTHS = {
    "2025-10", "2025-11", "2025-12",
    "2026-01", "2026-02", "2026-03", "2026-04",
}

# ── Product classification ────────────────────────────────────────────────────
# These three product IDs are the quantumSCALE Institute consulting program.
# Everything else (Calendar Automation, etc.) is SaaS.
HIGH_TICKET_PRODUCTS = {
    "prod_7MNNKNOvuS4V5",   # quantumSCALE Institute (main)
    "prod_OicLQ3n7l2pPQ",   # quantumSCALE Institute (variant)
    "prod_MOqVyn0Tj36mR",   # quantumSCALE Institute (variant)
}


def classify_product(product_id: str) -> str:
    return "high_ticket" if product_id in HIGH_TICKET_PRODUCTS else "saas"


def whop_get(path: str) -> dict:
    """Make a GET request to Whop API v2 via curl (avoids Python SSL issues)."""
    result = subprocess.run(
        [
            "curl", "-s", "-X", "GET",
            f"https://api.whop.com/api/v2{path}",
            "-H", f"Authorization: Bearer {WHOP_API_KEY}",
        ],
        capture_output=True, text=True, check=True,
    )
    return json.loads(result.stdout)


def fetch_all_payments() -> list[dict]:
    """Fetch all Whop payments across all pages."""
    all_payments = []
    page = 1
    while True:
        data = whop_get(f"/payments?page={page}&per_page=50")
        payments = data.get("data", [])
        all_payments.extend(payments)
        total_pages = data["pagination"]["total_page"]
        print(f"  Page {page}/{total_pages} — {len(payments)} payments")
        if page >= total_pages:
            break
        page += 1
    print(f"  Total fetched: {len(all_payments)} payments")
    return all_payments


def ts_to_month(ts: int | None) -> str | None:
    """Convert Unix timestamp to YYYY-MM string (UTC)."""
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m")


def month_to_period(month: str) -> tuple[str, str]:
    """Convert YYYY-MM to (period_start, period_end) as first/last day strings."""
    import calendar
    year, mo = int(month[:4]), int(month[5:7])
    last_day = calendar.monthrange(year, mo)[1]
    return f"{month}-01", f"{month}-{last_day:02d}"


def process_payments(payments: list[dict]) -> dict:
    """
    Returns:
        {
          "2025-10": {
            ("cash_collected", "high_ticket"): {"amount": X, "count": N},
            ("splitit_ar", "high_ticket"):    {"amount": X, "count": N},
            ...
          },
          ...
        }
    """
    # month → (category, product_type) → {amount, count}
    buckets: dict[str, dict[tuple, dict]] = defaultdict(lambda: defaultdict(lambda: {"amount": 0.0, "count": 0}))

    skipped_free = 0
    skipped_month = 0
    skipped_refunded = 0

    for p in payments:
        prod_id = p.get("product", "")
        processor = p.get("payment_processor", "")
        status = p.get("status", "")
        final_amount = float(p.get("final_amount") or 0)
        refunded_amount = float(p.get("refunded_amount") or 0)
        paid_at = p.get("paid_at")
        created_at = p.get("created_at")
        currency = p.get("currency", "usd")

        # Skip free/trial payments
        if processor == "free" or final_amount == 0:
            skipped_free += 1
            continue

        # Skip fully refunded payments
        if refunded_amount >= final_amount and final_amount > 0:
            skipped_refunded += 1
            continue

        # Determine effective amount (net of partial refunds)
        effective_amount = final_amount - refunded_amount

        # Classify
        product_type = classify_product(prod_id)

        if status == "paid":
            # Cash collected — attribute to month when payment landed (paid_at)
            month = ts_to_month(paid_at) or ts_to_month(created_at)
            category = "cash_collected"
        elif status == "open" and processor == "splitit":
            # Splitit AR — future installment, attribute to month it was created
            month = ts_to_month(created_at)
            category = "splitit_ar"
        else:
            # Unknown status (failed, etc.) — skip
            continue

        if month not in INCLUDE_MONTHS:
            skipped_month += 1
            continue

        key = (category, product_type)
        buckets[month][key]["amount"] += effective_amount
        buckets[month][key]["count"] += 1

    print(f"\n  Skipped: {skipped_free} free, {skipped_refunded} fully-refunded, {skipped_month} out-of-range months")
    return buckets


def build_items(month_buckets: dict) -> list[dict]:
    """Flatten bucket dict into list of revenue items for the API."""
    items = []
    for key, data in month_buckets.items():
        category, product_type = key
        items.append({
            "source": "whop",
            "category": category,
            "product_type": product_type,
            "amount": round(data["amount"], 2),
            "payment_count": data["count"],
            "notes": f"Seeded from Whop API — {data['count']} payment(s)",
        })
    return items


def post_to_railway(period_start: str, period_end: str, items: list[dict]) -> dict:
    """POST revenue items to Railway backend via curl."""
    payload = json.dumps({
        "period_start": period_start,
        "period_end": period_end,
        "items": items,
        "replace": True,
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
    print("=" * 60)
    print("Whop Revenue Seed")
    print("=" * 60)

    # 1. Fetch all payments
    print("\n[1] Fetching all Whop payments...")
    payments = fetch_all_payments()

    # 2. Process into monthly buckets
    print("\n[2] Processing payments into monthly buckets...")
    all_buckets = process_payments(payments)

    if not all_buckets:
        print("  No payments found in target months. Exiting.")
        sys.exit(0)

    # 3. Preview what we're about to seed
    print("\n[3] Monthly revenue breakdown:")
    print(f"  {'Month':<10}  {'Category':<16}  {'Type':<12}  {'Amount':>10}  {'Pmts':>5}")
    print("  " + "-" * 58)
    for month in sorted(all_buckets.keys()):
        for (cat, ptype), data in sorted(all_buckets[month].items()):
            print(f"  {month:<10}  {cat:<16}  {ptype:<12}  ${data['amount']:>9,.2f}  {data['count']:>5}")

    # 4. Seed each month
    print(f"\n[4] Seeding {len(all_buckets)} months to Railway...")
    total_rows = 0
    for month in sorted(all_buckets.keys()):
        period_start, period_end = month_to_period(month)
        items = build_items(all_buckets[month])
        response = post_to_railway(period_start, period_end, items)
        rows = response.get("rows_upserted", "?")
        total_rows += rows if isinstance(rows, int) else 0
        print(f"  {month}: {rows} rows upserted — {response}")

    print(f"\n✓ Done. Total rows upserted: {total_rows}")
    print("\nVerify with:")
    print(f"  curl -s '{RAILWAY_URL}/api/dashboard/revenue/summary' | python3 -m json.tool")


if __name__ == "__main__":
    main()
