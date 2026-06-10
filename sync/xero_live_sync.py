#!/usr/bin/env python3
"""Live Xero P&L → QS Revenue Dashboard expense + revenue sync.

Usage:
    python qs-dashboard/sync/xero_live_sync.py --month 2026-05 --token "$XERO_JWT"
    python qs-dashboard/sync/xero_live_sync.py --month 2026-05 --token "$XERO_JWT" --post

Requires a short-lived Xero access token (from API Explorer — see /sync-pnl-month skill).

Expense side: fetches P&L expense rows locally, classifies vendors, converts EUR→USD,
previews result. With --post, submits to Railway.

Revenue side: with --post, triggers the server-side /xero/sync-revenue endpoint,
which fetches income rows and upserts them into revenue_line_items.
"""

import argparse
import calendar
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

# Allow importing from the qs-dashboard package (vendor_classification lives there)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.queries.vendor_classification import classify_vendor  # noqa: E402

# ── Constants ─────────────────────────────────────────────────────────────────

XERO_TENANT_ID = "3bead22e-28ff-4eb1-92cd-9b9d648e188a"
XERO_PNL_URL   = "https://api.xero.com/api.xro/2.0/Reports/ProfitAndLoss"

RAILWAY_BASE  = "https://qs-revenue-dashboard-production.up.railway.app"
RAILWAY_TOKEN = "RAILWAY_BEARER_REMOVED"

# ECB monthly average EUR/USD rates — updated as new months are confirmed
EUR_USD_RATES: dict[str, float] = {
    "2025-10": 1.1630,
    "2025-11": 1.1560,
    "2025-12": 1.1709,
    "2026-01": 1.1738,
    "2026-02": 1.1824,
    "2026-03": 1.1558,
    "2026-04": 1.1706,
    "2026-05": 1.1729,
}

# ── EUR/USD rate ──────────────────────────────────────────────────────────────


def get_eur_usd_rate(year: int, month: int) -> float:
    """Return ECB monthly average EUR/USD. Hardcoded table first; Frankfurter fallback."""
    key = f"{year}-{month:02d}"
    if key in EUR_USD_RATES:
        return EUR_USD_RATES[key]

    # Live fetch from Frankfurter (date-range average)
    last_day = calendar.monthrange(year, month)[1]
    start = f"{year}-{month:02d}-01"
    end   = f"{year}-{month:02d}-{last_day:02d}"
    url   = f"https://api.frankfurter.dev/{start}..{end}?from=EUR&to=USD"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
        usd_values = [v["USD"] for v in data.get("rates", {}).values() if "USD" in v]
        if usd_values:
            rate = round(sum(usd_values) / len(usd_values), 4)
            print(f"  Frankfurter EUR/USD {key}: {rate} (avg {len(usd_values)} days)")
            return rate
    except Exception as exc:
        print(f"  ⚠️  Frankfurter rate fetch failed: {exc} — using 1.10 fallback")
    return 1.10


# ── Xero P&L fetch ────────────────────────────────────────────────────────────


def fetch_xero_pnl(access_token: str, period_start: str, period_end: str) -> list[dict]:
    """Pull Xero P&L and return all non-summary line items with their section title.

    Each item: {section: str, name: str, amount_eur: float}
    Income section rows are included — caller filters by section.
    """
    params = urllib.parse.urlencode({"fromDate": period_start, "toDate": period_end})
    req = urllib.request.Request(
        f"{XERO_PNL_URL}?{params}",
        headers={
            "Authorization":  f"Bearer {access_token}",
            "Xero-Tenant-Id": XERO_TENANT_ID,
            "Accept":         "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        print(f"\n❌ Xero P&L API error {exc.code}: {body[:500]}")
        sys.exit(1)

    rows = data.get("Reports", [{}])[0].get("Rows", [])
    items = []
    for section in rows:
        if section.get("RowType") != "Section":
            continue
        title = section.get("Title", "").strip()
        for row in section.get("Rows", []):
            if row.get("RowType") in ("SummaryRow", "Header"):
                continue
            cells = row.get("Cells", [])
            if len(cells) < 2:
                continue
            name   = cells[0].get("Value", "").strip()
            raw    = cells[1].get("Value", "0").replace(",", "").strip()
            if not name or not raw:
                continue
            try:
                amount = float(raw)
            except ValueError:
                continue
            if amount == 0:
                continue
            items.append({"section": title, "name": name, "amount_eur": amount})

    return items


# ── Expense classification ────────────────────────────────────────────────────


def build_expense_items(
    pnl_rows: list[dict],
    eur_usd: float,
) -> tuple[list[dict], list[dict]]:
    """Classify P&L expense rows and convert to USD.

    Returns (classified_items, unclassified_rows).
    Skips Income section (handled by server-side revenue sync).
    Skips non_revenue items (delivery team costs, bank fees — never shown on P&L).
    """
    classified: list[dict] = []
    unclassified: list[dict] = []

    for row in pnl_rows:
        if row["section"].lower() == "income":
            continue  # revenue sync handles this side

        vendor   = row["name"]
        eur_amt  = row["amount_eur"]
        usd_amt  = round(abs(eur_amt) * eur_usd, 2)  # abs() — Xero shows expenses as positive
        bucket   = classify_vendor(vendor)

        if bucket == "non_revenue":
            continue  # delivery-team / bank costs — hidden from dashboard

        item = {
            "bucket": bucket,
            "vendor": vendor,
            "amount": usd_amt,
            "notes":  f"Xero P&L — {row['section']} — EUR {eur_amt:,.2f} × {eur_usd} = USD {usd_amt:,.2f}",
        }
        classified.append(item)

        if bucket == "unclassified":
            unclassified.append({"vendor": vendor, "section": row["section"], "amount_usd": usd_amt})

    return classified, unclassified


# ── Preview ───────────────────────────────────────────────────────────────────

_BUCKET_LABELS = {
    "sales":               "Sales",
    "marketing_salaries":  "Marketing Salaries",
    "tech_tools":          "Tech & Tools",
    "advertising":         "Digital Advertising",
    "experiments":         "Experiments",
    "unclassified":        "⚠️ Unclassified",
}


def print_preview(month: str, items: list[dict], unclassified: list[dict], eur_usd: float) -> None:
    print(f"\n{'='*62}")
    print(f"  P&L Expense Preview — {month}  (EUR/USD: {eur_usd:.4f})")
    print(f"{'='*62}")

    grouped: dict[str, list[dict]] = {}
    for it in items:
        grouped.setdefault(it["bucket"], []).append(it)

    total = 0.0
    for key, label in _BUCKET_LABELS.items():
        bucket_items = [i for i in grouped.get(key, []) if i["bucket"] != "unclassified"]
        if not bucket_items:
            continue
        subtotal = sum(i["amount"] for i in bucket_items)
        total += subtotal
        print(f"\n  [{label}]  ${subtotal:,.2f}")
        for it in bucket_items:
            print(f"    • {it['vendor']:<46}  ${it['amount']:>10,.2f}")

    print(f"\n  {'─'*57}")
    print(f"  Total Expenses:  ${total:,.2f}")

    if unclassified:
        print(f"\n  ⚠️  Unclassified vendors — will be SKIPPED in POST ({len(unclassified)}):")
        for u in unclassified:
            print(f"    • {u['vendor']:<40}  [{u['section']}]  ${u['amount_usd']:>10,.2f}")
        print("  → Add to qs-dashboard/db/queries/vendor_classification.py, then re-run.")

    print(f"\n{'='*62}\n")


# ── Railway HTTP helpers ──────────────────────────────────────────────────────


def _railway_request(url: str, payload: dict | None = None) -> dict:
    """POST to Railway with bearer auth. payload=None sends empty body."""
    body = json.dumps(payload).encode("utf-8") if payload is not None else b""
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": f"Bearer {RAILWAY_TOKEN}",
            "Content-Type":  "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", errors="replace")
        print(f"  ❌ HTTP {exc.code}: {body_text[:500]}")
        sys.exit(1)


# ── Post functions ────────────────────────────────────────────────────────────


def post_expenses(month: str, items: list[dict]) -> dict:
    """POST classified expense items to Railway /api/dashboard/expenses/upsert."""
    year, mon = int(month[:4]), int(month[5:7])
    last_day   = calendar.monthrange(year, mon)[1]

    # Exclude unclassified from POST — they were shown in preview as a warning
    post_items = [i for i in items if i["bucket"] != "unclassified"]

    payload = {
        "period_start": f"{year}-{mon:02d}-01",
        "period_end":   f"{year}-{mon:02d}-{last_day:02d}",
        "replace":      True,
        "items":        post_items,
    }
    url = f"{RAILWAY_BASE}/api/dashboard/expenses/upsert"
    print(f"  POST {url}")
    return _railway_request(url, payload)


def post_revenue_sync(month: str, xero_token: str) -> dict:
    """Trigger server-side revenue sync: POST /xero/sync-revenue?month=...&xero_token=..."""
    params = urllib.parse.urlencode({"month": month, "xero_token": xero_token})
    url    = f"{RAILWAY_BASE}/xero/sync-revenue?{params}"
    print(f"  POST {url[:80]}...")
    return _railway_request(url)


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync Xero P&L expenses (and optionally revenue) to QS Revenue Dashboard",
    )
    parser.add_argument("--month", required=True, help="YYYY-MM, e.g. 2026-05")
    parser.add_argument("--token", required=True, help="Xero access token (JWT from API Explorer)")
    parser.add_argument("--post", action="store_true", help="Submit to Railway (default: preview only)")
    args = parser.parse_args()

    month = args.month.strip()
    try:
        year, mon = int(month[:4]), int(month[5:7])
        if not (1 <= mon <= 12):
            raise ValueError
    except (ValueError, IndexError):
        print(f"❌ Invalid month: {month!r} — use YYYY-MM format")
        sys.exit(1)

    last_day     = calendar.monthrange(year, mon)[1]
    period_start = f"{year}-{mon:02d}-01"
    period_end   = f"{year}-{mon:02d}-{last_day:02d}"

    print(f"\nFetching Xero P&L for {month} ({period_start} → {period_end})...")

    eur_usd  = get_eur_usd_rate(year, mon)
    pnl_rows = fetch_xero_pnl(args.token, period_start, period_end)

    if not pnl_rows:
        print("❌ No rows returned from Xero P&L — check that the month is reconciled in Xero.")
        sys.exit(1)

    expense_items, unclassified = build_expense_items(pnl_rows, eur_usd)
    print_preview(month, expense_items, unclassified, eur_usd)

    if not args.post:
        print("  (dry run — pass --post to submit to Railway)\n")
        return

    if unclassified:
        print(f"  ⚠️  {len(unclassified)} unclassified vendor(s) skipped.\n")

    # Post expenses
    print("Posting expenses to Railway...")
    exp_result  = post_expenses(month, expense_items)
    exp_rows    = exp_result.get("rows_upserted", exp_result)
    print(f"  ✅ Expenses: {exp_rows} rows upserted")

    # Trigger revenue sync
    print("Syncing revenue from Xero P&L...")
    rev_result  = post_revenue_sync(month, args.token)
    rev_rows    = rev_result.get("rows_upserted", "?")
    rev_rate    = rev_result.get("eur_usd_rate", eur_usd)
    print(f"  ✅ Revenue: {rev_rows} rows upserted (EUR/USD: {rev_rate:.4f})")

    print(f"\n✅ {month} synced — expenses + revenue loaded to dashboard.\n")


if __name__ == "__main__":
    main()
