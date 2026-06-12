# UX Design Spec — Deals Page v1 (Live + Historical lenses)
**Locked:** 2026-06-12 (Lloyd approved mockup prototypes/deals-redesign.html). Static HTML+JS (no React).

## IA decisions
- Deals page (static/deals.html) hosts TWO lenses via a segmented toggle: **Live · This Month** (DEFAULT) and **Historical · Reconciled**.
- LIVE lens = the per-rep Whop cash view RELOCATED from static/pnl.html. Per-rep collapsible buckets (existing renderBucket pattern); totals strip (Net Cash Collected / Gross Contract / Deals / Flagged); HIGH/MEDIUM confidence + Splitit/ClarityPay -15% badges.
- LIVE lens month nav = OWN deal-driven picker (prev/next arrows + dropdown), DEFAULTS to current month, lists months that have deals (new GET /api/dashboard/pnl/whop-live/months) — fixes the bug where May/June were unreachable because the picker was chained to P&L revenue periods.
- Per deal row: explicit labels — big right number tagged "CASH COLLECTED"; left meta "Contract $X · date". (Fixes the ambiguous-number complaint.)
- HISTORICAL lens = the EXISTING Deal Reconciliation content (summary cards + filterable/sortable matched-deals table) shown as-is behind the toggle. Its rep filter is the cross-month "all of a rep's deals" view.
- Remove the Live Whop section from static/pnl.html entirely.

## DEFERRED (NOT v1)
- Historical lens month-selection / month-nav -> deferred to the main UX redesign (Lloyd, 2026-06-12).
- Top-nav consolidation -> deferred (separate).
- Net-new vs recurring monthly split -> v2 (blueprints/monthly_cash_ledger_blueprint_v2.md).

## Empty states
- Live lens, month with no deals -> "No Whop-settled deals closed this month yet — they'll appear here as they close, refreshed nightly." (not an error)

## Framework: static HTML + vanilla JS (whole dashboard is). Reuse existing dark-theme tokens + renderBucket collapsible.
