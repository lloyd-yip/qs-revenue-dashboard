# Blueprint: Whop Live Revenue — V2
# Feature: Real-time Whop cash collected + contract value on P&L page
# Date: 2026-06-11
# Status: APPROVED — fill in function bodies only, structure is locked

## Changes from V1

- Extracted `_parse_month_range()` utility — month string→date range parsing removed from route handler
- Extracted `_detect_external_processor()` sub-helper from `_compute_payment_metrics` — satisfies single-purpose rule
- Added `WhopPaymentItem` Pydantic model at Whop API boundary — validates payment payload shape
- Added `WhopLiveResponse` Pydantic model on endpoint — typed response contract
- Added `Query(pattern=...)` validation on `month` parameter
- Per-row try/except in `refresh_current_month_payment_metrics()` — graceful degradation on Whop API errors
- Added `asyncio.sleep(0.2)` between Whop API calls — rate limit protection
- Skip rows with `metrics_updated_at` within last 6h — idempotency window protection
- Frontend: filter deals by `first_payment_date` in month — prevents historical installment inflation
- Frontend: per-deal confidence badge — Lloyd sees data quality signal
- Frontend: NULL `ghl_owner_name` grouped under "Unassigned"

---

## INTENT

```yaml
outcome_trigger: >
  Lloyd cannot see current-month revenue without waiting 10+ days for Xero
  reconciliation; Whop has the data in real-time but the dashboard doesn't show it.

success_state: >
  P&L page shows "Live Whop Revenue" section refreshed at EOD daily, with
  per-rep net cash collected and contract value for current-month deals,
  fee flags for Splitit/ClarityPay deals, and anomaly badges for internal
  plans >3 installments.

observable_done_state:
  - GET /api/dashboard/pnl/whop-live?month=2026-06 returns 200 with per_rep array
  - P&L page renders "Live Whop Revenue" section with per-rep collapsible rows
  - DealWhopMatch rows for current-month deals carry net_cash_collected, is_claritypay, provider_fee_pct, plan_months_flag after EOD cron fires
  - plan_months_flag=True only for internal plans (no Splitit/ClarityPay) with total_installments > 3
  - Scheduler _run_whop_refresh() fires at 22:00 UTC daily, logs completion without raising

depends_on: []
```

---

## SCOPE_FENCE

```yaml
explicitly_excluded:
  - item: Historical month data refresh
    reason: Cron only refreshes current-month matches; past months use snapshot data from original matching run
  - item: Flagged deal review workflow / approval UI
    reason: plan_months_flag is data-only in v1 — surfaced as badge in table, no in-app review flow
  - item: Wire transfer (Wise) deals in live section
    reason: No Whop membership ID — no real-time data available
  - item: Stripe-matched deals in live section
    reason: Already included via total_paid from existing match data; no separate Stripe refresh needed
  - item: ghl_owner_name normalization / dedup
    reason: Pre-existing data quality issue; out of scope for this blueprint
  - item: Daylight-saving-aware cron scheduling
    reason: 22:00 UTC is close enough; exact DST alignment deferred

conformance_rules:
  allowed_without_flagging:
    - Error handling for states listed in VERIFICATION_CONTRACTS
    - Logging that does not alter return values
    - Type narrowing guards (isinstance checks)
    - asyncio.sleep() calls for rate limiting
  flag_for_review:
    - Any conditional branch not covered by acceptance_criteria
    - Any import not declared in the file's Dependencies header
  disallowed:
    - DB queries in route handlers (routes → queries → DB)
    - Writing to deal_whop_matches from the dashboard route (read-only)
    - Overwriting rows where is_confirmed=True (existing guard must cover new columns)
    - Bypassing WhopPaymentItem validation on raw Whop API response
```

---

## VERIFICATION_CONTRACTS

```yaml
acceptance_criteria:
  - function: _detect_external_processor
    given: list of paid payments where one has payment_processor="splitit"
    then: returns (True, False) — is_splitit=True, is_claritypay=False

  - function: _detect_external_processor
    given: list of paid payments where one has payment_processor="claritypay"
    then: returns (False, True) — is_splitit=False, is_claritypay=True

  - function: _detect_external_processor
    given: list of paid payments with no payment_processor field or empty string
    then: returns (False, False) — internal plan

  - function: _compute_payment_metrics
    given: total_paid=10000, is_splitit=True
    then: net_cash_collected=8500.0, provider_fee_pct=0.15

  - function: _compute_payment_metrics
    given: total_paid=10000, is_splitit=False, is_claritypay=False
    then: net_cash_collected=10000.0, provider_fee_pct=0.0

  - function: _compute_payment_metrics
    given: is_splitit=False, is_claritypay=False, total_installments=4
    then: plan_months_flag=True

  - function: _compute_payment_metrics
    given: is_splitit=True, total_installments=12
    then: plan_months_flag=False (external financing, not internal plan)

  - function: get_whop_live_summary_for_month
    given: month with 3 deals: 2 high confidence, 1 unmatched
    then: only 2 rows returned (unmatched excluded)
    expected_query_shape:
      table: deal_whop_matches
      required_where: [first_payment_date, match_confidence]
      row_ceiling: 200

  - function: get_whop_live_summary_for_month
    given: two deals with NULL ghl_owner_name
    then: those deals appear under rep_name="Unassigned" in the response

  - function: pnl_whop_live
    given: month="2026-13" (invalid month)
    then: HTTP 422 Unprocessable Entity

  - function: refresh_current_month_payment_metrics
    given: 3 rows to refresh, Whop API returns 404 for row 2
    then: rows 1 and 3 are updated, errors count=1, function returns without raising

  - function: refresh_current_month_payment_metrics
    given: row with metrics_updated_at set to 2 hours ago
    then: row is skipped (within 6h idempotency window)

dependency_state:
  - name: _fetch_membership_payments
    status: stable
    contract: returns list of raw Whop payment dicts for a membership_id, paginated

  - name: upsert_deal_match
    status: stable
    contract: inserts or updates DealWhopMatch row; no-ops if is_confirmed=True

  - name: AsyncSessionLocal
    status: stable
    contract: async context manager that yields an SQLAlchemy AsyncSession
```

---

## SYSTEM_CONTRACTS

```yaml
preconditions:
  - whop001_add_claritypay_fields migration must be applied before any code is deployed
  - WHOP_API_KEY env var must be set in Railway (already present from existing matching engine)
  - DealWhopMatch table exists with match_confidence, first_payment_date, whop_membership_id columns

postconditions:
  success: deal_whop_matches updated with is_claritypay/provider_fee_pct/net_cash_collected/plan_months_flag for refreshed rows; no other tables written
  failure: partial writes possible per row (each row committed independently); failed rows leave existing values unchanged

ownership_boundaries:
  reads_from: [deal_whop_matches.whop_membership_id, deal_whop_matches.match_confidence, deal_whop_matches.first_payment_date, deal_whop_matches.metrics_updated_at, deal_whop_matches.total_installments, deal_whop_matches.is_splitit]
  writes_to: [deal_whop_matches.is_claritypay, deal_whop_matches.provider_fee_pct, deal_whop_matches.net_cash_collected, deal_whop_matches.plan_months_flag, deal_whop_matches.metrics_updated_at]
  must_never_touch: [revenue_line_items, expense_line_items, opportunities, sync_runs]

integration_failure_contracts:
  - dependency: Whop API (_fetch_membership_payments)
    on_timeout: log error, increment errors counter, continue to next row — never raise
    on_network_failure: same as timeout

observability_contracts:
  - function: refresh_current_month_payment_metrics
    external_dependency: Whop API
    required_log_fields: [function, status, duration_ms, refreshed_count, error_count]
    log_level: info
    b1_pattern: 'logger\.(info|error)\([^)]*refreshed[^)]*\)'
```

---

## Files in This Blueprint

| File | Status | Owns |
|------|--------|------|
| `migrations/versions/whop001_add_claritypay_fields.py` | NEW | 4 new columns on deal_whop_matches |
| `db/models.py` | MODIFIED | DealWhopMatch: +4 Mapped columns |
| `db/queries/deal_matches.py` | MODIFIED | upsert/enrich + get_whop_live_summary_for_month |
| `sync/match_deals_whop.py` | MODIFIED | _detect_external_processor + _compute_payment_metrics extensions + refresh_current_month_payment_metrics |
| `sync/scheduler.py` | MODIFIED | Daily EOD cron for Whop refresh |
| `api/routers/dashboard.py` | MODIFIED | GET /pnl/whop-live endpoint |
| `static/pnl.html` | MODIFIED | "Live Whop Revenue" section |

---

## Skeleton

---

### FILE 1: `migrations/versions/whop001_add_claritypay_fields.py` (NEW)

```python
# migrations/versions/whop001_add_claritypay_fields.py
# Owns: Adds ClarityPay detection and net cash tracking columns to deal_whop_matches
# Dependencies: alembic op

"""Add ClarityPay and net cash columns to deal_whop_matches.

Revision ID: whop001
Revises: wise001
Create Date: 2026-06-11

Plain English:
  - is_claritypay: TRUE when deal's Whop payments used ClarityPay processor.
    ClarityPay = external financing, QS receives 85% net (15% fee).
  - provider_fee_pct: 0.15 for Splitit/ClarityPay, 0.0 for internal plans.
  - net_cash_collected: total_paid * (1 - provider_fee_pct). What QS actually keeps.
  - plan_months_flag: TRUE when internal plan (no Splitit/ClarityPay) with
    total_installments > 3. Flags for Lloyd's review — unusually long internal plan.

VERIFICATION — run in Supabase SQL editor after deploy:
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'deal_whop_matches'
      AND column_name IN ('is_claritypay', 'provider_fee_pct', 'net_cash_collected', 'plan_months_flag');
    -- Should return 4 rows.

SILENT FAILURE SIGNAL: If net_cash_collected is NULL for all high/medium matches
after running the EOD refresh, check Railway logs for Whop API errors.
"""

from alembic import op


revision = "whop001"
down_revision = "wise001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE deal_whop_matches
        ADD COLUMN IF NOT EXISTS is_claritypay BOOLEAN,
        ADD COLUMN IF NOT EXISTS provider_fee_pct NUMERIC(5, 4),
        ADD COLUMN IF NOT EXISTS net_cash_collected NUMERIC(12, 2),
        ADD COLUMN IF NOT EXISTS plan_months_flag BOOLEAN
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE deal_whop_matches
        DROP COLUMN IF EXISTS is_claritypay,
        DROP COLUMN IF EXISTS provider_fee_pct,
        DROP COLUMN IF EXISTS net_cash_collected,
        DROP COLUMN IF EXISTS plan_months_flag
    """)
```

---

### FILE 2: `db/models.py` — DealWhopMatch additions (MODIFIED, additive only)

REFACTOR SCOPE: None. Add these 4 columns after `total_installments` in DealWhopMatch.

```python
# Add after: total_installments: Mapped[int | None]

# ClarityPay / fee detection (populated by _compute_payment_metrics)
is_claritypay: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
provider_fee_pct: Mapped[float | None] = mapped_column(Numeric(5, 4), nullable=True)
# net_cash_collected: total_paid * (1 - provider_fee_pct). NULL until EOD refresh runs.
net_cash_collected: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
# plan_months_flag: True = internal plan (no Splitit/ClarityPay) AND total_installments > 3.
plan_months_flag: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
```

---

### FILE 3: `db/queries/deal_matches.py` (MODIFIED)

REFACTOR SCOPE:
- `upsert_deal_match()`: additive — 4 new fields in `values()` and `set_` dicts. The existing `is_confirmed` guard fires BEFORE the pg_insert statement, so new columns are protected by the same guard. No call sites need changes — callers use `record.update(metrics)`.
- `enrich_deal_match_payments()`: additive — `net_cash_collected` null-guard block added.

```python
# db/queries/deal_matches.py
# [existing file — additions only, do not remove anything]

# ── ADDITIONS to upsert_deal_match() ──────────────────────────────────────────
# In the .values() call, add after total_installments:
#   is_claritypay=data.get("is_claritypay"),
#   provider_fee_pct=data.get("provider_fee_pct"),
#   net_cash_collected=data.get("net_cash_collected"),
#   plan_months_flag=data.get("plan_months_flag"),
#
# In the .on_conflict_do_update set_ dict, add same 4 keys.

# ── ADDITIONS to enrich_deal_match_payments() ─────────────────────────────────
# After the existing total_installments null-guard block, add:
#   if existing.net_cash_collected is None and payment_data.get("net_cash_collected") is not None:
#       updates["net_cash_collected"] = payment_data["net_cash_collected"]
#   if existing.is_claritypay is None and payment_data.get("is_claritypay") is not None:
#       updates["is_claritypay"] = payment_data["is_claritypay"]
#   if existing.provider_fee_pct is None and payment_data.get("provider_fee_pct") is not None:
#       updates["provider_fee_pct"] = payment_data["provider_fee_pct"]
#   if existing.plan_months_flag is None and payment_data.get("plan_months_flag") is not None:
#       updates["plan_months_flag"] = payment_data["plan_months_flag"]


async def get_whop_live_summary_for_month(
    session: AsyncSession,
    month_start: date,
    month_end: date,
) -> dict:
    """Return per-rep live Whop revenue aggregated from deal_whop_matches for a calendar month.

    Filters: first_payment_date in [month_start, month_end], confidence high/medium.
    Groups by ghl_owner_name (NULL owner → "Unassigned").
    Returns structured dict ready for WhopLiveResponse serialisation.
    """
    pass
```

---

### FILE 4: `sync/match_deals_whop.py` (MODIFIED)

REFACTOR SCOPE:
- `_compute_payment_metrics()`: Additive — new sub-helper called from it, 4 new return keys added. Existing keys unchanged. Call sites spread the return dict into `record` — new keys flow automatically. No call sites need changes.

```python
# sync/match_deals_whop.py
# [existing file — additions/modifications below]


def _detect_external_processor(paid_payments: list[dict]) -> tuple[bool, bool]:
    """Detect whether any paid payment used an external financing processor.

    Returns (is_splitit, is_claritypay) — mutually exclusive in practice.
    """
    pass


# ── MODIFICATION to _compute_payment_metrics() ───────────────────────────────
# After the existing is_splitit detection block, add:
#
#   is_splitit, is_claritypay = _detect_external_processor(paid)  # replaces inline splitit detection
#   is_external = is_splitit or is_claritypay
#   provider_fee_pct = 0.15 if is_external else 0.0
#   net_cash_collected = round(total_paid * (1 - provider_fee_pct), 2)
#   plan_months_flag = (
#       not is_external
#       and total_installments is not None
#       and total_installments > 3
#   )
#
# Add to return dict:
#   "is_claritypay": is_claritypay,
#   "provider_fee_pct": provider_fee_pct,
#   "net_cash_collected": net_cash_collected,
#   "plan_months_flag": plan_months_flag,
#
# NOTE: existing is_splitit detection inline code replaced by _detect_external_processor call.
# upfront_cash logic for Splitit (lines ~317-325 in original) stays — not replaced by this.


async def refresh_current_month_payment_metrics() -> dict:
    """Orchestrator: re-fetch Whop payment data for current-month high/medium matches.

    Designed for daily EOD cron. Faster than run_matching() — skips GHL contact
    resolution and Stripe pass. Only hits Whop payments API for matched rows.

    Idempotency window: rows with metrics_updated_at within last 6h are skipped.
    Per-row failure isolation: one Whop 404/timeout increments errors, does not abort.
    Rate limiting: asyncio.sleep(0.2) between Whop API calls.

    Returns: {refreshed: int, skipped: int, errors: int, flagged: int}
    """
    pass
```

---

### FILE 5: `sync/scheduler.py` (MODIFIED)

REFACTOR SCOPE: None. Additive only.

```python
# sync/scheduler.py
# [existing file — additions below]

# In create_scheduler(), add after the daily_appointment_resolver job:
#
#   scheduler.add_job(
#       _run_whop_refresh,
#       trigger=CronTrigger(hour=22, minute=0, timezone="UTC"),  # ~6pm EST
#       id="daily_whop_refresh",
#       name="Daily EOD Whop payment refresh",
#       replace_existing=True,
#       misfire_grace_time=300,
#       max_instances=1,
#   )


async def _run_whop_refresh() -> None:
    """Execute daily EOD Whop payment refresh for current-month deals."""
    pass
```

---

### FILE 6: `api/routers/dashboard.py` (MODIFIED)

REFACTOR SCOPE: None. Additive only.

```python
# api/routers/dashboard.py
# [existing file — additions below]

# Add to imports:
#   from datetime import date
#   from calendar import monthrange
#   from fastapi import Query


# ── Utility (add near top of file, not in a route handler) ────────────────────

def _parse_month_range(month: str) -> tuple[date, date]:
    """Parse "YYYY-MM" string into (month_start, month_end) date tuple.

    Raises ValueError on invalid input — caller converts to HTTP 422.
    """
    pass


# ── Response model ────────────────────────────────────────────────────────────

class WhopLiveDealItem(BaseModel):
    """One deal row inside a rep's collapsible table."""
    ghl_opportunity_id: str
    ghl_opportunity_name: str | None
    ghl_close_date: str | None
    first_payment_date: str | None
    gross_contract_value: float | None
    total_paid: float | None
    net_cash_collected: float | None
    provider_fee_pct: float | None
    is_splitit: bool | None
    is_claritypay: bool | None
    plan_months_flag: bool | None
    match_confidence: str
    total_installments: int | None


class WhopLiveRepRow(BaseModel):
    """Aggregated row for one rep in the Live Whop Revenue section."""
    rep_name: str
    deal_count: int
    gross_contract_value: float
    net_cash_collected: float
    flagged_count: int
    deals: list[WhopLiveDealItem]


class WhopLiveResponse(BaseModel):
    """Response shape for GET /pnl/whop-live."""
    month: str
    reps: list[WhopLiveRepRow]
    totals: dict  # {gross_contract_value, net_cash_collected, deal_count, flagged_count}
    last_refreshed: str | None  # ISO timestamp of most recent metrics_updated_at


# ── Endpoint ──────────────────────────────────────────────────────────────────

@router.get("/pnl/whop-live", response_model=WhopLiveResponse)
async def pnl_whop_live(
    month: str = Query(..., pattern=r"^\d{4}-\d{2}$", description="Month in YYYY-MM format"),
    db: AsyncSession = Depends(get_db),
) -> WhopLiveResponse:
    """Return real-time Whop revenue grouped by rep for a calendar month."""
    pass
```

---

### FILE 7: `static/pnl.html` (MODIFIED)

REFACTOR SCOPE: None. Additive only — new section and JS functions.

Design tokens (existing): `--bg: #0f1117`, `--surface: #1a1d27`, `--surface2: #22263a`, `--border: #2e3347`, `--accent: #6366f1`, `--text: #e2e8f0`, `--text2: #94a3b8`, `--text3: #64748b`, `--yellow: #f59e0b`.

```javascript
// Add these JS functions to the <script> block:

async function loadWhopLive(month) {
    /**
     * Fetch /pnl/whop-live?month=YYYY-MM and render section into #whop-live-section.
     * Shows loading state, handles empty state, handles API error.
     */
    // TODO: implement
}

function renderRepBucket(rep, isFirst) {
    /**
     * Render one rep's deals as a collapsible bucket row matching existing renderBucket style.
     * Header: rep_name + deal_count badge + net_cash_collected total.
     * Each deal row: opportunity name + confidence badge + net_cash + flags (plan_months_flag, is_splitit/claritypay).
     */
    // TODO: implement
}

function renderWhopLiveSection(data) {
    /**
     * Render full "Live Whop Revenue" section from WhopLiveResponse data.
     * Includes: section header, totals summary, per-rep collapsible rows, "Last refreshed" timestamp.
     */
    // TODO: implement
}
```

```html
<!-- Add inside renderPnL() after the contractValueCats block, before the Expenses section: -->

<!-- Live Whop Revenue section placeholder — populated by loadWhopLive() -->
<div id="whop-live-section"></div>

<!-- loadWhopLive is called in loadPnL() after the revenue/expenses fetch:
     const month = start.slice(0, 7);  // "YYYY-MM" from period_start
     await loadWhopLive(month);
-->
```

**CSS additions** (add to `<style>` block):
```css
/* Confidence badge — small inline tag next to deal name */
.confidence-badge {
  font-size: 10px; font-weight: 600; padding: 2px 6px;
  border-radius: 4px; text-transform: uppercase; letter-spacing: 0.04em;
}
.confidence-badge.high   { color: var(--green); background: var(--green-dim); }
.confidence-badge.medium { color: var(--yellow); background: var(--yellow-dim); }

/* Anomaly flag badge — for plan_months_flag deals */
.flag-badge {
  font-size: 10px; font-weight: 600; padding: 2px 6px;
  border-radius: 4px; color: var(--yellow); background: var(--yellow-dim);
}

/* Live Whop section last-refreshed timestamp */
.whop-last-refreshed {
  font-size: 11px; color: var(--text3); margin-bottom: 12px;
}
```

---

## Implementation Notes

1. **Migration first.** Apply `whop001_add_claritypay_fields` before deploying any code. The new columns are NULLABLE, so existing rows are unaffected.

2. **`_compute_payment_metrics()` refactor is a replacement, not an addition.** The existing inline Splitit detection (`splitit_payments = [p for p in paid if ...]`) must be REMOVED and replaced by the `_detect_external_processor()` call. Run `grep -n "splitit_payments" qs-dashboard/sync/match_deals_whop.py` — should return 0 results after the build.

3. **`upsert_deal_match()` existing confirmation guard covers new columns.** The guard `if existing and existing.is_confirmed: return` fires before the pg_insert statement. New columns in `values()` and `set_` are protected automatically.

4. **EOD cron refresh window.** `refresh_current_month_payment_metrics()` queries WHERE `first_payment_date` BETWEEN first and last day of current calendar month. Deals without `first_payment_date` (unmatched or payment not yet fetched) are skipped by the refresh — they still get new columns populated the next time `run_matching()` is triggered.

5. **Frontend loadPnL call.** `loadWhopLive(month)` must be called from `loadPnL(start, end)` after the existing revenue/expenses fetches complete. It is NOT part of the existing `Promise.all([revenue, expenses])` — it is a separate sequential call (different data, different UX section).

6. **No bearer token on pnl.html requests.** The existing pnl.html does NOT send auth headers on `/revenue` and `/expenses` calls. The new `/pnl/whop-live` call should match this pattern — check if the existing endpoints require auth from the browser or only from the sync scripts.

## Post-Build Grep Invariants

Run before declaring done:

```bash
# Inline splitit_payments detection must be removed — replaced by _detect_external_processor:
grep -n "splitit_payments" qs-dashboard/sync/match_deals_whop.py
# Expected: 0 results

# New columns must be in upsert_deal_match values AND set_ dicts:
grep -n "is_claritypay" qs-dashboard/db/queries/deal_matches.py
# Expected: ≥4 results (in values, set_, enrich_deal_match_payments, get_whop_live_summary_for_month)

# WhopLiveResponse must be declared before the endpoint:
grep -n "WhopLiveResponse" qs-dashboard/api/routers/dashboard.py
# Expected: ≥2 results (class declaration + response_model)

# Month validation must be on the endpoint param:
grep -n 'pattern.*\\d{4}-\\d{2}' qs-dashboard/api/routers/dashboard.py
# Expected: ≥1 result
```
