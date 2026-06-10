# Blueprint v2 — Xero Invoice Sync (Contract Value)
# Feature: Pull Xero ACCREC invoices monthly → store as contract_value in revenue_line_items → surface in P&L view
# Generated: 2026-06-10

---

## Changes from V1

| # | V1 Issue | V2 Fix |
|---|---|---|
| 1 | xero_invoices.py imported from xero_auth.py (router→router violation) | New `api/utils/xero_utils.py` owns shared constants + helpers |
| 2 | `_build_contract_value_items` did conversion + mapping + validation (violated one-purpose rule) | Split into `_parse_invoice_totals` (count + sum EUR) and `_to_revenue_item` (convert + format) |
| 3 | `replace=False` left stale rows on re-sync | Route performs category-scoped DELETE before upsert (category='contract_value' only) |
| 4 | pnl.html hardcodes `.filter(c.category === "cash_collected")` — contract_value would never render | pnl.html updated to render contract_value section separately |
| 5 | product_type/source undocumented | Hardcoded: `source='xero'`, `product_type='invoiced_total'` — constants in xero_invoices.py |
| 6 | No date boundary guard | `_fetch_xero_invoices` filters by invoice DateString; out-of-range invoices are logged and dropped |
| 7 | EUR/USD rate not stored per-row | notes field carries `EUR X.XX × rate = USD Y.YY` (consistent with existing sync pattern) |
| 8 | File size risk | xero_invoices.py scoped to ~180 lines by splitting helpers into xero_utils.py |

---

## INTENT

```
outcome_trigger: >
  Lloyd runs /sync-pnl-month and wants contract value (what was invoiced to clients)
  captured in the same monthly sync as expenses and cash collected.

success_state: >
  After running /sync-pnl-month with --post, the P&L view shows a Contract Value
  section sourced from Xero invoices, and Funnel Economics RORI can use it as
  the contract value numerator.

observable_done_state:
  - POST /xero/sync-invoices?month=2026-05&xero_token=... returns 200 with rows_upserted >= 1
  - revenue_line_items contains rows with source='xero', category='contract_value', product_type='invoiced_total' for the synced month
  - GET /api/dashboard/revenue/period?... response includes contract_value in categories array
  - pnl.html renders a "Contract Value (Invoiced)" section below Revenue Breakdown
  - python xero_live_sync.py --month 2026-05 --token $T --post prints "Contract Value: N invoices, $X synced"

depends_on: []
```

---

## SCOPE_FENCE

```
explicitly_excluded:
  - item: Per-deal invoice matching (linking Xero invoice to a specific GHL opportunity)
    reason: Phase 3 — requires fuzzy name matching + separate table. Monthly aggregate is sufficient for RORI v1.
  - item: Per-product-type invoice breakdown (high_ticket vs SaaS vs referral)
    reason: Xero invoice line item account mapping is complex; one invoiced_total row is sufficient for RORI.
  - item: Refactoring xero_auth.py (1,107 lines)
    reason: Scope explosion risk. xero_auth.py is NOT modified in this blueprint. Shared helpers extracted to new file.
  - item: Funnel Economics RORI wiring (reading contract_value to compute RORI)
    reason: Separate task — RORI query already reads from revenue_line_items; adding category pulls automatically.

conformance_rules:
  allowed_without_flagging:
    - Error handling for states listed in VERIFICATION_CONTRACTS
    - Logging with duration_ms
    - Type narrowing guards
  flag_for_review:
    - Any function that queries revenue_line_items with a WHERE on category and does not include category='contract_value'
    - Any import not declared in the file's Dependencies header
  disallowed:
    - DB queries inside route handlers (use db/queries layer)
    - Importing from api/routers/xero_auth.py in any new file
    - replace=True in upsert_revenue_line_items for invoice sync (would wipe cash_collected rows)
```

---

## VERIFICATION_CONTRACTS

```
acceptance_criteria:
  - function: _fetch_xero_invoices
    given: valid access_token, month with 3 AUTHORISED ACCREC invoices
    then: returns list of 3 dicts each with keys {InvoiceID, Total, DateString, Contact.Name, Status}
    rationale: confirms pagination and status filter work

  - function: _fetch_xero_invoices
    given: valid access_token, month with no invoices
    then: returns empty list (does not raise)
    rationale: empty month must not crash the sync — route returns rows_upserted=0

  - function: _fetch_xero_invoices
    given: expired/invalid access_token
    then: raises XeroAuthError (HTTP 401 from Xero)
    rationale: caller must get a clean error, not a silent empty list

  - function: _parse_invoice_totals
    given: list of 3 invoices with Total [1000.0, 500.0, 250.0]
    then: returns (total_eur=1750.0, count=3)

  - function: _parse_invoice_totals
    given: list contains one VOIDED invoice (should have been filtered upstream)
    then: VOIDED invoice is skipped; only AUTHORISED/PAID counted

  - function: xero_sync_invoices (route)
    given: valid month + token, 3 invoices found
    then: revenue_line_items has exactly 1 row for (period, source='xero', category='contract_value')
    rationale: confirms category-scoped delete + single-row upsert pattern is idempotent

  - function: xero_sync_invoices (route)
    given: called twice for same month
    then: revenue_line_items still has exactly 1 row (idempotent)
    rationale: re-sync must not accumulate duplicate rows

  - function: xero_sync_invoices (route)
    given: existing cash_collected rows for the same period
    then: cash_collected rows are untouched after invoice sync completes
    rationale: category-scoped delete must not wipe other categories

dependency_state:
  - name: upsert_revenue_line_items
    status: stable
    contract: inserts/upserts items into revenue_line_items; replace=False leaves other categories intact

  - name: get_revenue_for_period
    status: stable
    contract: returns categories array including any category present in revenue_line_items for the period
```

---

## SYSTEM_CONTRACTS

```
preconditions:
  - Xero OAuth token is valid (30-min window from API Explorer or refresh token flow)
  - revenue_line_items table exists with (period_start, period_end, source, category, product_type) unique constraint
  - XERO_TENANT_ID constant matches quantumSCALE Institute OÜ Xero org

postconditions:
  success: revenue_line_items gains exactly 1 row per synced month with category='contract_value'.
           All other rows for the period are untouched.
  failure: If Xero fetch fails → no DB write. If upsert fails after delete → 0 contract_value rows for period (recoverable by re-running). No partial writes within a single sync.

ownership_boundaries:
  reads_from: [Xero Invoices API]
  writes_to: [revenue_line_items.amount, revenue_line_items.notes, revenue_line_items.payment_count]
  must_never_touch: [revenue_line_items rows where category != 'contract_value', deal_whop_matches, opportunities]

integration_failure_contracts:
  - dependency: Xero Invoices API
    on_timeout: raise XeroApiError — caller surfaces HTTP 502
    on_network_failure: raise XeroApiError — no retry in route (CLI can re-run)

observability_contracts:
  - function: _fetch_xero_invoices
    external_dependency: Xero Invoices API
    required_log_fields: [function, month, invoice_count, duration_ms, status]
    log_level: info
    b1_pattern: 'logger\.(info|error)\([^)]*duration_ms[^)]*\)'

  - function: xero_sync_invoices
    external_dependency: Xero Invoices API + PostgreSQL
    required_log_fields: [function, month, rows_upserted, eur_usd_rate, total_eur]
    log_level: info
    b1_pattern: 'logger\.(info|error)\([^)]*rows_upserted[^)]*\)'
```

---

## Files in This Blueprint

| File | Action | Size estimate |
|---|---|---|
| `api/utils/xero_utils.py` | CREATE | ~70 lines |
| `api/routers/xero_invoices.py` | CREATE | ~180 lines |
| `db/queries/revenue.py` | MODIFY (additive) | +15 lines |
| `sync/xero_live_sync.py` | MODIFY (additive) | +25 lines |
| `static/pnl.html` | MODIFY | ~20 lines changed |
| `~/.claude/commands/sync-pnl-month.md` | MODIFY | +4 lines |

---

## BROWNFIELD AUDIT

### `db/queries/revenue.py`
**Redundant:** Nothing removed.
**Inconsistent:** `get_all_revenue_periods_summary` returns only `cash_collected` and `splitit_ar` pivot keys — will be inconsistent after adding `contract_value` to CATEGORY_ORDER unless the pivot is also updated.
**Misleading:** `grand_total` in `get_revenue_for_period` is currently documented as "total cash collected" — adding contract_value does NOT change grand_total (it remains cash-only). This is correct and should be left as-is to avoid breaking RORI calculations that depend on the existing grand_total semantics.

No REFACTOR SCOPE — purely additive changes. No functions removed. No call sites affected.

### `sync/xero_live_sync.py`
**Redundant:** Nothing.
**Inconsistent:** Nothing — new function added, existing functions unchanged.
**Misleading:** Nothing.

No REFACTOR SCOPE.

### `static/pnl.html`
**Redundant:** `.filter(c => c.category === "cash_collected")` at line 361 — this filter was correct before but now needs to be a separate Cash Collected block, with Contract Value rendered separately.
**Inconsistent:** Nothing else changes.
**Misleading:** Nothing.

CALL SITES for filter change: line 361 only. No other files reference this filter.

---

## V2 SKELETON

---

### FILE: `api/utils/xero_utils.py` (NEW)
```
# Owns: Shared Xero API constants and stateless helpers used across xero_* routers
# Dependencies: fastapi, config (settings)
```

```python
XERO_TENANT_ID: str = "3bead22e-28ff-4eb1-92cd-9b9d648e188a"

EUR_USD_RATES: dict[str, float] = {
    "2025-10": 1.1630, "2025-11": 1.1560, "2025-12": 1.1709,
    "2026-01": 1.1738, "2026-02": 1.1824, "2026-03": 1.1558,
    "2026-04": 1.1706, "2026-05": 1.1729,
}

_bearer_scheme: HTTPBearer  # FastAPI HTTPBearer instance

async def verify_bearer(credentials: HTTPAuthorizationCredentials | None = Security(_bearer_scheme)) -> None:
    """Raise 401 if the bearer token does not match settings.api_bearer_token."""
    pass

def get_eur_usd_rate(year: int, month: int) -> float:
    """Return the ECB monthly average EUR/USD rate; fetch from Frankfurter if month not in table."""
    pass
```

---

### FILE: `api/routers/xero_invoices.py` (NEW)
```
# Owns: POST /xero/sync-invoices — fetches Xero ACCREC invoices and upserts contract value per month
# Dependencies: fastapi, httpx, logging, api.utils.xero_utils, db.queries.revenue, db.session, db.models
```

```python
import logging
from datetime import date

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from api.utils.xero_utils import XERO_TENANT_ID, get_eur_usd_rate, verify_bearer
from db.models import RevenueLineItem
from db.queries.revenue import upsert_revenue_line_items
from db.session import AsyncSessionLocal

logger = logging.getLogger(__name__)
router = APIRouter(tags=["xero"])

XERO_INVOICES_URL = "https://api.xero.com/api.xro/2.0/Invoices"
_INVOICE_SOURCE   = "xero"
_INVOICE_CATEGORY = "contract_value"
_INVOICE_PRODUCT  = "invoiced_total"
_BILLABLE_STATUSES = {"AUTHORISED", "PAID"}


class XeroApiError(Exception):
    """Raised when the Xero API returns a non-200 response."""
    pass


class InvoiceSyncResult(BaseModel):
    month: str
    period_start: str
    period_end: str
    invoice_count: int
    total_eur: float
    total_usd: float
    eur_usd_rate: float
    rows_upserted: int


async def _fetch_xero_invoices(
    access_token: str,
    period_start: date,
    period_end: date,
) -> list[dict]:
    """Fetch all ACCREC invoices from Xero whose DateString falls within the period."""
    pass  # paginated; filter Type=ACCREC; include AUTHORISED+PAID; drop out-of-range by DateString; log duration_ms


def _parse_invoice_totals(invoices: list[dict]) -> tuple[float, int]:
    """Sum Total amounts and count invoices from a list of raw Xero invoice dicts."""
    pass  # skip any invoice whose Status not in _BILLABLE_STATUSES; sum Total; return (total_eur, count)


def _to_revenue_item(total_eur: float, eur_usd: float, month: str) -> dict:
    """Build a revenue_line_items-compatible dict from an invoice total and FX rate."""
    pass  # hardcode source, category, product_type; compute amount_usd; set notes with rate trail


@router.post(
    "/xero/sync-invoices",
    response_model=InvoiceSyncResult,
    dependencies=[Depends(verify_bearer)],
)
async def xero_sync_invoices(
    month: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
    xero_token: str = Query(..., description="Xero access token from API Explorer or OAuth flow"),
) -> InvoiceSyncResult:
    """Fetch Xero ACCREC invoices for a month, convert EUR→USD, and upsert as contract_value."""
    pass
    # Steps:
    # 1. Parse month → period_start, period_end
    # 2. get_eur_usd_rate(year, mon)
    # 3. _fetch_xero_invoices(xero_token, period_start, period_end)
    # 4. _parse_invoice_totals(invoices)
    # 5. Category-scoped DELETE: DELETE FROM revenue_line_items WHERE period=X AND category='contract_value'
    # 6. upsert_revenue_line_items(session, period_start, period_end, [item], replace=False)
    # 7. Log rows_upserted, total_eur, eur_usd_rate
    # 8. Return InvoiceSyncResult
```

---

### FILE: `db/queries/revenue.py` (MODIFY — additive)

CURRENT STRUCTURE
─────────────────
```python
CATEGORY_ORDER = ["cash_collected", "splitit_ar"]

CATEGORY_LABELS = {
    "cash_collected": "Cash Collected",
    "splitit_ar":     "Splitit AR (Outstanding)",
}

# get_revenue_for_period — returns grand_total, total_cash_collected, total_splitit_ar
# get_all_revenue_periods_summary — pivots on cash_collected and splitit_ar keys only
```

REPLACEMENT SKELETON
────────────────────
```python
CATEGORY_ORDER = ["cash_collected", "splitit_ar", "contract_value"]  # contract_value added

CATEGORY_LABELS = {
    "cash_collected":  "Cash Collected",
    "splitit_ar":      "Splitit AR (Outstanding)",
    "contract_value":  "Contract Value (Invoiced)",  # added
}

# get_revenue_for_period — add total_contract_value to return dict (additive, grand_total unchanged)
# get_all_revenue_periods_summary — add contract_value pivot key to return dicts (additive)
# All other functions: unchanged
```

No functions removed. No call sites to update.

---

### FILE: `sync/xero_live_sync.py` (MODIFY — additive only)

```python
# Add after post_revenue_sync():

def post_invoice_sync(month: str, xero_token: str) -> dict:
    """Trigger server-side invoice sync via POST /xero/sync-invoices."""
    pass  # same _railway_request pattern as post_revenue_sync; log duration; raise on HTTP error

# In main(), add after post_revenue_sync call:
#   print("Syncing contract value from Xero invoices...")
#   inv_result = post_invoice_sync(month, args.token)
#   print(f"  ✅ Contract Value: {inv_result.get('invoice_count','?')} invoices, ${inv_result.get('total_usd',0):,.2f}")
```

---

### FILE: `static/pnl.html` (MODIFY)

CURRENT STRUCTURE (line 359–377)
─────────────────────────────────
```javascript
if (rev && rev.categories && rev.categories.length > 0) {
  rev.categories
    .filter(c => c.category === "cash_collected")   // ← hardcoded, blocks contract_value
    .forEach((cat, i) => { ... renderBucket(...) })
}
```

REPLACEMENT SKELETON
────────────────────
```javascript
// Cash Collected section (unchanged logic, explicit filter)
if (rev && rev.categories && rev.categories.length > 0) {
  rev.categories
    .filter(c => c.category === "cash_collected")
    .forEach((cat, i) => { /* unchanged */ });

  // Contract Value section (new)
  const contractCats = rev.categories.filter(c => c.category === "contract_value");
  if (contractCats.length > 0) {
    html += `<div class="section-header">Contract Value (Invoiced)</div>`;
    contractCats.forEach((cat, i) => {
      html += renderBucket(
        `rev-${cat.category}`,
        cat.label,
        cat.total,
        cat.items.map(item => ({
          name: item.label,
          amount: item.amount,
          meta: `${item.payment_count} invoice${item.payment_count !== 1 ? "s" : ""}`,
          is_approximate: false,
          notes: item.notes,
        })),
        i === 0,
        `${cat.payment_count} invoice${cat.payment_count !== 1 ? "s" : ""}`,
      );
    });
  }
}
```

---

### FILE: `~/.claude/commands/sync-pnl-month.md` (MODIFY)

Add after Step 5 (Post):

```markdown
## Step 6 — Contract value included automatically

The `--post` flag in Step 5 already triggers invoice sync as step 3 inside `xero_live_sync.py`.
No separate command needed. Output will include:
  ✅ Contract Value: N invoices, $X synced

If invoice sync fails but expenses/revenue succeeded, re-run:
    python qs-dashboard/sync/xero_live_sync.py --month $MONTH --token "$TOKEN" --invoices-only
```

Note: `--invoices-only` flag is NOT in the current script — add it during implementation as a convenience.

---

## ALGORITHM DECISIONS

```
FUNCTION: _fetch_xero_invoices
APPROACH: Paginated GET with Type=ACCREC, per-page loop until empty response, then filter DateString client-side
WHY NOT server-side date filter only: Xero date params are inclusive but may return invoices from adjacent months
  depending on timezone handling. Client-side DateString check is a defensive second pass.

FUNCTION: xero_sync_invoices (category-scoped delete)
APPROACH: Explicit DELETE WHERE category='contract_value' before upsert, outside of upsert_revenue_line_items
WHY NOT replace=True in upsert_revenue_line_items: replace=True deletes ALL rows for the period including
  cash_collected — would wipe the revenue sync done in the same session.
WHY NOT on_conflict_do_update alone: If a re-sync has fewer invoices than the prior run (e.g., an invoice
  was voided), the old row remains with the stale higher total. Delete-then-insert is the only safe pattern.

FUNCTION: _parse_invoice_totals
APPROACH: Single-pass sum of Total field, skip any Status not in {AUTHORISED, PAID}
WHY NOT use SubTotal: Total = SubTotal + TotalTax. Xero org may have zero tax, but using Total is correct
  regardless. If refunds/credits are needed, they'd be separate ACCREC invoices with negative Total.
```

---

## RESILIENCE CONTRACTS

```
FUNCTION: _fetch_xero_invoices
EXTERNAL DEPENDENCY: Xero Invoices API
TIMEOUT: 30 seconds per page request
RETRY: No retry — Xero tokens are short-lived; a retry on 401 would always fail. CLI operator re-runs.
FAILURE STATE: raises XeroApiError with status code + body excerpt
IDEMPOTENT: Read-only — safe to retry by re-running the full sync

FUNCTION: xero_sync_invoices (route)
EXTERNAL DEPENDENCY: Xero Invoices API + PostgreSQL
TIMEOUT: 30s Xero + standard SQLAlchemy session timeout
RETRY: No automatic retry. DELETE + upsert is atomic within a session. If upsert fails after DELETE,
  the month has 0 contract_value rows — recoverable by re-running /xero/sync-invoices.
FAILURE STATE: HTTP 502 on Xero failure; HTTP 500 on DB failure; HTTP 422 on bad month format
IDEMPOTENT: Yes — DELETE + upsert on same key produces identical DB state on repeated calls
```
