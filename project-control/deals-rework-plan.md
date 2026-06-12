# Deals Rework — Live Deals Relocation + Rep Attribution Fix

**Created:** 2026-06-11 · **Scope:** FOCUSED (top-nav consolidation deferred) · **Status:** Step 1 (diagnosis) in progress

## Why this exists
Lloyd doesn't want the "Live Whop Revenue" view buried in the P&L page. It belongs on the
**Deals** page next to the historical reconciliation. Separately, the live view shows almost
every deal as **"Unassigned"** even though the closer is knowable — that's a data/attribution
bug to fix, not a feature to build around.

## Decisions locked (2026-06-11)
- **Diagnose attribution BEFORE designing** — likely a field-mapping fix, not a new "assign" UI.
- **Focused scope** — relocate live view + fix attribution. **Top-nav consolidation DEFERRED**
  (3 inconsistent navs exist across pages — real debt, separate job, not this cycle).
- Live view = same per-rep component, just relocated; Deals page gets two lenses:
  **Historical (reconciled)** [current Deal Reconciliation] + **Live (this month, real-time)**.
- Execution will use `/build-loop` + `/test-drive` — but ONLY after `/ux-design` → `/blueprint`
  are done and approved. Do NOT jump to build.

## Key files
- `static/deals.html` — Deal Reconciliation page (destination for the live view)
- `static/pnl.html` — currently hosts "Live Whop Revenue" section (to be REMOVED from here)
- `sync/sync_engine.py:132-136` — attribution source: `opp.assignedTo` → `user_map` → `opportunity_owner_name`
- `sync/ghl_client.py` — GHL user map + opportunity fetch (location_id = G7ZOWCq78JrzUjlLMCxt, won stage = 544b178f…)
- `db/queries/whop_live.py` — `get_whop_live_summary_for_month` (groups by `ghl_owner_name`, NULL→"Unassigned")
- `db/queries/deal_matches.py` — `DealWhopMatch.ghl_owner_name` copied from `Opportunity.opportunity_owner_name` at match time
- `api/routers/whop_live.py` — `GET /pnl/whop-live`, `POST /pnl/whop-refresh`

---

## STEP-BY-STEP (granular)

### STEP 0 — Checkpoint (before any code)
- `/git-checkpoint` — commit working state before touching code (build steps only; not needed for planning/diagnosis).

### STEP 1 — DIAGNOSE attribution (read-only, /debug) ← CURRENT
**Goal:** find where the closer actually lives in GHL for the "Unassigned" deals. NO code.
1. Pull April-2026 deals with `ghl_owner_name` NULL from `GET /deals/matches` → collect `ghl_opportunity_id` + `ghl_contact_id` (e.g. Judith DeFeo, Paul Cucinotta, Michael, Mark Angus…).
2. For 4-5 of them, GHL `get_opportunity(id)` → inspect: `assignedTo`, owner, and any "Closer"/rep custom field.
3. Check the contact's appointments → the **Call-2 (decision call) assigned user** (QS closers run Call 2).
4. Confirm whether `user_map` resolution is the gap (assignedTo set but name unresolved) vs assignedTo genuinely empty.
**Output:** evidence-based root cause + the correct field to use. Record findings in this doc (Step 1 Findings below).

### STEP 2 — DECISION POINT (Lloyd approves fix approach)
Present diagnosis. Confirm which GHL field becomes the closer source. Candidate fixes:
- (a) Closer is in Call-2 appointment `assignedUser` → derive owner from appointment, not opportunity.
- (b) Closer is a GHL custom field → map that field in sync.
- (c) `assignedTo` is set but `user_map` missing the ID → fix user resolution.
Get explicit greenlight on the approach before any backend change.

### STEP 3 — /ux-design (Deals page only; nav untouched)
- IA: Deals page = **Historical (Reconciled)** + **Live (This Month)** — decide tabs vs toggle vs stacked.
- ASCII wireframe both lenses + the switch. Confirm with Lloyd.
- Empty/loading/error states for the Live lens (June currently empty).

### STEP 4 — /blueprint (focused)
- **Backend (attribution):** the field-mapping fix from Step 2 + backfill of `Opportunity.opportunity_owner_name` (and propagate to `DealWhopMatch.ghl_owner_name`). Decide: migration? backfill script? re-run match vs targeted UPDATE.
- **Frontend (IA move):** relocate live component `pnl.html` → `deals.html`; add Historical/Live switch; REMOVE live section from `pnl.html`.
- Blueprint critique + expert debate per skill. Approve before build.

### STEP 5 — Approve blueprint (Lloyd greenlight)

### STEP 6 — /build-loop (build → /build-verify → /test-drive → fix)
- git-checkpoint → build → static conformance → runtime QA → targeted fixes → loop to green.
- deployment-check before push. Subtree push (FETCH FIRST — Geri shares this repo, never force).

### STEP 7 — Verify live + update state
- Curl + visual proof. Update `project_state.json`. Knowledge-base any new attribution fact.

---

## DEFERRED (not this cycle)
- **Top-nav consolidation.** 3 different navs across pages (`Data Quality|P&L`; `Sales|P&L|Deals|SLWA|Sync`;
  `QS|Dashboard|P&L|Expenses|Deals|Data Quality`). Unify into one nav component. Separate /ux-design + build.

## Open questions (resolve as we go)
- Historical vs Live on Deals page: tabs, toggle, or stacked sections? (→ Step 3)
- Backfill strategy for attribution once the correct field is known? (→ Step 4)

## STEP 1 FINDINGS (2026-06-11) — ROOT CAUSE CONFIRMED

**It is NOT a missing-data or wrong-field problem. It's a user-ID→name resolution gap.**

Evidence (live GHL, 3 "Unassigned" April deals):
- Every opportunity HAS `assignedTo` populated:
  - Judith DeFeo → `qzlkNXmWljggweGFjFaY` (Ryan)
  - Michael Niemann → `qzlkNXmWljggweGFjFaY` (Ryan — same closer, multiple deals)
  - Morike Talabi → `GuG4LdvGwduEQQCntOnK`
- Judith's sales appointments (Call 1 + "2nd Meeting") BOTH carry `assignedUserId: qzlkNXmWljggweGFjFaY` — matches the opportunity. Closer is consistent and knowable. (Onboarding call is a different user = post-sale, correctly not the closer.)
- `sync_engine.py:132-136` reads the RIGHT field (`opp.assignedTo` → `owner_id`) and stores `opportunity_owner_id`, but `owner_name = user_map.get(owner_id)` returns None → name NULL → "Unassigned".
- `ghl_client.get_users()` builds the whole map from a SINGLE, UNPAGINATED `/users/` call. Some closers resolve (e.g. Melissa Fredericks), most don't → **map is incomplete; active closer IDs (Ryan, etc.) are absent from it.**

**Root cause:** `get_users()` returns an incomplete user map (no pagination / response cap), so opportunities assigned to the missing closers get a NULL owner name. `owner_id` is already stored correctly.

**Fix shape (NO schema change, NO new UI, NO manual assignment):**
1. Make `get_users()` return ALL location users (paginate the `/users/` call; log the count + confirm `qzlkNXmWljggweGFjFaY` now resolves). Confirm whether cause is pagination vs response cap during the fix.
2. Backfill existing rows from data already in the DB: `Opportunity.opportunity_owner_name` ← complete_map[`opportunity_owner_id`] where name IS NULL and owner_id IS NOT NULL.
3. Propagate to `DealWhopMatch.ghl_owner_name` by joining on `ghl_opportunity_id` (DealWhopMatch has no owner_id column).
4. Going forward, sync resolves correctly on its own.

**Open sub-question for the fix:** confirm `Opportunity.opportunity_owner_id` is in fact populated for these NULL-name rows (expected yes — assignedTo was a string on all sampled deals). If yes, backfill is purely internal (no GHL re-fetch beyond the user map).

**Backfill scope decision (for Step 2):** targeted UPDATE (fast, internal) vs full re-sync (slow). Targeted UPDATE recommended.

## STEP 2 — FIX SPEC (APPROVED 2026-06-11, no /blueprint — bug fix)

Two plausible mechanisms for the unresolved IDs (can't pre-verify: no local GHL key):
- **(B, most likely)** the user IS in `/users/` but its `name` field is empty → `u.get("name","")` returns "" → grouped as "Unassigned". (GHL users often have firstName/lastName but blank `name`.)
- **(A, less likely)** the user isn't in the `/users/` response at all (pagination/scope). GHL `/users/?locationId=` is believed to return all location users unpaginated, so A is unlikely.

1. **Fix:** `get_users()` resolves display name as `name` → `firstName+lastName` → `email` (handles B), and logs the resolved user count (observability; reveals A if count is suspiciously low). Single safe change to `sync/ghl_client.py:121`. No pagination added (unverified param names = risk for an unlikely cause); add only if behavioral verification shows A.
2. **Backfill (reuse existing jobs — no new loop):** after deploy, trigger a **full sync** (re-resolves `opportunity_owner_name` for all opps via the fixed `get_users`) → then **Run Match** (copies names onto `DealWhopMatch.ghl_owner_name`). Fallback if full sync is too heavy/timeout-prone: a set-based `UPDATE opportunities SET owner_name = map[owner_id] WHERE owner_name IS NULL AND owner_id IS NOT NULL` (bulk, idempotent — NOT a row loop) + join-update to deal_whop_matches.
3. **Verify:** `GET /api/dashboard/deals/matches` → Judith DeFeo & Michael now show owner "Ryan" (was Unassigned); `GET /pnl/whop-live?month=2026-04` → Unassigned bucket shrinks, real rep buckets appear; sanity-check total distinct reps is plausible (not inflated/duplicated).
4. **Rollback:** fix is additive + idempotent; re-runnable. To revert, restore prior `get_users` and re-sync. No schema change, no migration.

---

## RECURRING CASH WORKSTREAM (added 2026-06-11, from mockup feedback)

### Confirmed business model (Lloyd, 2026-06-11)
- NO true monthly subscriptions. All "recurring" = internal payment plans, usually 2-3 months.
- Splitit & ClarityPay: realized as PAY-IN-FULL upfront (QS receives 100% upfront minus the 15% fee = the discount). NEVER recurring.
- The ONLY recurring source = internally-financed plan installments landing in months AFTER the close month.

### Key insight (why this matters)
The "numbers aren't clear" complaint AND the recurring requirement are the SAME root problem:
attributing each payment to the month it actually landed. Today a deal shows in its CLOSE month
with its ALL-TIME cash number — fine for upfront deals, WRONG for multi-month internal plans
(close-month view shows cash that trickles in over 2-3 months). Cannot fix clarity without
monthly attribution; monthly attribution IS the recurring feature. One job, not two.

### The model the Live view needs
- Net-new cash (this month)        = cash from deals that CLOSED this month.
- Recurring cash (this month)      = installments landing this month from PRIOR-month internal-plan deals.
- Total cash collected (this month)= net-new + recurring.
- Contract value realized (month)  = contract value of deals closed this month.
- Live view layout: Net-New section (per-rep) + a SEPARATE Recurring cash display + a clear
  totals strip: Net-New Cash / Recurring Cash / Total Cash / Contract Realized.

### SEQUENCING — DO IN THIS ORDER (backend-first, because the UI depends on uncertain data)
1. VERIFY DATA (read-only, ~10 min): pull real Whop payment records for known internal-plan deals
   that are several months into their plan; confirm per-installment paid-dates EXIST, are
   bucketable by month, and attributable to a rep. NOTE from earlier probe: Whop does NOT
   pre-create future installment records — only COLLECTED installments appear as payment rows,
   each with its own paid date. Need to confirm multiple collected installments show distinct
   monthly dates. This greenlights the clean path OR forces plan B.
2. BLUEPRINT BACKEND: monthly-cash data model + new-vs-recurring logic + endpoint shape.
   (We currently store only all-time total_paid per deal — this adds per-payment-month tracking.)
3. FINALIZE FRONTEND MOCKUP against the now-known data shape (net-new + recurring sections +
   unambiguous Cash/Contract labels). Re-confirm with Lloyd.
4. BUILD AS ONE: Deals-page relocation + rep fallback + historical month-nav all ride along,
   so the Live view is designed once, correctly.

### Failure mode to watch (why step 1 comes first)
If Whop does NOT expose clean per-payment dates -> recurring split is much harder -> pivot to
inferring installments from the plan schedule (split_pay_required_payments + per-installment
amount + cadence). Find this out in 10 minutes BEFORE blueprinting, not after.

### Other mockup feedback folded into the BUILD (step 4)
- CLARITY: each deal row explicitly labels "Cash collected $X" and "Contract $Y"; totals strip
  spells out Net-New / Recurring / Total / Contract Realized (no bare ambiguous numbers).
- REP FALLBACK: for deals with no opportunity owner, use the Call-2 appointment's assignedUserId
  (CONFIRMED present in appointment data earlier) -> recovers Dwight Crain + most residual Unassigned.
- HISTORICAL MONTH-NAV: the Historical/reconciliation lens needs its OWN month navigation
  (currently cannot flip months — Lloyd flagged).
- LOCKED 2026-06-11: default lens = Live; reuse Historical table rep-filter for cross-month rep lookup.
- Mockup approved in shape (3 lenses/states rendered) pending the recurring additions above.

### RECURRING — STEP 1 RESULT (2026-06-11): GREEN LIGHT ✅
Verified against live Whop payments for 3 internal-plan deals:
- Sara Downey/Jason Bern (mem_ypwZeyThuSjgJ4): 3×$5,000 paid 2026-03-20, 04-19, 05-19 (3 months)
- Diane Primo/Ryan (mem_hcMP2G9lsj6hBP): 2×$8,997 paid 2026-05-05, 06-04 (June = recurring)
- ZeroMils/Melissa (mem_Jrn8npTVQUsLed): 2×$5,000 paid 2026-04-01, 05-01
CONFIRMED: each collected installment is a SEPARATE payment row with its own paid_at/created_at
date; installments span different months; proc=multi_psp; tied to membership->deal->rep. We CAN
bucket cash by collection-month and split net-new (deal closed this month) vs recurring (closed prior).
No plan B needed. Proceeding to Step 2 (blueprint backend).

### RECURRING — STEP 2 ARCHITECTURE DIRECTION (for the blueprint)
- NEW table (per-payment ledger): one row per Whop payment — membership_id, ghl_opportunity_id,
  paid_date, gross_amount, fee_pct, net_amount, processor. Idempotency key = whop payment id.
- POPULATION: extend the existing refresh/match path to upsert per-payment rows (we already fetch
  _fetch_membership_payments; today we only aggregate). Backfill historical.
- QUERY (monthly): for month M -> sum net by paid_date in M, split by deal.first_payment_date==M
  (net-new) vs <M (recurring); group by rep. Contract realized = contract value of deals closed in M.
- ENDPOINT: extend GET /pnl/whop-live response: { net_new:{per_rep,totals}, recurring:{per_rep or list,total}, totals:{net_new_cash, recurring_cash, total_cash, contract_realized} }.
- Rep fallback (Call-2 appointment assignedUserId) folds into attribution.
