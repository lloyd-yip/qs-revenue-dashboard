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
