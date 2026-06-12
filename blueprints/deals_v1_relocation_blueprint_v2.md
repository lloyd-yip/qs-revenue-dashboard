# Blueprint v1: Deals Page — Live-lens relocation + month picker + rep fallback + labels
# Project: qs-dashboard · PHASED v1 (uses EXISTING data; net-new/recurring split is v2)
# Visual contract: prototypes/deals-redesign.html (Lloyd-approved 2026-06-12) + project-control/specs/ux-design.md
# Status: awaiting Lloyd's ONE approval -> then /build-loop drives build->verify->test-drive->fix

## INTENT
outcome_trigger: Live Whop view is buried on P&L, its month picker can't reach the current month (May/June unreachable), deals show "Unassigned", and per-deal numbers are ambiguous.
success_state: The Deals page hosts the Live view (default) with a deal-driven month picker reaching the current month, owner-less deals are recovered via the call rep, every number is labeled, and P&L no longer carries the Live section.

observable_done_state (= build-loop acceptance contract; each falsifiable):
1. GET /api/dashboard/pnl/whop-live/months returns 200 with a JSON array of "YYYY-MM" strings that have deals, INCLUDING the current month and 2026-05/2026-06.
2. The Deals page (/deals or /static/deals.html) renders a "Live · This Month" / "Historical · Reconciled" toggle; Live is the default lens; Live shows per-rep collapsible buckets + the totals strip for the selected month.
3. The Live-lens month picker defaults to the current month and can navigate to 2026-05 and 2026-06 (i.e. it is NOT the P&L revenue-period list).
4. Each Live deal row shows the amount tagged "cash collected" AND the meta shows "Contract $X" (explicit labels).
5. P&L page (/pnl) no longer renders #whop-live-section and does not call loadWhopLive.
6. Rep fallback: a known owner-less deal (e.g. Dwight Crain) shows a rep name after backfill, OR is confirmed to have no Call-2 appointment in GHL (genuinely apptless). Unassigned count drops.

depends_on: []

## SCOPE_FENCE
explicitly_excluded:
  - Historical-lens month navigation -> DEFERRED to main UX redesign (Lloyd 2026-06-12).
  - Net-new vs recurring monthly split / per-payment ledger -> v2 (blueprints/monthly_cash_ledger_blueprint_v2.md).
  - Top-nav consolidation -> deferred.
disallowed:
  - DB queries in route handlers.
  - Any change to the net-cash math or the matching engine (v1 reuses existing get_whop_live_summary_for_month).
  - Touching the top navigation.

## VISUAL_CONTRACT
Source: prototypes/deals-redesign.html (approved). Reuse EXISTING dark tokens (--bg #0f1117, --surface #1a1d27, --accent #6366f1, --green, --yellow, --text3) and the existing renderBucket collapsible. Live default; segmented toggle; month nav = arrows + dropdown; per-row right number labeled "cash collected"; meta "Contract $X · date". No new design system.

## FILES + SKELETON

### FILE 1: db/queries/whop_live.py (MOD, additive)
- async def get_available_deal_months(session) -> list[str]
  # distinct to_char(first_payment_date,'YYYY-MM') from deal_whop_matches where confidence in (high,medium), desc; ALWAYS include current month even if empty.

### FILE 2: api/routers/whop_live.py (MOD, additive)
- @router.get("/pnl/whop-live/months") async def whop_live_months(db) -> list[str]
  # returns get_available_deal_months(db); current month guaranteed present.

### FILE 3: sync/sync_engine.py (MOD) — rep fallback at owner resolution (lines ~132-136)
CURRENT: owner_id = opp.assignedTo if str else None; owner_name = user_map.get(owner_id)
REPLACEMENT: if owner_id is None, fall back to the Call-2 (FOLLOW_UP_CALENDAR_IDS) appointment's assignedUserId for this contact, then resolve via user_map.
- helper: def _resolve_owner(opp, followup_appt, user_map) -> tuple[str|None, str|None]
  # purpose: return (owner_id, owner_name) preferring opp.assignedTo, falling back to followup_appt.assignedUserId.
CALL SITES: the one owner-resolution block in sync_engine (verify followup_appt is in scope there; it is used to derive call2 status/date).

### FILE 4: db/queries/attribution_backfill.py (MOD) + sync/attribution_backfill.py (MOD)
- async def backfill_owner_from_appointments() -> dict  (sync/attribution_backfill.py)
  # for each won deal with NULL opportunity_owner_name AND NULL/zero assignedTo: GHL get_contact_appointments -> pick Call-2 appt (FOLLOW_UP_CALENDAR_IDS) -> assignedUserId -> resolve via complete user_map -> UPDATE opportunity_owner_id/name -> propagate to DealWhopMatch. Per-row try/except. Returns {checked, recovered, still_unassigned, errors}.
- reuse propagate_owner_names_to_deal_matches.

### FILE 5: api/routers/sync.py (MOD, additive)
- @router.post("/backfill-appointment-owners") -> backfill_owner_from_appointments()  (bearer-protected)

### FILE 6: static/deals.html (MOD) — add Live lens + toggle (Historical stays as-is)
- Segmented toggle (Live default / Historical). Live section: month nav (arrows + <select> from /pnl/whop-live/months, default current) + totals strip + per-rep renderBucket (relocated from pnl.html: loadWhopLive/renderWhopLiveSection/renderRepBucket + their CSS). Per-row labels per VISUAL_CONTRACT. Historical = existing reconciliation content, shown when toggled.
- loadWhopLive(month) calls existing GET /pnl/whop-live?month=YYYY-MM (UNCHANGED endpoint).

### FILE 7: static/pnl.html (MOD) — REMOVE Live Whop
- Delete #whop-live-section div, the loadWhopLive call in loadPnL, and the Live Whop CSS/JS blocks. P&L keeps revenue/expenses only.

## VERIFICATION_CONTRACTS
- get_available_deal_months: given DB has deals in 2026-04/05/06 -> returns those + current month; given a month with zero deals (current) -> current month still present.
- whop_live_months endpoint: 200, array of YYYY-MM, current month included.
- _resolve_owner: given opp.assignedTo set -> returns that; given opp.assignedTo None + followup_appt.assignedUserId set -> returns the appt user; given both None -> (None, None).
- backfill_owner_from_appointments: given an owner-less deal whose contact has a Call-2 appt -> opportunity_owner_name set + DealWhopMatch updated; given no Call-2 appt -> still_unassigned++ (no crash).
- deals.html: Live default renders; toggling shows Historical reconciliation table; month picker options come from /months.
- pnl.html: no #whop-live-section in DOM; no loadWhopLive ReferenceError in console.

## SYSTEM_CONTRACTS
preconditions: get_users() name-resolution fix already deployed (done); existing GET /pnl/whop-live live.
postconditions success: deals.html serves Live+Historical; pnl.html Live removed; opportunities/deal_whop_matches owner backfilled for appt-recoverable deals. failure: per-row isolation in backfill (no partial-abort).
ownership_boundaries: reads deal_whop_matches, opportunities, GHL appointments; writes opportunity_owner_id/name + DealWhopMatch.ghl_owner_name; must_never_touch revenue_line_items, the net-cash math.
observability: backfill_owner_from_appointments logs {function, status, recovered, errors}.

## ROLLBACK
- Frontend: revert deals.html + pnl.html (instant).
- Backend: months endpoint + query are additive (revert files). Rep fallback in sync is additive logic (revert sync_engine block). Backfill is idempotent (only fills NULLs). No migration, no schema change.

## /build-loop ACCEPTANCE
Loop until observable_done_state 1-6 all pass via /build-verify (static: files match skeleton, grep invariants) + /test-drive (runtime on deployed Railway: Deals page Live default + month picker reaches June + labels present + P&L Live gone). Circuit-breaker: pull Lloyd on contract-defect, oscillation, or cost ceiling.
