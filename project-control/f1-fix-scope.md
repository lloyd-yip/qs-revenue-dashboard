# F1 Fix — Scope: Re-base the sales funnel on the calendar (appointments), not the custom field

> **STATUS 2026-07-07 — F1-core CODE IMPLEMENTED (branch `feature/slwa-dashboards-v3`), not yet deployed/backfilled.**
> Changed: `sync/ghl_client.py` (name-based `classify_calendar` / `funnel_of_calendar` + `get_calendars()`), `sync/sync_engine.py` (`_derive_calls_from_appointments` + calendar-derived call1/call2 in `_build_opportunity_row`, name-based `appointment_type`; removed dead helpers), `db/models.py` (docstring). 15 new tests in `tests/test_critical_paths.py` — **53 passed**. Classifier validated vs all 275 live calendars.
> **Remaining (production actions — need go-ahead):** 1) deploy to Railway, 2) run one full sync to backfill, 3) live-API before/after check. Order matters: deploy *before* backfill, else the old deployed sync overwrites call1 from the custom field on its next run.


**Problem (from audit F1/F2):** `Opportunity.call1_appointment_date` / `call1_appointment_status` come from GHL opportunity **custom fields** that only capture ~50–60% of real first-calls and collapsed to ~10% in June after the calendar restructure. The reliable data (the `appointments` table, synced from the GHL calendar, 99.8% status coverage) is already in the DB but unused for call-1.

**Goal:** make the five columns every metric depends on — `call1_appointment_date`, `call1_appointment_status`, `call1_booking_date`, `call2_appointment_date`, `call2_appointment_status` — derive from the calendar appointments using the approved per-opportunity positional model, so **all tabs become correct with zero changes to the query layer.**

---

## Why this is the right shape (minimal blast radius)

Every metric query (`db/queries/common.py`, `metrics_summary.py`, `metrics_by_rep.py`, `time_series.py`) reads those five `Opportunity` columns and nothing else appointment-related. If we fix how those columns are *populated*, we fix Sales, Lead-Quality volumes, Compliance detection, and Funnel-Economics cost-per-X **without touching any query, schema, or endpoint.**

The sync **already fetches** each contact's appointments — `sync/sync_engine.py:172` `all_appointments = await ghl_client.get_contact_appointments(contact_id)`. Today it ignores them for call-1 and reads the custom field instead (`sync_engine.py:141,158`). We redirect that derivation. **No new API calls, no added sync cost.**

---

## Changes

### 1. Calendar classifier (new) — `sync/ghl_client.py`
Replace the single hardcoded `FOLLOW_UP_CALENDAR_IDS` set with a **name-pattern classifier** that is robust to new per-rep calendars (they add "…Business Evaluation Call - <Rep>" calendars regularly, so an ID list rots).

- Add `GHLClient.get_calendars()` → `{calendar_id: name}` (endpoint `GET /calendars/?locationId=`), fetched **once per sync run** and passed down.
- Add `classify_calendar(name) -> 'first' | 'followup' | 'exclude'` using the rules Lloyd approved (2026-07-07):
  - **first** — name contains `business evaluation` / `business growth evaluation`; or `quantumscale … demo` (outreach funnel); or `referral call` (Referral channel).
  - **followup** — name contains `follow up` / `follow-up`; or `(2nd|3rd|4th|5th) meeting`; or `enrollment call into quantumscaling`; or `custom demo`.
  - **exclude** — everything else (Tech/Strategy/Coaching/Onboarding/Check-In/Client Commitment/Mastermind/Presentation Success/Personal/Interview/Scaling Map/Webinar Slides).
- Keep a small `funnel_of_calendar(name)` → `webinar | outreach | referral` for phase 2 reporting (see below); store it but no metric depends on it yet.

### 2. Derive call1/call2 from appointments — `sync/sync_engine.py::_build_opportunity_row` (lines ~139–178)
Replace the custom-field call1 derivation with, over the contact's `all_appointments` (drop `deleted`):
- Partition into `first_appts` and `followup_appts` via the classifier.
- **call1** = **earliest** `first_appts` by `startTime`:
  - `call1_appointment_date` = its `startTime`
  - `call1_booking_date` = its `createdAt` (already handled by `_appointment_booking_date`) → fixes `date_by=booked` for real
  - `call1_appointment_status` = **outcome-aware** (see decision D1)
- **call2** = **earliest `followup_appts` after call1** (falls back to earliest followup) — same as today but via the classifier, not the ID list. This also fixes F3 (follow-ups on non-listed calendars, and delivery calls no longer leak into call-1).
- Delete reliance on `custom.get("call1_appointment_date"|"call1_appointment_status")`. Leave the custom-field extraction in place (harmless), just stop feeding call-1 from it.

`_normalize_appt_status` already emits `Showed / No Show / Cancelled / Confirmed`, exactly what the query layer compares against — no query change needed.

### 3. Backfill existing rows
Re-derive call1/call2 for all historical opps. Two options:
- **A (recommended): full sync.** The full sync path already fetches appointments per contact and includes `createdAt` (booking date). Change the code, run one full sync → every opp recomputed from GHL. Clean, no migration.
- **B (fast, no GHL): SQL/one-shot backfill** from the existing `appointments` table. Cheaper but the appointments table lacks the GHL booking timestamp, so `call1_booking_date` can't be recovered this way (would need a schema add). Use only if a full sync is too slow.

### 4. (Optional, low priority) drop stale custom-field dependence in docs/comments
Update model/comment references so future readers know call1 is calendar-derived.

---

## Decisions — RESOLVED (Lloyd, 2026-07-07)

**D1 — call1 status under reschedules → OUTCOME-AWARE.** `call1_appointment_status` = `Showed` if **any** 1st-call appt showed; else `No Show` if any no-showed; else `Cancelled`; else `Confirmed` (still upcoming). `call1_appointment_date` = date of the **showed** appt if one exists, otherwise the earliest 1st-call appt. Also emit a per-opp `first_call_attempts` count (appts on 1st-call calendars) to power the separately-tracked reschedule/cancel rate later.

**D2 — funnel split → PHASE 2.** Ship F1-core (aggregate volumes correct) first. Webinar / outreach-demo / referral split + Referral channel is a follow-on reporting dimension (`first_call_funnel` column + channel mapping + UI).

**D3 — multi-opp contacts → SIMPLE.** Every opp on a contact inherits that contact's earliest/outcome-aware 1st-call appt (same as today's call2 behavior). Revisit only if reactivations misattribute.

---

## Out of scope for F1 (tracked separately in the audit)
- F5/F8 orphan + stale-snapshot reconciliation (full re-sync + delete handling).
- F7 `source_normalization` additions (email/call/li/fb/… → channels).
- F4 summary `units_closed` → close_date.
- F9 compliance re-base (largely fixed *for free* once call1 date/status are reliable, since `outcome_unfilled` reads them).

---

## Verification plan
1. **Unit:** feed `_build_opportunity_row` a synthetic contact with (a) single BE showed, (b) BE cancelled→BE showed reschedule, (c) BE + follow-up, (d) delivery-only calendar → assert call1/call2 date+status.
2. **Backfill parity:** after backfill, re-run the audit query (per-opportunity earliest BE appt) and confirm `Opportunity.call1_appointment_date` monthly counts match the calendar truth (Jun ≈133, not 26).
3. **Live API diff:** hit `/api/dashboard/summary?start=2026-06-01&end=2026-06-30&date_by=appointment` before/after — `calls_booked_1st` should jump from 26 → ~133, `shows_1st` 14 → ~67, show_rate stays ~0.58.
4. **No regression on closes/rep attribution:** `units_closed`, per-rep names unchanged.
5. **Spot-check** 5 opps against GHL calendar UI.

## Rollback
Pure sync-logic change. Revert `_build_opportunity_row` + classifier and run one full sync to repopulate call1 from custom fields. No schema/data-shape change (unless D-backfill option B adds a column).

## Effort estimate
- Classifier + `get_calendars()`: ~½ day.
- `_build_opportunity_row` rewrite of call1/call2 block: ~½ day.
- Backfill (full sync) + verification: ~½ day.
- **F1-core total: ~1.5 days.** Phase 2 funnel split: +1–2 days.
