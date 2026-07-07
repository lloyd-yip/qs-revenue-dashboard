# QS Revenue Dashboard — Data-Accuracy Audit (2026-07-07)

**Auditor:** Claude Code, working directly against three layers:
1. **GHL v2 API** (live source of truth) — opportunities, pipelines, users, calendars, custom fields, appointment events.
2. **Postgres** (Supabase project `xsunuheuabnoqseafobb`) — the synced `opportunities` + `appointments` tables.
3. **Live dashboard API** (`qs-revenue-dashboard-production.up.railway.app`, open/unauthenticated) — the computed metrics users see.

**Scope of this pass:** Sales Performance tab (complete). Other tabs pending.
**Mandate:** Audit and document only — **no code changes**.

---

## TL;DR — Sales Performance tab is measuring the wrong source

The entire first-call funnel (calls booked → shows → qualification → close-rate denominator) is computed from a **GHL opportunity custom field** (`call1_appointment_date` / `call1_appointment_status`) that is populated by automation + rep data-entry. That field:

- **Chronically captures only ~50–60% of real first-call appointments**, even in good months.
- **Collapsed to ~10–20% in June 2026** because the team **migrated Business-Evaluation calendars** (old "QS Institute: Business Evaluation Call" → new "…Business Evaluation Call (P)"), and the custom-field/automation was tied to the old flow.

The **calendar** (`appointments` table, synced straight from GHL) is the reliable source: **99.8% status coverage** vs the custom field's 52%. The corrected numbers below come from it.

### Corrected vs displayed — 1st-call funnel (per opportunity)

| Month | Booked (corrected) | Booked (dashboard) | Showed (corrected) | Showed (dashboard) |
|-------|-----|-----|-----|-----|
| 2026-01 | 123 | 164 | 77 | — |
| 2026-02 | 75  | 84  | 47 | — |
| 2026-03 | 107 | 120 | 52 | — |
| 2026-04 | 128 | 148 | 71 | 72 |
| 2026-05 | 133 | 140 | 69 | 71 |
| **2026-06** | **133** | **26** | **67** | **14** |
| 2026-07* | 59  | 49  | 18 (35 still upcoming) | — |

*July partial (as of 2026-07-07). Corrected show-rate: Jan 62.6%, Feb 62.7%, Mar 50.5%, Apr 58.2%, May 52.7%, **Jun 58.3%**.

**Business did not slow down in June** — the dashboard's "26 first calls / 1 close" for June is a data artifact, not reality (real: ~133 first calls, ~67 shows, 13 wins).

---

## Findings (Sales Performance)

### 🔴 F1 — First-call funnel is built on an unreliable custom field (CRITICAL)
- Source: `db/queries/common.py` `has_1st_call()` / `date_filter()` key off `Opportunity.call1_appointment_date`; `bookable_1st_call_expr()` requires `call1_appointment_status ∈ (Showed, No Show, Cancelled)`.
- These columns come from GHL opportunity custom fields (`We5c2Oiz8kC3FgjOO2XD`/`bFDWu3koncdxn26h6nAm` for date, `V82ErbW24izA5aQUzRUv` for status) — sync mapping in `sync/ghl_client.py::extract_custom_fields`.
- Evidence: of 258 opps with a real June 1st-call on the calendar, **116 (45%) have NULL custom-field date, only 26 (10%) dated in June**; ~48% of all historical appointments have NULL status.
- **Impact:** every Sales KPI on the appointment-date basis (booked, shows, show rate, qual rate, DQ rate, close-rate denominator) understates volume 40–90%; rates are biased (missing appointments are non-random).
- **Recommended fix (not yet applied):** re-base the funnel on the `appointments` table (calendar), using the per-opportunity positional model below.

### 🔴 F2 — June calendar migration broke the field entirely
- Old "QS Institute: Business Evaluation Call" (`m3e2od…`): 810 lifetime, **3 in June**. New "…Business Evaluation Call (P)" (`pK68Ca…`): **121 in June**. The custom-field automation never followed the migration.

### 🟠 F3 — `call_1` / `call_2` classification is unreliable
- Sync labels an appointment `call_2` only if its calendar is in a hardcoded 19-ID follow-up list (`sync/ghl_client.py FOLLOW_UP_CALENDAR_IDS`); everything else becomes `call_1`.
- Consequence: post-sale delivery calls (Client Commitment Check-in, Strategy Call 3, Tech Calls, Onboarding, Mastermind, Presentation Success) are miscounted as **1st sales calls**, and follow-ups whose calendar isn't in the list (e.g. "Follow Up 60 min: Armando Valencia") leak into `call_1`.
- **Correct key is the calendar name** (approved with Lloyd 2026-07-07). See classification below.

### 🟠 F4 — Closes counted inconsistently between views
- Summary KPI `units_closed` filters by **appointment date** (broken field) → June shows **1**. Per-rep table counts by **close_date** → **13** actually won in June. The two views disagree; the headline inherits the appointment-date breakage.
- `close_date` (`vzU9IqXPuwAYkKrJ3I3F` wonlostabandoned) is set for won **and** lost/abandoned; gating on stage=Deal Won is correct and is done for units, but any close-*date*-based logic must also gate on stage.

### 🟡 F5 — ~229 orphaned opportunities in the DB
- DB `opportunities` = 4,093; GHL Sales (3,593) + Upsell/Client-Delivery (271) = 3,864. Sync upserts but never deletes, so opps merged/moved out of the synced pipelines in GHL linger. Minor inflation of totals.

### 🟡 F6 — `pipeline_stage_name` is NULL for 100% of rows
- The search endpoint returns `pipelineStageId` but not the stage name; sync never resolves it. Harmless today (metrics use `pipeline_stage_id`), but a latent trap for any future name-based logic.

---

## Approved call classification (calendar-name based, per opportunity)

**Model (approved by Lloyd 2026-07-07):** per opportunity, order its *sales* appointments chronologically → earliest = **1st call**; the rest = **follow-up**. Count per opportunity (dedupe reschedules). Track show-rate and reschedule/cancel-rate separately for 1st vs follow-up.

- **1st sales call — webinar funnel** — 27 calendars whose name contains **"Business Evaluation"** (variants: (P), (A), 4W, 6M, Extra Weeks, Intro, Reschedule, Screening, External/Outreach, per-rep).
- **1st sales call — outreach funnel (separate funnel, channel = Slack/WhatsApp/SMS, NOT webinar)** — "QuantumSCALE 15 minutes Demo", "QuantumSCALE 30 minutes Demo" (`8Ah4KDHm3VCW21H8ilPX`, `X7wOBb9Q6pamQq4maYaL`, `pwZRBXSpz4VwLysFwfx2`). *Ruled by Lloyd 2026-07-07.*
- **1st sales call — Referral funnel (channel source = "Referral")** — "quantumSCALE Institute: Referral Call", "Referral Call: 15 Min" (`Bl7DsqhsmxwIHBrQ9fpY`, `NAnH3Nrtx8GFLaPmmiHZ`). *Ruled by Lloyd 2026-07-07.*
- **Follow-up sales call** — "Follow Up" + "3rd/4th/5th Meeting with Armando/Ryan" + "Enrollment Call into QuantumScaling" + **"Custom Demo with Armando/Ryan"** (`m03njlJcj3uyNj1P4PoV`, `PPH5rjaZdiCMrcMFpU0V`) (positional: follow-up if a call precedes it). *Custom Demo ruled follow-up by Lloyd 2026-07-07.*
- **NOT a sales call (post-sale delivery, exclude):** Tech Call, Strategy Call, Coaching, Onboarding, Check-In, Client Commitment, Mastermind, Presentation Success, Personal calendars, Interviews, Scaling Map, Webinar Slides Review.

**Note:** the outreach-demo and referral calendars barely link to Sales-pipeline opportunities in current data (outreach ≈ 1–2 first-calls/month, referral ≈ 0; the QuantumSCALE Demo calendars hold ~30 appointments total but almost none map to a Sales opp via `ghl_contact_id`). Either these leads rarely become Sales-pipeline opps or there is a contact→opp linkage gap — flag for a separate look. The three funnels should be reported separately (webinar / outreach / referral) per Lloyd's tracking model, each with its own show-rate and reschedule/cancel-rate for 1st vs follow-up calls.

Calendar-ID lists for each bucket are stored with the audit working files.

---

## Method notes / reproducibility
- GHL Sales pipeline id `zbI8YxmB9qhk1h4cInnq` (3,593 opps); Upsell/Client-Delivery `NjidsHukHHUpYtTcQefX` (271). Sync ingests only these two of 19 pipelines.
- Corrected numbers use `appointments` JOIN `opportunities` on `ghl_contact_id`, filtered to the 1st-call calendar-ID set, earliest-per-opportunity via `ROW_NUMBER()`.
- Calendar `appointment_status` values are capitalized: `Confirmed / Showed / Cancelled / No Show` (99.8% populated, 14 NULL of 6,764).

---

---

## Findings (Lead Quality by Channel)

### 🟠 F7 — `canonical_channel` normalization gap dumps ~750 attributable opps into "Unknown"
- 33.9% of opps (1,283) are "Unknown". Of those, **438 have a booking source and 260 a UTM source** — real attribution that isn't mapped.
- Top unmapped raw source values: `email` (308), `call` (121), `calendar` (51), `slack calendly` (41), `li` (19), `fl` (17), `sms calendly` (11), `ai-caller` (11), `fb` (6), `wa-bot` (4), `ig` (2)…
- **Fix (config, not code):** add these to the `source_normalization` table (email→Email, call→Phone/Setter, li→LinkedIn, fb/ig→Meta, sms/wa→SMS/WhatsApp, ai-caller/wa-bot→AI Bot). Recovers ~12% of all opps from Unknown.

### 🟢 F-note — `lead_quality` itself is healthy
- Among real (calendar) 1st-call shows, reps fill `lead_quality` 72–88% of the time (Jun 82%). The distribution (Great/Ok/Barely/Bad/DQ) is trustworthy; only its **absolute counts inherit the F1 undercount** (Jun shows 14 of 67 real). Proportions ~ok, volumes ~5× low.

---

## Findings (Pipeline Intelligence)

### 🟠 F8 — Live stage snapshot is stale/inflated vs GHL
- DB over-counts every current stage: Upcoming 1st Booked **145 vs GHL 103 (+41%)**, 1st Call Done 318 vs 307, Deal Won 309 vs 299, Disqualified 227 vs 220, No-Show 392 vs 392→405.
- Root: upsert-only sync + missed incremental stage-transitions (esp. the mid-June stuck-sync period) + ~229 orphans (F5). Opps that left "Upcoming" in GHL are frozen there in the DB.
- **Fix:** run a full re-sync and add delete/absence reconciliation; the snapshot then matches GHL.
- Segment-by dimensions (business_fit, pain_goal_oriented, business_industry, current_revenue) are contact custom fields joined onto opps — populated only when reps fill them; sparse but not "wrong". (Quantify if this tab is used heavily.)

---

## Findings (Compliance)

### 🟠 F9 — Compliance detection is blind to opps with no appointment date
- `outcome_unfilled` / `rep_compliance_failure` are computed from `call1_appointment_date` + `call1_appointment_status` (the F1 broken field). If the date is NULL, the flags can never fire.
- Evidence: of 209 June-created opps, **88 (42%) have NULL `call1_appointment_date`** → invisible to compliance. June flags: outcome_unfilled 15, rep_compliance_failure 4 — almost certainly under-counted.
- **Fix:** re-base compliance on the calendar (`appointments`): a call whose calendar `appointment_status` stays `Confirmed` past its date = outcome-unfilled. 99.8% status coverage makes this reliable.

---

## Findings (Funnel Economics)

### 🟡 F10 — RORI inputs manual (known) + cost-per-X inflated by volume undercount
- Marketing spend + rep comp are manual entries (by design, per decision log). RORI columns blank until entered — not a bug.
- BUT cost-per-show / cost-per-close / cost-per-qual use the **undercounted** shows/closes (F1/F4) as denominators, so any populated cost-efficiency figure is **overstated** (e.g. cost-per-show ~5× too high for June). Fixing F1 fixes these automatically.
- `contract_value` / `cash_collected` come from `deal_whop_matches` (separate Whop reconciliation) — audit separately if that tab is in scope.

---

## Consolidated fix priority (when we move from audit → fix)
1. **F1/F2 (critical):** re-base the first/follow-up call funnel on the `appointments` calendar using the approved per-opportunity positional model. Fixes Sales, and the volume side of Lead-Quality, Compliance, Funnel-Economics at once.
2. **F8/F5:** full re-sync + orphan/stale reconciliation so the live pipeline snapshot matches GHL.
3. **F7:** extend `source_normalization` with the ~15 unmapped source values.
4. **F4:** make summary `units_closed` use close_date (consistent with per-rep view).
5. **F3:** replace the hardcoded 19-ID follow-up list with name-based sales-call classification.
6. **F9:** re-base compliance flags on calendar status.
7. **F6:** populate `pipeline_stage_name` (low priority; cosmetic).

## Open decisions for Lloyd
- ✅ All call-classification decisions resolved (2026-07-07). Classification above is final.
- Remaining: separately audit the **outreach-demo / referral → Sales-opp linkage gap** (why QuantumSCALE Demo appointments rarely attach to a Sales-pipeline opportunity).
