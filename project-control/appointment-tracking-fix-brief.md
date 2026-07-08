# Brief: Fixing GHL first/second call ("appointment booked") tracking

Portable write-up of a data-accuracy fix for a GoHighLevel-sourced sales dashboard.
Paste into the other app's AI. Anything in `CONFIG` is specific to our GHL location
("1. Sales Pipeline") — if the other app uses the same GHL sub-account the IDs are
reusable as-is; otherwise adapt the *pattern*, not the literal IDs.

---

## 1. The problem

The sales funnel (1st calls booked, shows, show rate, qualification, close-rate
denominator) was being computed from **GHL opportunity custom fields** that store the
call's appointment date and status (rep/automation-maintained). Those custom fields are
unreliable:

- They only ever captured ~50–60% of the real first-call appointments.
- They **collapsed to ~10%** the month the team restructured their booking calendars
  (a new "Business Evaluation" calendar replaced the old one; the automation that copied
  the appointment onto the custom field was tied to the old flow).

Net effect: the dashboard showed, e.g., **26 first-calls** for a month when the real
number was **~133** — an ~80% undercount. Every downstream metric (show rate, qual rate,
close rate denominator, compliance) inherited the error.

## 2. Root cause

The custom field is a *copy* of the truth, produced by a fragile automation + manual rep
entry. **The source of truth is the calendar itself** — the appointments booked on the
contact. The dashboard should read the calendar, not the copied custom field.

## 3. The fix (core idea)

**Derive call1/call2 date + status from the contact's actual GHL calendar appointments,
not from the opportunity custom fields.**

For each opportunity, fetch the contact's appointments:

```
GET /contacts/{contactId}/appointments      (LeadConnector v2)
```

Each appointment has: `id`, `calendarId`, `startTime`, `appointmentStatus`
(`"Showed" | "No Show" | "Cancelled" | "Confirmed"`), `createdAt` (when it was booked),
and a `deleted` flag. In our data the calendar `appointmentStatus` is ~99.8% populated —
vastly more reliable than the custom field (~52%).

## 4. Classify calendars by NAME (not a hardcoded ID list)

The account has 275+ calendars and adds new per-rep ones constantly, so classify by the
calendar's **name** (case-insensitive), which is robust to new calendars:

- **1st sales call** — name contains `business evaluation` (or `business growth
  evaluation`); OR `quantumscale … demo` (a separate outreach funnel); OR `referral call`.
- **Follow-up sales call** — name contains `follow up` / `follow-up`; OR
  `<2nd|3rd|4th|5th> meeting`; OR `enrollment call into quantumscaling`; OR `custom demo`.
- **Not a sales call (exclude)** — everything else: Tech Call, Strategy Call, Coaching,
  Onboarding, Check-In, Client Commitment, Mastermind, Presentation Success, Personal
  calendars, interviews, etc. (These are post-sale delivery and must NOT count as calls.)

Order matters: test the follow-up patterns first, because "custom demo" contains "demo"
which also appears in the 1st-call set.

Optional **funnel tag** for a 1st-call calendar (for reporting): `webinar` (Business
Evaluation), `outreach` (QuantumSCALE Demo), `referral` (Referral Call).

## 5. Per-opportunity derivation (the exact logic)

Per opportunity, over its contact's non-deleted appointments:

```
firsts    = appts whose calendar classifies as 'first',    sorted by startTime
followups = appts whose calendar classifies as 'followup',  sorted by startTime

# ---- 1st call ----
if firsts is non-empty:
    # OUTCOME-AWARE status across ALL first-call attempts (handles reschedules):
    status = 'Showed'    if any first attempt showed
        else 'No Show'   if any no-showed
        else 'Cancelled' if any cancelled
        else 'Confirmed' (still upcoming)

    # date the call "happened/will happen":
    call1_date = the SHOWED attempt's startTime if one showed,
                 else the earliest first attempt's startTime

    # when it was first booked (drives a "Booking Date" view):
    call1_booking_date = earliest first attempt's createdAt
else:
    fall back to the legacy custom field (only when the contact has NO sales appointment)

# ---- 2nd call ----
call2 = earliest followup appointment at/after call1_date (else earliest followup)
call2_date, call2_status = that appointment's startTime, appointmentStatus
```

Counting rule: **per opportunity** (dedupe reschedules — one 1st call per opp, not one
per calendar event).

Once call1/call2 date+status are set from the calendar, any existing metric that reads
those fields (show rate, qual rate, close-rate denominator, compliance flags) becomes
correct automatically — no query changes needed.

## 6. Metric definition to make explicit in the UI (this confused us)

"1st Calls Booked" is ambiguous unless the date dimension is labeled. Support three and
name them clearly:

- **By Appointment date** → count opps whose `call1_date` is on day X = calls *scheduled
  to happen* that day. (This is NOT "calls booked that day".)
- **By Booking date** → count opps whose `call1_booking_date` is on day X = the *booking
  action* happened that day (the call may be scheduled later). ← this is what people
  usually mean by "how many did we book today".
- **By Created date** → opportunity created that day.

They genuinely differ (you can book today for a call next week), so surface a toggle and
label it, or users will misread the number.

## 7. Reschedule nuance (decide deliberately)

With the rule above, an *upcoming* call rescheduled to a later day is attributed to its
*earliest* attempt date (not the new date). If your "by appointment date" view should show
rescheduled-upcoming calls on their *current* date, use the latest (not earliest) attempt
for non-showed calls. Showed calls always use the showed date. (We left it on earliest;
it's a small edge case.)

## 8. Sync reliability fixes (we hit these during backfill — likely relevant there too)

A long full sync of thousands of opps exposed two bugs that also explain a chronic
"sync stuck / incremental never completes" symptom:

1. **One DB connection held for the whole multi-hour run.** Managed Postgres poolers
   (Supabase/PgBouncer) close long-held connections; mid-operation the connection dies.
   Fix: **recycle the DB session every batch** (commit → close → open fresh) so no
   connection is held longer than ~a minute; add `pool_recycle` (e.g. 1800s) and
   `pool_pre_ping=True` on the engine.
2. **No rollback on a per-record error.** Once one statement fails, the transaction is
   aborted and *every subsequent* record fails until a rollback — one bad row (or the
   connection drop above) cascaded into thousands of failures. Fix: **roll back inside the
   per-record error handler** (and recreate the session if the connection itself died).
   Write the final "sync completed" status from a *fresh* session.

## 9. Performance (bulk backfill)

Re-deriving thousands of opps is API-bound (one `/appointments` call per contact).
Sequential (one contact at a time) is slow. Use **bounded concurrency** (e.g. a semaphore
of ~6 concurrent contact fetches) with **429 retry + backoff**, and — critically — if a
contact can't be fetched after retries, **skip it (leave existing data), never write
NULL**. Keep DB writes sequential (one writer) to stay safe. This was ~5× faster while
staying under GHL's rate ceiling.

---

## CONFIG — GHL-location-specific reference (our "1. Sales Pipeline")

Unreliable opportunity custom fields we STOPPED trusting for call data:
- call1 appointment status: `V82ErbW24izA5aQUzRUv`
- call1 initial appointment date: `We5c2Oiz8kC3FgjOO2XD`
- call1 (rescheduled) appointment date: `bFDWu3koncdxn26h6nAm`
- call2 appointment date: `oRRLUFWNYEeYSDVqV3DK`  (also unreliable — overwritten on reschedule)

Calendar `appointmentStatus` values: `Showed`, `No Show`, `Cancelled`, `Confirmed`.

Sales pipeline id: `zbI8YxmB9qhk1h4cInnq`. Deal Won stage: `544b178f-d1f2-4186-a8c2-00c3b0eeefe8`.

Endpoints: `GET /contacts/{id}/appointments`, `GET /calendars/?locationId=…` (to get the
`{calendarId: name}` map for name-based classification), `POST /opportunities/search`
(paginated, `location_id` + `pipeline_id`).
