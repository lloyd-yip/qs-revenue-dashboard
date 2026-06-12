# Blueprint v2: Monthly Cash Ledger (net-new vs recurring)
# Project: qs-dashboard · Step 2 of deals-rework-plan.md (RECURRING CASH WORKSTREAM)
# Status: V2 — pending Lloyd's decisions (see OPEN DECISIONS) before final approval to build

## What this is
A per-payment ledger so the Live view can show, per month M: net-new cash (deals that CLOSED in M),
recurring cash (installments landing in M from deals closed earlier), total cash, and contract realized.

## Why v1 was wrong (3 serious defects caught by adversarial review against the LIVE DB)

**D1 — FATAL: a Whop-membership-only ledger drops ~$2M / 85% of matched cash.**
Live DB: of high/medium matched deals WITH cash, only **39 have a whop_membership_id; 173 are Stripe-matched ($2,074,563 net)**. The existing `get_whop_live_summary_for_month` reads `net_cash_collected` off the match row (Stripe enrichment fills it), so it already includes Stripe. A ledger fed ONLY by `_fetch_membership_payments` would silently represent ~18% of cash. AND Stripe deals also collect across months (111 multi-installment, 75 with open AR $258K) — so Stripe has recurring cash too.
→ FIX: the ledger must ingest BOTH Whop payments AND Stripe charges. Rename table `whop_payments` → `deal_payments` (source-agnostic; "whop_payments" would be a misleading name).

**D2 — Recurring cash silently decays to zero without a refresh-selector fix.**
`get_current_month_refresh_targets` filters `first_payment_date in current month`. A deal closed in March with a June installment is NOT a current-month target → June's installment never enters the ledger. Live DB: **123 of 125 internal multi-installment deals closed in prior months.** A one-shot backfill only captures installments that exist AT backfill time.
→ FIX: refresh selector targets all OPEN payment plans (total_installments > payment_count OR remaining_ar > 0), regardless of close month. Bounded set (~125, shrinking as plans complete).

**D3 — Close-month anchor: `first_payment_date` disagrees with `ghl_close_date` ~47% of the time.**
Live DB: of 215 deals with both dates, **101 (47%) have first_payment_date in a different MONTH than ghl_close_date**. Anchoring net-new on first_payment_date means net-new(M) won't reconcile with the rest of the dashboard's "deals closed in M".
→ DECISION NEEDED (see OPEN DECISIONS): recommend anchor close-month on **ghl_close_date** (what sales team + every other view use), bucket each installment on its own **paid_date**.

## Other confirmed fixes folded into v2
- **Refunds:** add `refunded_amount` column; net = (gross − refunded) × (1 − fee_pct). `seed_revenue_whop.py` already does this — reuse, don't reinvent. Without it, a refunded deal counts as collected forever.
- **Idempotency:** upsert on (source, source_payment_id); `on_conflict_do_update` must OVERWRITE all mutable fields (status, amounts, paid_date, refunded_amount) so pending→paid and refunds land on re-sync.
- **Status set:** reuse the canon ("paid","complete","completed") — not just "paid" (else complete/completed silently dropped).
- **Validation:** Pydantic `WhopPaymentIn` / `StripeChargeIn` at the boundary; reject row if no payment id; skip+COUNT malformed (graceful degradation), never let one bad row abort a membership.
- **Per-PAYMENT fee derivation:** read fee from THAT payment's processor string — NOT the membership-level `_detect_external_processor` (category error; wrong on mixed-processor memberships).
- **Money math in Decimal** (fee_pct = Decimal("0.15"), quantize ROUND_HALF_UP) — per-row rounding will diverge from the legacy aggregate; reconciliation test decides canonical.
- **Unattributed bucket + reconciliation invariant:** NULL close-month or no-match payment → explicit surfaced "Unattributed" bucket. Assert/log `net_new + recurring + unattributed == SUM(net) for paid rows in M`. This single check catches every split defect.
- **contract_realized on DEAL grain** (join to deal), never summed from the ledger.
- **Recurring = portfolio total + drill-down list, NOT a per-rep leaderboard** (a rep's recurring is the mechanical tail of old deals; ranking by it starts the wrong conversation; ghl_owner_name sparse on old deals anyway). Net-new stays per-rep (the scoreboard).
- **Testability:** split `backfill_*` into zero-arg wrapper + injectable `_backfill(session, clients, deals)` core.
- **Nightly loop:** separate try/except for metric-refresh vs ledger-population; reuse the SINGLE `_fetch_membership_payments` result for both (don't double-fetch); per-row commit.
- **Purpose splits:** `split_payment_attribution(paid_month, close_month)→bucket` (pure); `group_monthly_cash(rows, M)→dict` (pure); `get_monthly_cash_summary` = thin assembler.

## V2 FILES (skeleton)
1. `migrations/versions/whop002_add_deal_payments.py` (NEW) — `deal_payments` table; down_revision=whop001; reversible.
2. `db/models.py` (+`DealPayment`) — id, source('whop'|'stripe'), source_payment_id, UNIQUE(source,source_payment_id), ghl_opportunity_id (idx), paid_date (idx), gross_amount, refunded_amount, fee_pct, net_amount, processor, status, currency, timestamps.
3. `db/queries/deal_payments.py` (NEW) — `upsert_deal_payments(session, rows)→int`; `query_month_payments(session, m_start, m_end)→list` (LEFT JOIN deal_whop_matches for close-month+owner); `get_available_payment_months(session)→list[str]`.
4. `sync/payment_ledger.py` (NEW) — `_whop_payment_to_ledger_row(payment, opp_id)→dict|None`; `_stripe_charge_to_ledger_row(charge, opp_id)→dict|None`; `sync_whop_deal_payments(session, client, deal)→int`; `sync_stripe_deal_payments(session, deal, stripe_index)→int`; `backfill_deal_payment_ledger()→dict` (wrapper) + `_backfill(session, whop_client, stripe_index, deals)→dict` (injectable).
5. `db/queries/whop_live.py` (MOD) — NEW `split_payment_attribution`, `group_monthly_cash`, `get_monthly_cash_summary`, `get_contract_realized_for_month`; REFACTOR `get_whop_live_summary_for_month` (superseded), `get_current_month_refresh_targets` → open-plan selector.
6. `sync/whop_refresh.py` (MOD) — open-plan selector; separate try/except; single-fetch reuse; also populate ledger.
7. `api/routers/whop_live.py` (MOD) — restructure `GET /pnl/whop-live` → {month, net_new:{reps,totals}, recurring:{total, deals[]}, unattributed:{total}, totals:{net_new_cash, recurring_cash, total_cash, contract_realized}, reconciles:bool, last_refreshed}; NEW `GET /pnl/whop-live/months`.
8. `api/routers/sync.py` (MOD) — `POST /api/sync/backfill-payment-ledger`.

## OPEN DECISIONS (need Lloyd before final build approval)
1. **Stripe scope:** confirm the ledger ingests Stripe charges too (required, else ~$2M dropped). [Recommend: YES]
2. **Close-month anchor:** ghl_close_date (reconciles with sales/CRM, recommended) vs first_payment_date (current live-view behavior). [Recommend: ghl_close_date, fallback first_payment_date]
3. **Recurring presentation:** portfolio total + drill-down list (recommended) vs per-rep. [Recommend: total + drill-down]
4. **Scope/timing:** this is meaningfully bigger than v1 (dual-source ingestion + refunds + open-plan refresh). Proceed as one build, or phase?

## VERIFICATION (silent-failure signals)
- Reconciliation: endpoint `total_cash` for a month == SUM(net_amount) of paid ledger rows in that month == (within cents) legacy `get_whop_live_summary_for_month` total. If off by ~$2M → Stripe not ingested.
- Decay check: after a clean backfill, advance one month with NO new backfill → recurring must NOT drop to ~0 (proves open-plan refresh works).
- Idempotency: sync same payment twice → COUNT==1, monthly total unchanged.
