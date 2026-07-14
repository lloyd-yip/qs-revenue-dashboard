---
tags: [whop, payments, deal-matching, revenue, fees, splitit, claritypay]
verified: 2026-06-11
source: live Whop API scan of all 65 memberships + payment objects
---

# Whop Payment Processor & Settlement Model

How Whop represents payments for QS deals. Verified against the live API (all 65
memberships + their payment records) on 2026-06-11. **Do not re-derive this by guessing
field names — it was wrong once already.**

## Where `payment_processor` lives — and the ClarityPay trap

`payment_processor` exists on BOTH the membership object and the payment object, and they
**disagree**:

| | membership.payment_processor | payment.payment_processor |
|---|---|---|
| Splitit deal | `splitit` | `splitit` |
| ClarityPay deal | `multi_psp` ⚠️ | `claritypay` |
| Internal plan / pay-in-full | `multi_psp` | `card` |

**ClarityPay is invisible at the membership level** — it reads `multi_psp` (Whop's generic
multi-PSP router). It is ONLY detectable on the PAYMENT object. Any processor detection MUST
scan paid payments, never the membership field. `multi_psp` is a catch-all and proves nothing.

Across 65 memberships: 54 `multi_psp`, 11 `splitit` at the membership level. ClarityPay (and
internal-vs-card) only separable at the payment level.

## The four deal types and how cash is settled

| Type | payment.processor | split_pay_required_payments | How Whop records it | QS fee | net cash |
|------|-------------------|------------------------------|---------------------|--------|----------|
| Splitit | `splitit` | None | **single upfront payment = full contract** | 15% | `total_paid × 0.85` |
| ClarityPay | `claritypay` | None | **single upfront payment = full contract** | 15% | `total_paid × 0.85` |
| Internal plan | `card` | N (2,3,4,6…) | **only installments collected so far** | 0% | `total_paid` (to date) |
| Pay-in-full | `card` | None | single payment | 0% | `total_paid` |

Key consequences:

1. **External financing (Splitit/ClarityPay) settles 100% upfront.** Whop logs the full
   contract as ONE paid payment immediately. So `total_paid` already equals the full amount,
   and `net = total_paid × 0.85` is correct from day one. QS banks 85% upfront; the financer
   collects installments from the customer.

2. **Internal plans only show collected installments.** A 6-month plan that has collected one
   installment shows ONE payment record (e.g. $2,875), not six. `len(payments)` therefore
   UNDER-COUNTS plan length. Use the membership's `split_pay_required_payments` as the
   authoritative plan length (it's set at membership creation). `match_deals_whop._compute_payment_metrics`
   takes `installments_override` for exactly this.

3. **Anomaly flag** (`plan_months_flag`): internal plan (NOT splitit/claritypay) with
   `total_installments > 3`. Assumes plans are monthly (installments ≈ months). If QS ever
   runs weekly/biweekly internal plans this flag over-fires.

## Payment object fields that matter

`status` (paid/complete/completed = counted), `final_amount` (preferred amount field, falls
back to `total`/`subtotal`), `payment_processor`, `payment_method_type`, `created_at`/`paid_at`
(Unix int → first_payment_date), `refunded_amount` (NOT currently subtracted from total_paid —
known gap).

## Data-quality gotchas observed

- **One deal can settle across MULTIPLE memberships (2026-07-14).** Splitit sometimes cannot
  approve the full contract on one card, so the customer completes it as two memberships —
  e.g. james@polarinsight.com: an $18,000 "quantumSCALE Institute" membership (Jun 11) plus a
  separate "Splitit $2,700" top-up membership (Jul 6). Counting only the matched membership's
  payments understated cash ($2,700 vs $20,700), projected total, first_payment_date, and cycle.
  Fixed by customer-level "sibling folding" in `sync/whop_payments.py`: payments from the
  customer's other memberships are folded in when (a) same email, (b) the membership is not
  claimed by a different deal, (c) the customer has no other matched deals, (d) created within
  ±60d of the deal close date, and (e) each folded payment is paid and > $100 (sub floor).
- **GHL contract value is unreliable.** Seen: a Splitit deal with `ghl_monetary_value` = $10,000
  but actual Splitit charge ~$18,000 → net cash ($15,296) exceeds the displayed "contract."
  Trust the Whop `total_paid`, not GHL `monetary_value`. This is the core reason the live-revenue
  feature exists.
- **`ghl_owner_name` is sparse on historical deals** → most pre-2026 matches group under
  "Unassigned" in per-rep views. Attribution is only as good as GHL owner population.

## Where this is used

- `sync/whop_payments.py` — payment fetching, `_compute_payment_metrics`, sibling folding
  (extracted from match_deals_whop.py 2026-07-14; matcher re-exports for compat).
- `sync/match_deals_whop.py` — matching engine; folds sibling payments during full Run Match.
- `sync/whop_refresh.py::refresh_current_month_payment_metrics` — lightweight EOD refresh of
  current-month rows (Whop-membership deals only); also folds siblings and can move
  first_payment_date earlier when a folded payment predates the matched membership's.
- `db/queries/whop_live.py` + `api/routers/whop_live.py` — the P&L "Live Whop Revenue" section.
