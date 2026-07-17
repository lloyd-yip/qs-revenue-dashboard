"""Whop payment fetching + payment-metric computation.

Extracted from sync/match_deals_whop.py (clean-as-you-go). Used by both the
matching engine and the nightly whop_refresh job.

Customer-level aggregation (2026-07-14): a customer can settle one deal across
MULTIPLE memberships — e.g. a Splitit contract split over two cards shows up as
an $18,000 membership plus a $2,700 top-up membership. Counting only the matched
membership's payments understates cash, projected total, and first-payment date.
`fetch_customer_payments` therefore folds in paid payments from the customer's
other ("sibling") memberships, guarded against double-counting (see docstring).
"""

import logging
from datetime import datetime, timezone

import httpx

from config import settings

logger = logging.getLogger(__name__)

WHOP_API_BASE = "https://api.whop.com/api/v2"

# Sibling folding guards:
# - only fold memberships created within this many days of the deal close date
#   (an old low-ticket purchase from months earlier is not part of this deal)
SIBLING_WINDOW_DAYS = 60
# - ignore sibling payments at/below this amount (community subs, small one-offs
#   are not deal payments — same floor idea as the Stripe pass)
MIN_SIBLING_PAYMENT = 100.0
# Unattached (membership-less) payments: Whop records some renewal/direct charges
# with membership=None, linked only to the user (verified 2026-07-14 via
# /api/sync/whop-inspect — e.g. a "$6,000 / 3-months" renewal landing as a direct
# charge). They are attributed to the customer's deal when paid, above the floor,
# and created within this window around the deal close date (renewals arrive
# throughout the plan's life).
UNATTACHED_WINDOW_BEFORE_DAYS = 60
UNATTACHED_WINDOW_AFTER_DAYS = 240


def _whop_headers() -> dict:
    return {
        "Authorization": f"Bearer {settings.whop_api_key}",
        "accept": "application/json",
    }


async def _fetch_whop_memberships(client: httpx.AsyncClient) -> list[dict]:
    """Fetch all Whop memberships (paginated). Returns raw API objects.

    Notes:
    - Whop v2 uses per_page (not limit) with a max of 50.
    - Whop v2 pagination: {"current_page": N, "total_page": M, "total_count": X}
      (NOT next_page — we must compare current_page vs total_page).
    - We fetch all statuses (omit status filter) so historical/expired members
      are included — we need to match against past close dates.
    """
    memberships: list[dict] = []
    page = 1
    while True:
        resp = await client.get(
            f"{WHOP_API_BASE}/memberships",
            headers=_whop_headers(),
            params={"per_page": 50, "page": page},
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("data", [])
        memberships.extend(items)
        pagination = data.get("pagination", {})
        current_page = pagination.get("current_page", page)
        total_pages = pagination.get("total_page", 1)
        logger.info(
            f"Whop memberships page {current_page}/{total_pages}: got {len(items)} items, "
            f"total so far={len(memberships)}"
        )
        if not items or current_page >= total_pages:
            break
        page = current_page + 1
    # Log a sample item shape for debugging
    if memberships:
        sample = memberships[0]
        logger.info(
            f"Whop membership sample keys: {list(sample.keys())}, "
            f"user type: {type(sample.get('user')).__name__}, "
            f"has email: {'email' in sample}"
        )
    return memberships


async def _fetch_membership_payments(
    client: httpx.AsyncClient, membership_id: str
) -> list[dict]:
    """Fetch all payment records for one Whop membership.

    Note: Whop v2 pagination uses current_page/total_page, not next_page.
    """
    payments: list[dict] = []
    page = 1
    while True:
        resp = await client.get(
            f"{WHOP_API_BASE}/payments",
            headers=_whop_headers(),
            params={"membership_id": membership_id, "per_page": 50, "page": page},
        )
        if resp.status_code == 404:
            break
        resp.raise_for_status()
        data = resp.json()
        items = data.get("data", [])
        payments.extend(items)
        pagination = data.get("pagination", {})
        current_page = pagination.get("current_page", page)
        total_pages = pagination.get("total_page", 1)
        if not items or current_page >= total_pages:
            break
        page = current_page + 1
    return payments


def _extract_whop_identity(m: dict) -> tuple[str, str]:
    """Pull email + name from a Whop membership object.

    Whop v2 sometimes embeds a full user object under 'user', sometimes
    just a user_id string. Guard against both shapes.
    Email may also sit directly on the membership at root level.
    """
    user = m.get("user")
    # Protect against user being a string user_id rather than an embedded dict
    if not isinstance(user, dict):
        user = {}
    email = m.get("email") or user.get("email") or ""
    name = (
        m.get("name")
        or user.get("name")
        or user.get("username")
        or f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        or ""
    )
    return email.lower().strip(), name.strip()


def _membership_created_date(m: dict):
    """created_at of a membership as a date, or None (handles int ts + ISO)."""
    raw = m.get("created_at")
    if not raw:
        return None
    try:
        if isinstance(raw, (int, float)):
            return datetime.fromtimestamp(raw, tz=timezone.utc).date()
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).date()
    except (ValueError, AttributeError, OSError):
        return None


def build_membership_email_index(memberships: list[dict]) -> dict[str, list[dict]]:
    """Index memberships by lowercased customer email (empty emails skipped)."""
    index: dict[str, list[dict]] = {}
    for m in memberships:
        email, _ = _extract_whop_identity(m)
        if email:
            index.setdefault(email, []).append(m)
    return index


def sibling_memberships(
    matched_m: dict,
    memberships_by_email: dict[str, list[dict]],
    claimed_other_ids: set[str],
    close_date,
) -> list[dict]:
    """Other memberships of the SAME customer whose payments belong to this deal.

    A sibling qualifies when it:
    - shares the matched membership's email,
    - is not the matched membership itself,
    - is not claimed by a DIFFERENT deal (prevents double-counting when one
      customer legitimately has two deals),
    - was created within ±SIBLING_WINDOW_DAYS of the deal close date (an old
      unrelated purchase is not part of this deal).
    """
    email, _ = _extract_whop_identity(matched_m)
    if not email:
        return []
    siblings = []
    for m in memberships_by_email.get(email, []):
        mid = m.get("id")
        if not mid or mid == matched_m.get("id") or mid in claimed_other_ids:
            continue
        if close_date is not None:
            created = _membership_created_date(m)
            if created is None or abs((created - close_date).days) > SIBLING_WINDOW_DAYS:
                continue
        siblings.append(m)
    return siblings


async def fetch_all_payments(client: httpx.AsyncClient) -> list[dict]:
    """Fetch ALL company payments (paginated sweep of /payments, no filter).

    One sweep per run replaces per-membership fetches AND surfaces payments
    with membership=None (direct charges / renewals), which membership-scoped
    queries can never return.
    """
    payments: list[dict] = []
    page = 1
    while page <= 200:  # hard backstop
        resp = await client.get(
            f"{WHOP_API_BASE}/payments",
            headers=_whop_headers(),
            params={"per_page": 50, "page": page},
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("data", [])
        payments.extend(items)
        pagination = data.get("pagination", {})
        current_page = pagination.get("current_page", page)
        total_pages = pagination.get("total_page", 1)
        if not items or current_page >= total_pages:
            break
        page = current_page + 1
    logger.info(f"Whop payments sweep: {len(payments)} payments across {page} page(s)")
    return payments


def _obj_id(v) -> str | None:
    """Whop nests related objects as either a string id or an embedded dict."""
    if isinstance(v, dict):
        return v.get("id")
    return v if isinstance(v, str) else None


def membership_user_id(m: dict | None) -> str | None:
    return _obj_id((m or {}).get("user"))


def build_payment_indexes(payments: list[dict]) -> tuple[dict, dict]:
    """Index a payments sweep → (by_membership_id, unattached_by_user_id)."""
    by_membership: dict[str, list[dict]] = {}
    unattached_by_user: dict[str, list[dict]] = {}
    for p in payments:
        mid = _obj_id(p.get("membership"))
        if mid:
            by_membership.setdefault(mid, []).append(p)
            continue
        uid = _obj_id(p.get("user")) or p.get("user_id")
        if uid:
            unattached_by_user.setdefault(uid, []).append(p)
    return by_membership, unattached_by_user


def _payment_date(p: dict):
    raw = p.get("created_at") or p.get("paid_at")
    if not raw:
        return None
    try:
        if isinstance(raw, (int, float)):
            return datetime.fromtimestamp(raw, tz=timezone.utc).date()
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).date()
    except (ValueError, AttributeError, OSError):
        return None


def _is_deal_payment(p: dict) -> bool:
    return (
        p.get("status") in ("paid", "complete", "completed")
        and float(p.get("final_amount") or p.get("total") or p.get("subtotal") or 0) > MIN_SIBLING_PAYMENT
    )


def collect_customer_payments(
    matched_m: dict,
    siblings: list[dict],
    by_membership: dict[str, list[dict]],
    unattached_by_user: dict[str, list[dict]],
    close_date,
) -> tuple[list[dict], list[str]]:
    """All payments belonging to one deal's customer, from the run's sweep indexes.

    = matched membership's payments
    + qualifying paid payments from sibling memberships (see sibling_memberships)
    + UNATTACHED payments (membership=None) of the customer's user(s), when paid,
      above the floor, and within [-UNATTACHED_WINDOW_BEFORE_DAYS,
      +UNATTACHED_WINDOW_AFTER_DAYS] of the deal close date.

    Returns (payments, fold_notes) — fold_notes describe what was folded, for logs.
    """
    payments = list(by_membership.get(matched_m.get("id") or "", []))
    notes: list[str] = []

    for sib in siblings:
        sib_id = sib.get("id")
        if not sib_id:
            continue
        deal_payments = [p for p in by_membership.get(sib_id, []) if _is_deal_payment(p)]
        if deal_payments:
            payments.extend(deal_payments)
            notes.append(f"sibling {sib_id}: {len(deal_payments)} payment(s)")

    user_ids = {membership_user_id(matched_m)} | {membership_user_id(s) for s in siblings}
    user_ids.discard(None)
    for uid in user_ids:
        for p in unattached_by_user.get(uid, []):
            if not _is_deal_payment(p):
                continue
            if close_date is not None:
                pdate = _payment_date(p)
                if pdate is None:
                    continue
                delta = (pdate - close_date).days
                if delta < -UNATTACHED_WINDOW_BEFORE_DAYS or delta > UNATTACHED_WINDOW_AFTER_DAYS:
                    continue
            payments.append(p)
            notes.append(f"unattached payment {p.get('id')} (user {uid})")
    return payments, notes


def membership_is_recurring(m: dict | None) -> bool:
    """Best-effort: a membership with a renewal period is a recurring subscription plan.

    Renewing plans (e.g. "$6,000 / 3-months") carry no split_pay_required_payments,
    so this signal lets the plan-length inference kick in from the FIRST payment
    instead of waiting for a second installment to prove the repeating pattern.
    Field absent → False (no inference; pay-in-full deals stay untouched).
    """
    if not isinstance(m, dict):
        return False
    return bool(m.get("renewal_period_start") or m.get("renewal_period_end"))


def _detect_external_processor(paid_payments: list[dict]) -> tuple[bool, bool]:
    """Detect whether any paid payment used an external financing processor.

    Returns (is_splitit, is_claritypay). Both signals read payment.payment_processor —
    the only place ClarityPay is visible (membership.payment_processor reads "multi_psp"
    for ClarityPay deals). Verified against live Whop data 2026-06-11.
    """
    processors = {(p.get("payment_processor") or "").lower() for p in paid_payments}
    return ("splitit" in processors, "claritypay" in processors)


def _compute_payment_metrics(
    payments: list[dict],
    ghl_monetary_value: float,
    installments_override: int | None = None,
    is_recurring: bool = False,
) -> dict:
    """Derive payment summary from raw Whop payment objects.

    Returns upfront_cash, total_paid, payment_count, is_financing, remaining_ar,
    is_splitit, is_claritypay, provider_fee_pct, net_cash_collected, plan_months_flag,
    first_payment_date, total_installments.

    installments_override: the membership's split_pay_required_payments — the
    authoritative plan length. Whop does NOT pre-create future installment records,
    so len(payments) under-counts internal plans (a 6-month plan shows only the
    installments collected so far). When provided, it sets total_installments and
    drives plan_months_flag. Falls back to len(payments) when absent.
    """
    paid = [
        p for p in payments
        if p.get("status") in ("paid", "complete", "completed")
    ]
    total_paid = sum(
        float(p.get("final_amount") or p.get("total") or p.get("subtotal") or 0)
        for p in paid
    )
    payment_count = len(paid)

    # Refunds — Whop marks a refunded payment with status 'refunded'/'partially_refunded'
    # and/or a refunded-amount field. Sum whatever was returned to the customer.
    def _refund_amount(p: dict) -> float:
        ramt = float(
            p.get("refunded_amount") or p.get("amount_refunded")
            or p.get("refund_amount") or 0
        )
        if ramt > 0:
            return ramt
        if (p.get("status") or "").lower() in ("refunded", "partially_refunded"):
            return float(p.get("final_amount") or p.get("total") or p.get("subtotal") or 0)
        return 0.0

    total_refunded = round(sum(_refund_amount(p) for p in payments), 2)
    contract_value = ghl_monetary_value or 0.0
    remaining_ar = max(contract_value - total_paid, 0.0) if contract_value else None
    # is_financing = any deal with remaining AR outstanding, regardless of
    # how many payments have been collected so far. A deal with 1 payment
    # made and $14k still owed is absolutely a financed deal.
    is_financing = bool(remaining_ar and remaining_ar > 0)

    # ── Processor detection + net cash (payment-level) ──────────────────────
    # Splitit / ClarityPay = external financing: QS receives the full contract
    # upfront, minus a 15% fee. Whop records these as a single upfront payment,
    # so total_paid == full contract and net = total_paid * 0.85.
    # Internal plans / pay-in-full: no fee, net = total_paid (cash collected to date).
    is_splitit, is_claritypay = _detect_external_processor(paid)
    is_external = is_splitit or is_claritypay
    provider_fee_pct = 0.15 if is_external else 0.0
    net_cash_collected = round(total_paid * (1 - provider_fee_pct), 2)

    # total_installments: authoritative plan length from the membership's
    # split_pay_required_payments (passed as installments_override). len(payments)
    # under-counts internal plans because Whop does not pre-create future records.
    #
    # Stale-override guard: if MORE paid installments exist than the override
    # claims, the stored plan length is not authoritative (e.g. a renewal plan
    # whose length was recorded as len(payments)=1 at match time) — discard it.
    # For recurring memberships, an override merely EQUAL to payments-seen is
    # the same stale fallback signature (the plan renews beyond it).
    if (
        installments_override
        and not is_external
        and (
            payment_count > installments_override
            or (is_recurring and payment_count >= installments_override)
        )
    ):
        installments_override = None
    total_installments = (
        installments_override if installments_override
        else (len(payments) if payments else None)
    )
    # Renewal-plan inference: internal recurring memberships (e.g. "$6,000 /
    # 3-months" renewing quarterly) have no split_pay_required_payments, so the
    # plan length is unknown. Infer the intended count from the GHL contract as
    # a COUNT hint only — never trust its amount: round-to-NEAREST of
    # contract ÷ avg installment. Nearest (not ceil) because plans are typically
    # 3/6/12 payments and the GHL value skews high — prefer UNDER-projecting;
    # when more installments actually land, the plan length grows to match, and
    # a fully-paid deal always ends at its true total.
    # Triggers on ≥2 installments (a proven repeating pattern), or from the FIRST
    # payment when the membership itself is a renewing plan (is_recurring).
    # Single-payment one-time deals are untouched (a rep-overstated GHL value
    # must not inflate a pay-in-full deal).
    if (
        not installments_override
        and not is_external
        and (payment_count >= 2 or (is_recurring and payment_count >= 1))
        and contract_value > 0
        and total_paid > 0
    ):
        avg_installment = total_paid / payment_count
        inferred = int(contract_value / avg_installment + 0.5)  # round half up
        inferred = max(inferred, payment_count)  # never below what already landed
        if inferred > (total_installments or 0):
            total_installments = min(inferred, 12)
    # plan_months_flag: internal plan (no external financing) running longer than 3.
    plan_months_flag = bool(
        not is_external
        and total_installments is not None
        and total_installments > 3
    )

    # Upfront cash: external financing = full amount upfront (sum of the financed
    # payment records). Otherwise the first payment amount.
    upfront_cash = None
    first_payment_date = None

    if paid:
        if is_external:
            ext_payments = [
                p for p in paid
                if (p.get("payment_processor") or "").lower() in ("splitit", "claritypay")
            ]
            upfront_cash = sum(
                float(p.get("final_amount") or p.get("total") or 0)
                for p in ext_payments
            )
        else:
            first = min(paid, key=lambda p: p.get("created_at") or p.get("paid_at") or 0)
            upfront_cash = float(first.get("final_amount") or first.get("total") or 0)

        # first_payment_date: earliest paid payment — used as canonical close date.
        earliest = min(paid, key=lambda p: p.get("created_at") or p.get("paid_at") or 0)
        raw_ts = earliest.get("created_at") or earliest.get("paid_at")
        if raw_ts:
            try:
                if isinstance(raw_ts, (int, float)):
                    first_payment_date = datetime.fromtimestamp(raw_ts, tz=timezone.utc).date()
                else:
                    first_payment_date = datetime.fromisoformat(
                        str(raw_ts).replace("Z", "+00:00")
                    ).date()
            except (ValueError, AttributeError, OSError):
                first_payment_date = None

    return {
        "upfront_cash": round(upfront_cash, 2) if upfront_cash else None,
        "total_paid": round(total_paid, 2),
        "total_refunded": total_refunded or None,
        "payment_count": payment_count,
        "is_financing": is_financing,
        "total_contract_value": round(contract_value, 2) if contract_value else None,
        "remaining_ar": round(remaining_ar, 2) if remaining_ar is not None else None,
        "is_splitit": is_splitit,
        "is_claritypay": is_claritypay,
        "provider_fee_pct": provider_fee_pct,
        "net_cash_collected": net_cash_collected,
        "plan_months_flag": plan_months_flag,
        "first_payment_date": first_payment_date,
        "total_installments": total_installments,
    }
