"""GHL ↔ Whop + Stripe Deal Reconciliation Engine.

Two-pass matching:
  Pass 1 (Whop): Match deals to Whop memberships by email/domain/name.
  Pass 2 (Stripe): Match remaining unmatched deals via Stripe metadata
    (GHL contactId in charge/customer metadata) and email. Also enriches
    Whop-matched deals that have missing payment data (upfront_cash, total_paid).

Scoring (Whop pass):
  1. Exact email match      — perfect match (score 1.00)
  2. Email domain match     — strong signal (score 0.80)
  3. Fuzzy domain match     — similar domains / typos (score 0.50)
  4. Name similarity        — secondary signal (+0.25 / +0.12)
  5. ±3-day timing window   — only for fuzzy/domain matches

Scoring (Stripe pass):
  1. GHL contactId match    — from Stripe charge/customer metadata (score 1.00)
  2. Email exact match      — customer email = GHL contact email (score 0.95)

Idempotency gate: if is_confirmed=True on an existing row, it is NEVER
overwritten — manual matches survive any number of re-runs.

Run (triggered from dashboard button or Railway endpoint):
    POST /api/dashboard/deals/run-match   (bearer token required)

Or run locally for testing (requires env vars GHL_API_KEY, DATABASE_URL):
    python3 sync/match_deals_whop.py
"""

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher

import httpx

from config import settings
from db.queries.deal_matches import get_won_deals, upsert_deal_match
from db.session import AsyncSessionLocal
from sync.ghl_client import GHLClient

logger = logging.getLogger(__name__)

WHOP_API_BASE = "https://api.whop.com/api/v2"
MATCH_WINDOW_DAYS = 3  # scan Whop memberships created ±3 days from GHL close_date

# Minimum Stripe charge (in cents) that counts as a DEAL payment. Clients also buy our
# GoHighLevel SaaS subscription (~$97/month) — that is them paying us for a service, NOT a
# deal payment, and must never be counted as the deal's first payment or contract value.
# Real deal payments are high-ticket (or monthly but >$100), so flooring Stripe matching at
# $100 cleanly excludes the $97 GHL sub. (GHL subscriptions bill via Stripe, not Whop, so the
# Whop pass needs no equivalent floor.)
MIN_STRIPE_DEAL_PAYMENT_CENTS = 10000  # $100.00
# Corporate-domain matches are high-signal (a shared company domain rarely collides),
# so we allow a much wider window for them — the payer at a company can sign up on Whop
# days or weeks apart from when the rep marks the GHL deal Won. Name/fuzzy candidates
# stay gated to MATCH_WINDOW_DAYS to avoid false positives.
DOMAIN_MATCH_WINDOW_DAYS = 30

# Free/personal email domains — a shared personal domain (both @gmail.com) proves nothing,
# so it never triggers a corporate-domain match.
PERSONAL_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
    "icloud.com", "me.com", "aol.com", "protonmail.com",
}

# Whop product IDs that count as "high-ticket" (for plan type classification)
HIGH_TICKET_PRODUCT_IDS = {
    "prod_7MNNKNOvuS4V5",
    "prod_OicLQ3n7l2pPQ",
    "prod_MOqVyn0Tj36mR",
}


# ── Scoring helpers ──────────────────────────────────────────────────────────

def _normalize_name(name: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    name = name.lower().strip()
    name = re.sub(r"[^\w\s]", "", name)
    return re.sub(r"\s+", " ", name)


def _name_similarity(a: str, b: str) -> float:
    """Blended Jaccard + sequence ratio — handles 'Acme Inc' vs 'Acme LLC'."""
    a, b = _normalize_name(a), _normalize_name(b)
    if not a or not b:
        return 0.0
    a_tokens = set(a.split())
    b_tokens = set(b.split())
    union = len(a_tokens | b_tokens)
    jaccard = len(a_tokens & b_tokens) / union if union else 0.0
    seq = SequenceMatcher(None, a, b).ratio()
    return max(jaccard, seq * 0.85)


def _domains_similar(a: str, b: str) -> bool:
    """True when two email domains are very close (typo, subdomain, alias).

    E.g. 'acmecorp.com' vs 'acme-corp.com' → True.
    Compares the base domain name only (strips TLD).
    """
    a_base = a.split(".")[0] if "." in a else a
    b_base = b.split(".")[0] if "." in b else b
    return SequenceMatcher(None, a_base, b_base).ratio() >= 0.82


def score_match(
    ghl_email: str,
    ghl_name: str,
    whop_email: str,
    whop_name: str,
) -> tuple[float, str]:
    """Return (score 0..1, method_label) for one GHL↔Whop candidate pair.

    Score interpretation:
      >= 0.75 → HIGH confidence
      >= 0.50 → MEDIUM confidence
      >= 0.25 → LOW confidence
      <  0.25 → effectively unmatched
    """
    ghl_email = (ghl_email or "").lower().strip()
    whop_email = (whop_email or "").lower().strip()
    ghl_name = (ghl_name or "").strip()
    whop_name = (whop_name or "").strip()

    # ── Exact email match ───────────────────────────────────────────────────
    if ghl_email and whop_email and ghl_email == whop_email:
        return 1.0, "email_exact"

    score = 0.0
    methods: list[str] = []

    # ── Email domain match ──────────────────────────────────────────────────
    ghl_domain = ghl_email.split("@")[1] if "@" in ghl_email else ""
    whop_domain = whop_email.split("@")[1] if "@" in whop_email else ""

    # Skip personal domains for domain-based matching — they prove nothing
    if ghl_domain and whop_domain:
        ghl_personal = ghl_domain in PERSONAL_DOMAINS
        whop_personal = whop_domain in PERSONAL_DOMAINS

        if ghl_domain == whop_domain and not ghl_personal:
            score = 0.80
            methods.append("email_domain")
        elif ghl_domain == whop_domain and ghl_personal:
            # Same personal domain match (e.g. both @gmail.com) — weak signal
            score = 0.35
            methods.append("email_personal_domain")
        elif not ghl_personal and not whop_personal and _domains_similar(ghl_domain, whop_domain):
            score = 0.50
            methods.append("email_domain_fuzzy")

    # ── Name similarity (additive bonus) ───────────────────────────────────
    if ghl_name and whop_name:
        name_sim = _name_similarity(ghl_name, whop_name)
        if name_sim >= 0.85:
            score += 0.25
            methods.append("name_strong")
        elif name_sim >= 0.65:
            score += 0.12
            methods.append("name_partial")

    return min(score, 1.0), "+".join(methods) if methods else "none"


def classify_confidence(score: float) -> str:
    if score >= 0.75:
        return "high"
    if score >= 0.50:
        return "medium"
    if score >= 0.25:
        return "low"
    return "unmatched"


# ── Whop API helpers ─────────────────────────────────────────────────────────

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
    total_installments = (
        installments_override if installments_override
        else (len(payments) if payments else None)
    )
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


# ── Stripe API helpers ──────────────────────────────────────────────────────

STRIPE_API_BASE = "https://api.stripe.com/v1"


async def _fetch_stripe_charges(client: httpx.AsyncClient) -> list[dict]:
    """Fetch all succeeded Stripe charges above the deal-payment floor.

    The floor (MIN_STRIPE_DEAL_PAYMENT_CENTS) excludes the ~$97/mo GoHighLevel SaaS
    subscription, which is not a deal payment. Uses Stripe Search API with pagination
    via next_page token. Returns raw charge objects with amount in cents.
    """
    charges: list[dict] = []
    query = f"status:'succeeded' AND amount>{MIN_STRIPE_DEAL_PAYMENT_CENTS}"
    next_page = None

    while True:
        params: dict = {"query": query, "limit": 100}
        if next_page:
            params["page"] = next_page

        resp = await client.get(
            f"{STRIPE_API_BASE}/charges/search",
            params=params,
            auth=(settings.stripe_secret_key, ""),
        )
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("data", [])
        charges.extend(batch)
        logger.info(
            f"Stripe charges page: got {len(batch)}, total so far={len(charges)}"
        )

        if data.get("has_more") and data.get("next_page"):
            next_page = data["next_page"]
        else:
            break

    return charges


async def _fetch_stripe_customer(
    client: httpx.AsyncClient, customer_id: str, cache: dict
) -> dict:
    """Fetch a Stripe customer by ID, with in-memory cache."""
    if customer_id in cache:
        return cache[customer_id]

    resp = await client.get(
        f"{STRIPE_API_BASE}/customers/{customer_id}",
        auth=(settings.stripe_secret_key, ""),
    )
    if resp.status_code == 404:
        cache[customer_id] = {}
        return {}
    resp.raise_for_status()
    customer = resp.json()
    cache[customer_id] = customer
    return customer


async def _build_stripe_index(
    client: httpx.AsyncClient,
) -> tuple[dict[str, list[dict]], dict[str, list[dict]]]:
    """Build lookup maps from Stripe charges for deal matching.

    Resolves customer data for each charge (cached) and extracts:
    - GHL contact ID from charge.metadata.contactId or customer.metadata.id
    - Email from charge.receipt_email or customer.email

    Returns:
        ghl_contact_map: {ghl_contact_id: [charges_with_resolved_data]}
        email_map:       {email: [charges_with_resolved_data]}
    """
    charges = await _fetch_stripe_charges(client)
    logger.info(f"Fetched {len(charges)} total Stripe charges")

    customer_cache: dict[str, dict] = {}
    ghl_contact_map: dict[str, list[dict]] = {}
    email_map: dict[str, list[dict]] = {}

    for charge in charges:
        cust_id = charge.get("customer")
        customer: dict = {}
        if cust_id:
            customer = await _fetch_stripe_customer(client, cust_id, customer_cache)
            await asyncio.sleep(0.05)  # ~20 req/s — stay well inside Stripe 100/s limit

        # Attach resolved data to charge object for later use
        charge["_resolved_customer"] = customer
        charge["_resolved_email"] = (
            charge.get("receipt_email")
            or customer.get("email")
            or ""
        ).lower().strip()

        # Extract GHL contact ID from either charge or customer metadata
        ch_meta = charge.get("metadata") or {}
        cu_meta = customer.get("metadata") or {}
        ghl_cid = ch_meta.get("contactId") or cu_meta.get("id") or ""
        charge["_ghl_contact_id"] = ghl_cid

        if ghl_cid:
            ghl_contact_map.setdefault(ghl_cid, []).append(charge)
        email = charge["_resolved_email"]
        if email:
            email_map.setdefault(email, []).append(charge)

    logger.info(
        f"Stripe index built: {len(ghl_contact_map)} GHL contacts, "
        f"{len(email_map)} unique emails, "
        f"{len(customer_cache)} customers resolved"
    )
    return ghl_contact_map, email_map


def _compute_stripe_payment_metrics(
    charges: list[dict], ghl_monetary_value: float
) -> dict:
    """Derive payment summary from Stripe charge objects.

    Stripe amounts are in cents — divide by 100.
    Returns same shape as _compute_payment_metrics for consistency.
    """
    succeeded = [c for c in charges if c.get("status") == "succeeded"]
    if not succeeded:
        return {}

    total_paid = sum(c.get("amount", 0) / 100.0 for c in succeeded)
    payment_count = len(succeeded)
    contract_value = ghl_monetary_value or 0.0
    remaining_ar = max(contract_value - total_paid, 0.0) if contract_value else None
    is_financing = bool(remaining_ar and remaining_ar > 0)

    sorted_charges = sorted(succeeded, key=lambda c: c.get("created", 0))
    upfront_cash = sorted_charges[0].get("amount", 0) / 100.0 if sorted_charges else None

    first_ts = sorted_charges[0].get("created") if sorted_charges else None
    first_payment_date = None
    if first_ts:
        try:
            first_payment_date = datetime.fromtimestamp(first_ts, tz=timezone.utc).date()
        except (ValueError, OSError):
            pass

    return {
        "upfront_cash": round(upfront_cash, 2) if upfront_cash else None,
        "total_paid": round(total_paid, 2),
        "payment_count": payment_count,
        "is_financing": is_financing,
        "total_contract_value": round(contract_value, 2) if contract_value else None,
        "remaining_ar": round(remaining_ar, 2) if remaining_ar is not None else None,
        "is_splitit": False,  # Splitit is Whop-specific
        "is_claritypay": False,  # ClarityPay is Whop-specific
        "provider_fee_pct": 0.0,  # Stripe charges are face value — no financing fee
        "net_cash_collected": round(total_paid, 2),
        "plan_months_flag": False,  # Stripe deals are not QS Whop internal plans
        "first_payment_date": first_payment_date,
        "total_installments": payment_count,
    }


async def _run_stripe_pass(
    session,
    won_deals: list,
    contact_cache: dict,
) -> dict:
    """Second pass: match unmatched deals via Stripe and enrich existing matches.

    Runs after the Whop pass. For each deal:
    1. If unmatched → try Stripe contactId match → email match → full upsert
    2. If matched but missing payment data → fill from Stripe charges (NULL fields only)

    Returns stats: {stripe_matched, stripe_enriched, stripe_errors}
    """
    from db.queries.deal_matches import enrich_deal_match_payments, get_existing_match

    stats = {"stripe_matched": 0, "stripe_enriched": 0, "stripe_errors": 0}

    if not settings.stripe_secret_key:
        logger.info("STRIPE_SECRET_KEY not set — skipping Stripe pass")
        return stats

    logger.info("=== Stripe Enrichment Pass: start ===")

    async with httpx.AsyncClient(timeout=30.0) as stripe_client:
        ghl_map, email_map = await _build_stripe_index(stripe_client)

        for deal in won_deals:
            try:
                existing = await get_existing_match(session, deal.ghl_opportunity_id)

                contact = contact_cache.get(deal.ghl_contact_id or "") or {}
                ghl_email = contact.get("email", "").lower().strip()
                ghl_name = contact.get("name") or deal.opportunity_name or ""
                ghl_cid = deal.ghl_contact_id or ""

                # For confirmed rows, only proceed if payment metrics are missing
                if existing and existing.is_confirmed:
                    if existing.total_paid is not None and float(existing.total_paid or 0) > 0:
                        continue  # Already has payment data — skip entirely

                    # Try to find Stripe charges using the stored whop_email
                    # (which holds the Stripe customer email for manual_stripe matches)
                    confirmed_email = (existing.whop_email or "").lower().strip()
                    matched_charges: list[dict] = []
                    match_method_stripe = ""

                    if ghl_cid and ghl_cid in ghl_map:
                        matched_charges = ghl_map[ghl_cid]
                        match_method_stripe = "stripe_contactid"
                    if not matched_charges and confirmed_email and confirmed_email in email_map:
                        matched_charges = email_map[confirmed_email]
                        match_method_stripe = "stripe_email_confirmed"
                    if not matched_charges and ghl_email and ghl_email in email_map:
                        matched_charges = email_map[ghl_email]
                        match_method_stripe = "stripe_email_exact"

                    if matched_charges:
                        metrics = _compute_stripe_payment_metrics(
                            matched_charges, float(deal.monetary_value or 0)
                        )
                        if metrics:
                            enriched = await enrich_deal_match_payments(
                                session, deal.ghl_opportunity_id, metrics
                            )
                            if enriched:
                                stats["stripe_enriched"] += 1
                                logger.info(
                                    f"Stripe ENRICHED confirmed match {deal.ghl_opportunity_id}: "
                                    f"filled from {len(matched_charges)} charges"
                                )
                    continue

                # Find Stripe charges for this deal
                matched_charges: list[dict] = []
                match_method_stripe = ""

                # Priority 1: GHL contact ID → Stripe metadata
                if ghl_cid and ghl_cid in ghl_map:
                    matched_charges = ghl_map[ghl_cid]
                    match_method_stripe = "stripe_contactid"

                # Priority 2: Email exact match
                if not matched_charges and ghl_email and ghl_email in email_map:
                    matched_charges = email_map[ghl_email]
                    match_method_stripe = "stripe_email_exact"

                if not matched_charges:
                    continue

                metrics = _compute_stripe_payment_metrics(
                    matched_charges, float(deal.monetary_value or 0)
                )
                if not metrics:
                    continue

                is_unmatched = (
                    not existing
                    or existing.match_confidence == "unmatched"
                )

                if is_unmatched:
                    # Full match — previously unmatched, now matched via Stripe
                    close_dt = deal.close_date
                    close_date = close_dt.date() if hasattr(close_dt, "date") else close_dt

                    stripe_email = next(
                        (ch["_resolved_email"] for ch in matched_charges
                         if ch.get("_resolved_email")),
                        "",
                    )
                    stripe_name = next(
                        (ch.get("_resolved_customer", {}).get("name", "")
                         for ch in matched_charges
                         if ch.get("_resolved_customer", {}).get("name")),
                        "",
                    )

                    record: dict = {
                        "ghl_opportunity_id": deal.ghl_opportunity_id,
                        "ghl_close_date": close_date,
                        "ghl_opportunity_name": deal.opportunity_name,
                        "ghl_owner_name": deal.opportunity_owner_name,
                        "ghl_contact_id": deal.ghl_contact_id,
                        "ghl_contact_email": ghl_email or None,
                        "ghl_contact_name": ghl_name or None,
                        "ghl_monetary_value": (
                            float(deal.monetary_value) if deal.monetary_value else None
                        ),
                        "ghl_cash_collected": (
                            float(deal.cash_collected) if deal.cash_collected else None
                        ),
                        "match_confidence": "high",
                        "match_score": (
                            1.0 if match_method_stripe == "stripe_contactid" else 0.95
                        ),
                        "match_method": match_method_stripe,
                        # Stripe customer email/name — displayed in whop_email/name columns
                        "whop_email": stripe_email or None,
                        "whop_name": stripe_name or None,
                    }
                    record.update(metrics)
                    await upsert_deal_match(session, record)
                    stats["stripe_matched"] += 1
                    logger.info(
                        f"Stripe MATCHED {deal.ghl_opportunity_id} via "
                        f"{match_method_stripe}: {len(matched_charges)} charges, "
                        f"total=${metrics.get('total_paid', 0)}"
                    )
                else:
                    # Enrichment — fill missing payment data from Stripe
                    enriched = await enrich_deal_match_payments(
                        session, deal.ghl_opportunity_id, metrics
                    )
                    if enriched:
                        stats["stripe_enriched"] += 1
                        logger.info(
                            f"Stripe ENRICHED {deal.ghl_opportunity_id}: "
                            f"filled from {len(matched_charges)} charges"
                        )

            except Exception as exc:
                logger.error(
                    f"Stripe error on {deal.ghl_opportunity_id}: {exc}",
                    exc_info=True,
                )
                stats["stripe_errors"] += 1

    logger.info(f"=== Stripe pass complete: {stats} ===")
    return stats


# ── Main engine ──────────────────────────────────────────────────────────────

async def run_matching() -> dict:
    """Run GHL↔Whop matching for all closed-won deals. Called by FastAPI route.

    Returns stats dict: {matched_high, matched_medium, matched_low, unmatched,
                          skipped_confirmed, errors, total}
    """
    logger.info("=== Deal Matching Engine: start ===")

    stats = {
        "matched_high": 0,
        "matched_medium": 0,
        "matched_low": 0,
        "unmatched": 0,
        "skipped_confirmed": 0,
        "errors": 0,
        "total": 0,
    }

    async with AsyncSessionLocal() as session:
        # ── Step 1: Load closed-won deals from DB ───────────────────────────
        won_deals = await get_won_deals(session)
        stats["total"] = len(won_deals)
        logger.info(f"Found {len(won_deals)} closed-won deals")

        if not won_deals:
            return stats

        # ── Step 2: Fetch GHL contact emails (one API call per unique contact)
        ghl_client = GHLClient()
        contact_cache: dict[str, dict] = {}

        logger.info("Fetching GHL contact emails...")
        unique_contact_ids = {
            d.ghl_contact_id for d in won_deals if d.ghl_contact_id
        }
        for contact_id in unique_contact_ids:
            contact = await ghl_client.get_contact(contact_id)
            if contact:
                contact_cache[contact_id] = {
                    "email": (contact.get("email") or "").lower().strip(),
                    "name": (
                        contact.get("name")
                        or f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip()
                    ),
                }
            await asyncio.sleep(0.12)  # ~8 req/s — stay inside GHL rate limit

        logger.info(f"Resolved {len(contact_cache)}/{len(unique_contact_ids)} contacts")

        # ── Step 3: Fetch all Whop memberships ─────────────────────────────
        logger.info("Fetching Whop memberships...")
        async with httpx.AsyncClient(timeout=30.0) as whop_client:
            memberships = await _fetch_whop_memberships(whop_client)
            logger.info(f"Fetched {len(memberships)} Whop memberships")

            # ── Step 4: Match each deal ─────────────────────────────────────
            for deal in won_deals:
                try:
                    result = await _match_one_deal(
                        session, deal, contact_cache, memberships, whop_client
                    )
                    stats[result] = stats.get(result, 0) + 1
                except Exception as exc:
                    logger.error(
                        f"Error matching {deal.ghl_opportunity_id}: {exc}", exc_info=True
                    )
                    stats["errors"] += 1

        # ── Step 5: Stripe enrichment pass ─────────────────────────────
        stripe_stats = await _run_stripe_pass(session, won_deals, contact_cache)
        stats.update(stripe_stats)

    logger.info(f"=== Matching complete: {stats} ===")
    return stats


async def _match_one_deal(
    session,
    deal,
    contact_cache: dict,
    memberships: list[dict],
    whop_client: httpx.AsyncClient,
) -> str:
    """Match a single deal and upsert result. Returns stats key."""
    from db.queries.deal_matches import get_existing_match  # local to avoid circular

    # Idempotency gate — never overwrite a confirmed match's identity,
    # but still enrich payment metrics if they're missing.
    existing = await get_existing_match(session, deal.ghl_opportunity_id)
    if existing and existing.is_confirmed:
        # Enrich payment metrics for confirmed Whop matches
        if existing.whop_membership_id and (
            existing.total_paid is None or float(existing.total_paid or 0) == 0
        ):
            from db.queries.deal_matches import enrich_deal_match_payments
            try:
                payments = await _fetch_membership_payments(
                    whop_client, existing.whop_membership_id
                )
                if payments:
                    metrics = _compute_payment_metrics(
                        payments, float(deal.monetary_value or 0),
                        installments_override=existing.total_installments,
                    )
                    enriched = await enrich_deal_match_payments(
                        session, deal.ghl_opportunity_id, metrics
                    )
                    if enriched:
                        logger.info(
                            f"Enriched confirmed Whop match {deal.ghl_opportunity_id} "
                            f"({existing.whop_membership_id}): {metrics.get('total_paid')}"
                        )
            except Exception as exc:
                logger.warning(
                    f"Failed to enrich confirmed match {deal.ghl_opportunity_id}: {exc}"
                )
        return "skipped_confirmed"

    contact = contact_cache.get(deal.ghl_contact_id or "") or {}
    ghl_email = contact.get("email", "")
    ghl_name = contact.get("name") or deal.opportunity_name or ""

    # Close date for time-window search
    close_dt = deal.close_date
    if not close_dt:
        return "unmatched"
    # close_date is a datetime (with tz) from the DB — normalise to date
    close_date = close_dt.date() if hasattr(close_dt, "date") else close_dt

    # ── Step A: Email-exact match (no time window) ─────────────────────────
    # If the GHL email matches a Whop email exactly, that's definitive
    # regardless of how far apart the dates are.
    best_score = 0.0
    best_m: dict | None = None
    best_method = "none"

    if ghl_email:
        for m in memberships:
            w_email, _ = _extract_whop_identity(m)
            if w_email and w_email == ghl_email.lower().strip():
                score, method = score_match(ghl_email, ghl_name, w_email, _)
                if score > best_score:
                    best_score = score
                    best_m = m
                    best_method = method

    # ── Step B: Time-windowed fuzzy search (only if no exact match) ───────
    # Name/fuzzy candidates use the tight ±MATCH_WINDOW_DAYS to avoid false positives.
    # A candidate sharing the exact corporate (non-personal) domain is high-signal, so it
    # gets the wider ±DOMAIN_MATCH_WINDOW_DAYS — a company payer can sign up on Whop days
    # or weeks apart from when the rep marks the GHL deal Won.
    if not best_m:
        ghl_domain = ghl_email.split("@")[1] if "@" in ghl_email else ""
        ghl_corporate = bool(ghl_domain) and ghl_domain not in PERSONAL_DOMAINS

        candidates: list[tuple[dict, int]] = []  # (membership, days_diff)
        for m in memberships:
            created_raw = m.get("created_at")
            if not created_raw:
                continue
            try:
                if isinstance(created_raw, (int, float)):
                    m_date = datetime.fromtimestamp(created_raw, tz=timezone.utc).date()
                else:
                    m_date = datetime.fromisoformat(
                        str(created_raw).replace("Z", "+00:00")
                    ).date()
            except (ValueError, AttributeError, OSError):
                continue

            days_diff = (m_date - close_date).days
            w_email, _ = _extract_whop_identity(m)
            w_domain = w_email.split("@")[1] if "@" in w_email else ""
            same_corp_domain = ghl_corporate and w_domain == ghl_domain
            window = DOMAIN_MATCH_WINDOW_DAYS if same_corp_domain else MATCH_WINDOW_DAYS
            if abs(days_diff) <= window:
                candidates.append((m, days_diff))

        logger.info(
            f"Deal {deal.ghl_opportunity_id} ({ghl_email or 'no-email'}): "
            f"close={close_date}, corporate={ghl_corporate}, candidates={len(candidates)}"
        )

        best_days: int | None = None
        for m, days_diff in candidates:
            w_email, w_name = _extract_whop_identity(m)
            score, method = score_match(ghl_email, ghl_name, w_email, w_name)
            logger.info(
                f"  candidate {m.get('id')} ({w_email}, {days_diff:+d}d): "
                f"score={score:.2f} method={method}"
            )
            # Higher score wins; on a tie, the membership closest to the close date wins
            # (matters now that same-domain candidates share a wide window).
            better = score > best_score or (
                score == best_score
                and best_days is not None
                and abs(days_diff) < abs(best_days)
            )
            if better:
                best_score = score
                best_m = m
                best_method = method
                best_days = days_diff

    if best_m:
        logger.info(
            f"Deal {deal.ghl_opportunity_id}: best match={best_m.get('id')} "
            f"score={best_score:.2f} method={best_method}"
        )

    confidence = classify_confidence(best_score)

    # ── Build the record ──────────────────────────────────────────────────
    record: dict = {
        "ghl_opportunity_id": deal.ghl_opportunity_id,
        "ghl_close_date": close_date,
        "ghl_opportunity_name": deal.opportunity_name,
        "ghl_owner_name": deal.opportunity_owner_name,
        "ghl_contact_id": deal.ghl_contact_id,
        "ghl_contact_email": ghl_email or None,
        "ghl_contact_name": ghl_name or None,
        "ghl_monetary_value": float(deal.monetary_value) if deal.monetary_value else None,
        "ghl_cash_collected": float(deal.cash_collected) if deal.cash_collected else None,
        "match_confidence": confidence,
        "match_score": round(best_score, 3),
        "match_method": best_method,
    }

    if best_m:
        w_email, w_name = _extract_whop_identity(best_m)
        # Whop v2: "product" and "plan" are string IDs, not embedded dicts.
        # Guard: if they happen to be dicts in a future API version, extract .get("id").
        _plan = best_m.get("plan")
        _product = best_m.get("product")

        # Whop v2 returns created_at as a Unix timestamp (int).
        # DB column is DateTime(timezone=True) — must convert.
        _created_raw = best_m.get("created_at")
        if isinstance(_created_raw, (int, float)):
            _whop_created_dt = datetime.fromtimestamp(_created_raw, tz=timezone.utc)
        elif _created_raw:
            _whop_created_dt = datetime.fromisoformat(
                str(_created_raw).replace("Z", "+00:00")
            )
        else:
            _whop_created_dt = None

        record.update({
            "whop_membership_id": best_m.get("id"),
            "whop_email": w_email or None,
            "whop_name": w_name or None,
            "whop_product_id": _product if isinstance(_product, str) else (_product or {}).get("id"),
            "whop_plan_name": _plan if isinstance(_plan, str) else (_plan or {}).get("name"),
            "whop_created_at": _whop_created_dt,
        })

        # Fetch payment metrics for HIGH and MEDIUM matches only
        if confidence in ("high", "medium") and best_m.get("id"):
            payments = await _fetch_membership_payments(whop_client, best_m["id"])
            # split_pay_required_payments = authoritative plan length (set at membership
            # creation for QS internal financing plans). Passed so total_installments and
            # plan_months_flag derive from it, not from len(payments) which under-counts.
            split_pay_count = best_m.get("split_pay_required_payments")
            metrics = _compute_payment_metrics(
                payments, float(deal.monetary_value or 0),
                installments_override=split_pay_count,
            )
            record.update(metrics)
            if split_pay_count:
                logger.info(
                    f"  split_pay_required_payments={split_pay_count} "
                    f"→ total_installments for {best_m.get('id')}"
                )

    await upsert_deal_match(session, record)

    return f"matched_{confidence}" if confidence != "unmatched" else "unmatched"
