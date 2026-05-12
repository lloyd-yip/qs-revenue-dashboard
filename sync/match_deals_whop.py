"""GHL ↔ Whop Deal Reconciliation Engine.

Matches every GHL closed-won opportunity to a Whop membership using:
  1. Email domain match     — strongest signal (score 0.80)
  2. Exact email match      — perfect match (score 1.00)
  3. Fuzzy domain match     — similar domains / typos (score 0.50)
  4. Name similarity        — secondary signal (+0.25 / +0.12)
  5. ±3-day timing window   — only candidates created within 3 days of close

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
    personal_domains = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
                        "icloud.com", "me.com", "aol.com", "protonmail.com"}

    if ghl_domain and whop_domain:
        ghl_personal = ghl_domain in personal_domains
        whop_personal = whop_domain in personal_domains

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


def _compute_payment_metrics(payments: list[dict], ghl_monetary_value: float) -> dict:
    """Derive payment summary from raw Whop payment objects.

    Returns:
        total_paid, payment_count, is_financing, remaining_ar
    """
    paid = [
        p for p in payments
        if p.get("status") in ("paid", "complete", "completed")
    ]
    total_paid = sum(
        float(p.get("final_price") or p.get("price") or p.get("amount") or 0)
        for p in paid
    )
    payment_count = len(paid)
    contract_value = ghl_monetary_value or 0.0
    remaining_ar = max(contract_value - total_paid, 0.0) if contract_value else None
    is_financing = payment_count > 1 and bool(remaining_ar and remaining_ar > 1)

    return {
        "total_paid": round(total_paid, 2),
        "payment_count": payment_count,
        "is_financing": is_financing,
        "total_contract_value": round(contract_value, 2) if contract_value else None,
        "remaining_ar": round(remaining_ar, 2) if remaining_ar is not None else None,
    }


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

    # Idempotency gate — never touch a confirmed match
    existing = await get_existing_match(session, deal.ghl_opportunity_id)
    if existing and existing.is_confirmed:
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

    # ── Find candidates within ±MATCH_WINDOW_DAYS ─────────────────────────
    window_start = close_date - timedelta(days=MATCH_WINDOW_DAYS)
    window_end = close_date + timedelta(days=MATCH_WINDOW_DAYS)

    candidates: list[tuple[dict, int]] = []  # (membership, days_diff)
    for m in memberships:
        created_raw = m.get("created_at")
        if not created_raw:
            continue
        try:
            # Whop v2 returns created_at as a Unix timestamp (int).
            # Fall back to ISO string parsing for forward compatibility.
            if isinstance(created_raw, (int, float)):
                m_date = datetime.fromtimestamp(created_raw, tz=timezone.utc).date()
            else:
                m_date = datetime.fromisoformat(
                    str(created_raw).replace("Z", "+00:00")
                ).date()
        except (ValueError, AttributeError, OSError):
            continue
        if window_start <= m_date <= window_end:
            candidates.append((m, (m_date - close_date).days))

    logger.info(
        f"Deal {deal.ghl_opportunity_id} ({ghl_email or 'no-email'}): "
        f"close={close_date}, window=[{window_start}→{window_end}], "
        f"candidates={len(candidates)}"
    )

    # ── Score all candidates and pick the best ────────────────────────────
    best_score = 0.0
    best_m: dict | None = None
    best_method = "none"

    for m, _days_diff in candidates:
        w_email, w_name = _extract_whop_identity(m)
        score, method = score_match(ghl_email, ghl_name, w_email, w_name)
        logger.info(
            f"  candidate {m.get('id')} ({w_email}): score={score:.2f} method={method}"
        )
        if score > best_score:
            best_score = score
            best_m = m
            best_method = method

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
            metrics = _compute_payment_metrics(
                payments, float(deal.monetary_value or 0)
            )
            record.update(metrics)
            record["upfront_cash"] = float(deal.cash_collected or 0) or None

    await upsert_deal_match(session, record)

    return f"matched_{confidence}" if confidence != "unmatched" else "unmatched"
