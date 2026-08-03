"""Wise API client with SCA (Strong Customer Authentication) support.

Wise requires RSA-signed requests for financial endpoints (PSD2 compliance).
The two-step flow:
  1. Make request → get 403 + x-2fa-approval OTP in response headers
  2. Sign the OTP with the private key → retry with X-2FA-Approval + X-Signature

Setup (one-time):
  1. Generate RSA key pair (already done — key in secrets-reference.md)
  2. Register PUBLIC key in Wise → Settings → Developer → Strong Customer Auth
  3. Store PRIVATE key as Railway env var: WISE_PRIVATE_KEY

Plain English: Wise uses a "challenge-response" auth on top of the API token.
The server says "prove you have the private key by signing this random string."
We sign it with our key, send it back, and Wise lets us in.

Verification — after key registration, run:
    python3 sync/wise_client.py
Should print incoming USD + EUR transactions for the last 6 months.

Silent failure signal: if step 2 still returns 403, the public key isn't
registered yet in Wise (or there's a key mismatch).
"""

import asyncio
import base64
import logging
import re
from datetime import datetime, timezone
from typing import Optional

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding as asym_padding

from config import settings

logger = logging.getLogger(__name__)

WISE_API_BASE = "https://api.wise.com"
WISE_PROFILE_ID = 17188420      # quantumSCALE Institute OÜ (business profile)
WISE_USD_BALANCE_ID = 15299186  # Primary USD balance
WISE_EUR_BALANCE_ID = 15299185  # Primary EUR balance
WISE_ACCOUNT_ID = 7668590       # Borderless account ID


class WiseSCARejected(Exception):
    """Raised when Wise rejects the SCA signature (x-2fa-approval-result: REJECTED).

    Means the public key registered in Wise → Developer → SCA does not match the
    private key we signed with (or none is registered) — distinct from a valid call
    that simply returns no transactions.
    """


# ── Activities feed (NO SCA required) ─────────────────────────────────────────
#
# The balance-statement endpoints require Wise SCA (RSA-signed 2FA). On this account
# SCA is unavailable (Wise rejects a provably-valid signature — a Wise-side issue), so
# we read incoming money from the Activities feed instead, which only needs the API
# token. Incoming credits are TRANSFER activities whose primaryAmount is wrapped in a
# <positive>…</positive> tag (that's how Wise marks money IN).

_AMOUNT_RE = re.compile(r"([\d,]+(?:\.\d+)?)\s*([A-Z]{3})")
_TAG_RE = re.compile(r"<[^>]+>")


def _strip_tags(s: str) -> str:
    return _TAG_RE.sub("", s or "").strip()


def _parse_primary_amount(primary: str) -> tuple[bool, Optional[float], Optional[str]]:
    """Return (is_incoming, amount, currency) from a Wise activity primaryAmount string.

    Incoming credits look like '<positive>+ 1,150 USD</positive>'; outgoing/plain do not
    carry the <positive> tag.
    """
    incoming = "<positive>" in (primary or "")
    text = _strip_tags(primary).replace("+", "")
    m = _AMOUNT_RE.search(text)
    if not m:
        return incoming, None, None
    return incoming, float(m.group(1).replace(",", "")), m.group(2)


async def fetch_wise_incoming_transfers(days: int = 365, max_pages: int = 40) -> list[dict]:
    """Fetch incoming (money-in) TRANSFER activities from Wise — no SCA needed.

    Resolves credentials from the in-app connector store (Settings → Connectors), falling
    back to env, then pages the Activities feed and keeps only incoming TRANSFER credits.

    Returns normalised dicts: wise_reference_number, date, amount, currency, sender_name,
    description, raw_type — the same shape the matcher/ingest expects.
    """
    from api.utils.wise_utils import get_wise_config

    cfg = await get_wise_config()
    if cfg.api_key:
        settings.wise_api_key = cfg.api_key
    if not settings.wise_api_key:
        logger.error("Wise API key not set — cannot fetch activities")
        return []

    cutoff = datetime.now(timezone.utc).date()
    from datetime import timedelta
    cutoff = cutoff - timedelta(days=days)

    out: list[dict] = []
    cursor: Optional[str] = None
    url = f"{WISE_API_BASE}/v1/profiles/{WISE_PROFILE_ID}/activities"
    async with httpx.AsyncClient(timeout=30.0) as client:
        for _ in range(max_pages):
            params = {"size": 50}
            if cursor:
                params["nextCursor"] = cursor
            r = await client.get(url, headers=_wise_headers(), params=params)
            if r.status_code != 200:
                logger.warning("Wise activities page failed %s: %s", r.status_code, r.text[:150])
                break
            data = r.json()
            activities = data.get("activities", [])
            reached_cutoff = False
            for a in activities:
                created = (a.get("createdOn") or "")[:10]
                try:
                    a_date = datetime.strptime(created, "%Y-%m-%d").date()
                except ValueError:
                    a_date = None
                if a_date and a_date < cutoff:
                    reached_cutoff = True
                    continue
                if a.get("type") != "TRANSFER":
                    continue
                incoming, amount, currency = _parse_primary_amount(a.get("primaryAmount"))
                if not incoming or not amount:
                    continue
                resource_id = (a.get("resource") or {}).get("id") or a.get("id", "")
                out.append({
                    "wise_reference_number": f"wise-{resource_id}"[:100],
                    "date": a_date,
                    "amount": amount,
                    "currency": currency or "USD",
                    "sender_name": _strip_tags(a.get("title")),
                    "description": _strip_tags(a.get("description")),
                    "raw_type": "CREDIT",
                })
            cursor = data.get("cursor")
            if not cursor or not activities or reached_cutoff:
                break
    logger.info("Wise activities: %d incoming transfers (last %dd)", len(out), days)
    return out


def _load_private_key():
    """Load the RSA private key from env var WISE_PRIVATE_KEY.

    The key is stored as a PEM string (with or without escaped newlines).
    Railway env vars store multi-line values — if it arrives with literal \\n,
    we normalise those back to real newlines.
    """
    raw = getattr(settings, "wise_private_key", None)
    if not raw:
        raise RuntimeError(
            "WISE_PRIVATE_KEY env var not set. "
            "Add the RSA private key to Railway environment variables."
        )
    # Normalise escaped newlines (Railway sometimes collapses them)
    pem = raw.replace("\\n", "\n").encode("utf-8")
    return serialization.load_pem_private_key(pem, password=None)


def _sign_otp(private_key, otp_token: str) -> str:
    """Sign the Wise OTP challenge with SHA-256/RSA → base64 string."""
    signature = private_key.sign(
        otp_token.encode("utf-8"),
        asym_padding.PKCS1v15(),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


def _wise_headers() -> dict:
    return {
        "Authorization": f"Bearer {settings.wise_api_key}",
        "accept": "application/json",
    }


async def _sca_get(
    client: httpx.AsyncClient,
    url: str,
    private_key,
    params: Optional[dict] = None,
) -> httpx.Response:
    """Make a Wise SCA-protected GET request.

    Automatically handles the two-step OTP challenge.
    Raises httpx.HTTPStatusError on non-200 after both steps.
    """
    headers = _wise_headers()

    # Step 1 — trigger the SCA challenge
    r1 = await client.get(url, headers=headers, params=params)
    if r1.status_code == 200:
        return r1  # No SCA required for this endpoint

    otp = r1.headers.get("x-2fa-approval")
    if r1.status_code == 403 and otp:
        logger.debug(f"SCA challenge received — OTP={otp[:8]}…")
        signature = _sign_otp(private_key, otp)
        headers_signed = {
            **headers,
            "X-2FA-Approval": otp,
            "X-Signature": signature,
        }
        r2 = await client.get(url, headers=headers_signed, params=params)
        if r2.status_code == 200:
            logger.debug("SCA challenge passed ✓")
            return r2
        result = r2.headers.get("x-2fa-approval-result", "")
        logger.error(
            f"SCA step 2 failed {r2.status_code} (result={result or 'n/a'}): {r2.text[:200]}. "
            "Check that the public key is registered in Wise → Settings → "
            "Developer → Strong Customer Authentication."
        )
        if result.upper() == "REJECTED" or r2.status_code == 403:
            raise WiseSCARejected(
                "Wise rejected the SCA signature — the registered public key does not "
                "match the app's private key (or none is registered)."
            )
        r2.raise_for_status()

    r1.raise_for_status()
    return r1  # unreachable but satisfies type checker


async def fetch_wise_transactions(
    interval_start: str,
    interval_end: str,
    currency: str = "USD",
) -> list[dict]:
    """Fetch all CREDIT (incoming) transactions from Wise for the given period.

    Args:
        interval_start: ISO 8601 e.g. "2026-01-01T00:00:00.000Z"
        interval_end:   ISO 8601 e.g. "2026-05-12T23:59:59.000Z"
        currency:       "USD" or "EUR"

    Returns:
        List of normalised transaction dicts with keys:
            wise_reference_number, date, amount, currency,
            sender_name, description, raw_type
    """
    balance_id = WISE_USD_BALANCE_ID if currency == "USD" else WISE_EUR_BALANCE_ID

    # Resolve credentials from the in-app connector store (Settings → Connectors), falling
    # back to env. Applied onto the settings singleton so the unchanged _wise_headers /
    # _load_private_key helpers pick them up — Wise creds are used nowhere else.
    from api.utils.wise_utils import get_wise_config
    _cfg = await get_wise_config()
    if _cfg.api_key:
        settings.wise_api_key = _cfg.api_key
    if _cfg.private_key:
        settings.wise_private_key = _cfg.private_key

    try:
        private_key = _load_private_key()
    except RuntimeError as e:
        logger.error(str(e))
        return []

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Primary: v1 balance-statements (the /v4/balances/{id}/statement.json path 404s).
        url_primary = (
            f"{WISE_API_BASE}/v1/profiles/{WISE_PROFILE_ID}"
            f"/balance-statements/{balance_id}/statement.json"
        )
        try:
            resp = await _sca_get(
                client, url_primary, private_key,
                params={
                    "intervalStart": interval_start,
                    "intervalEnd": interval_end,
                    "type": "COMPACT",
                }
            )
            data = resp.json()
            transactions = data.get("transactions", [])
            logger.info(
                f"Wise v1 {currency} statement: {len(transactions)} transactions "
                f"({interval_start[:10]} → {interval_end[:10]})"
            )
        except WiseSCARejected:
            # A rejected signature is a setup problem, not a transient/endpoint issue —
            # the v3 fallback would reject too. Surface it so callers can report clearly.
            raise
        except Exception as exc:
            # Fall back to v3 borderless statement
            logger.warning(f"v1 statement failed ({exc}), trying v3 borderless…")
            url_v3 = (
                f"{WISE_API_BASE}/v3/profiles/{WISE_PROFILE_ID}"
                f"/borderless-accounts/{WISE_ACCOUNT_ID}/statement.json"
            )
            try:
                resp = await _sca_get(
                    client, url_v3, private_key,
                    params={
                        "currency": currency,
                        "intervalStart": interval_start,
                        "intervalEnd": interval_end,
                    }
                )
                data = resp.json()
                transactions = data.get("transactions", [])
                logger.info(
                    f"Wise v3 {currency} statement: {len(transactions)} transactions"
                )
            except WiseSCARejected:
                raise
            except Exception as exc2:
                logger.error(f"Both Wise statement endpoints failed: {exc2}")
                return []

    # Filter to incoming only and normalise
    return [_normalise_transaction(t, currency) for t in transactions
            if t.get("type") == "CREDIT"]


def _normalise_transaction(raw: dict, currency: str) -> dict:
    """Flatten a raw Wise transaction object into a consistent dict.

    Wise v3 and v4 have slightly different shapes — this handles both.

    Key fields we care about for matching:
        - wise_reference_number: Wise's own transfer number (unique)
        - date: when the credit arrived
        - amount: how much (always positive for CREDIT)
        - sender_name: who sent it (best available — may be empty)
        - description: raw description / reference text from the sender
    """
    details = raw.get("details", {})
    amount_obj = raw.get("amount", {})

    # Reference number: v4 uses "referenceNumber", v3 embeds it in details
    ref_number = (
        raw.get("referenceNumber")
        or details.get("referenceNumber")
        or raw.get("id")
        or ""
    )

    # Date: ISO string or epoch
    raw_date = raw.get("date") or raw.get("createdAt") or ""
    if isinstance(raw_date, (int, float)):
        parsed_date = datetime.fromtimestamp(raw_date, tz=timezone.utc).date()
    elif raw_date:
        try:
            parsed_date = datetime.fromisoformat(
                str(raw_date).replace("Z", "+00:00")
            ).date()
        except ValueError:
            parsed_date = None
    else:
        parsed_date = None

    # Sender name: sometimes in details.senderName or details.paymentReference
    sender_name = (
        details.get("senderName")
        or details.get("payerName")
        or details.get("merchant", {}).get("name", "") if isinstance(details.get("merchant"), dict) else ""
        or raw.get("senderName")
        or ""
    ).strip()

    # Description / reference text (what the sender typed)
    description = (
        details.get("description")
        or details.get("paymentReference")
        or details.get("reference")
        or raw.get("details", "")
        if isinstance(raw.get("details"), str) else ""
        or ""
    ).strip()

    amount_val = (
        amount_obj.get("value")
        or raw.get("amount")
        or 0.0
    )

    return {
        "wise_reference_number": str(ref_number),
        "date": parsed_date,
        "amount": float(amount_val),
        "currency": amount_obj.get("currency", currency),
        "sender_name": sender_name,
        "description": description,
        "raw_type": raw.get("type", "CREDIT"),
        "_raw": raw,  # keep full raw for schema discovery on first run
    }


# ── CLI test ─────────────────────────────────────────────────────────────────

async def _test():
    """Run directly to verify SCA signing and inspect transaction data shape."""
    import json
    print("Fetching USD transactions (2026-01-01 → now)…")
    txns = await fetch_wise_transactions(
        "2026-01-01T00:00:00.000Z",
        "2026-05-12T23:59:59.000Z",
        currency="USD",
    )
    print(f"\n→ {len(txns)} incoming USD transactions\n")
    for t in txns[:5]:
        raw = t.pop("_raw", {})
        print(json.dumps(t, default=str, indent=2))
        print("  raw keys:", list(raw.keys()))
        print("---")

    print("\nFetching EUR transactions…")
    txns_eur = await fetch_wise_transactions(
        "2026-01-01T00:00:00.000Z",
        "2026-05-12T23:59:59.000Z",
        currency="EUR",
    )
    print(f"→ {len(txns_eur)} incoming EUR transactions")


if __name__ == "__main__":
    import os
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

    # For local testing, set env vars before running:
    #   export WISE_API_KEY=<your-key>
    #   export WISE_PRIVATE_KEY=$(cat /tmp/wise-sca/private_key.pem)
    # Or point to your local key file:
    if not os.getenv("WISE_PRIVATE_KEY"):
        key_path = "/tmp/wise-sca/private_key.pem"
        if os.path.exists(key_path):
            os.environ["WISE_PRIVATE_KEY"] = open(key_path).read()

    asyncio.run(_test())
