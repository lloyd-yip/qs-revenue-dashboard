"""Shared Xero API constants and stateless helpers used across xero_* routers."""

import json
import logging
import urllib.request

from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from config import settings

logger = logging.getLogger(__name__)

XERO_TENANT_ID = "3bead22e-28ff-4eb1-92cd-9b9d648e188a"

# ECB monthly average EUR/USD rates — extend when syncing new months
EUR_USD_RATES: dict[str, float] = {
    "2025-10": 1.1630,
    "2025-11": 1.1560,
    "2025-12": 1.1709,
    "2026-01": 1.1738,
    "2026-02": 1.1824,
    "2026-03": 1.1558,
    "2026-04": 1.1706,
    "2026-05": 1.1729,
}

_bearer_scheme = HTTPBearer(auto_error=False)


async def verify_bearer(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer_scheme),
) -> None:
    """Raise 401 if the bearer token does not match settings.api_bearer_token."""
    if credentials is None or credentials.credentials != settings.api_bearer_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing bearer token.",
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_eur_usd_rate(year: int, month: int) -> float:
    """Return the ECB monthly average EUR/USD rate for the given month.

    Uses hardcoded table for known months; falls back to Frankfurter API for future months.
    """
    import calendar

    key = f"{year}-{month:02d}"
    if key in EUR_USD_RATES:
        return EUR_USD_RATES[key]

    last_day = calendar.monthrange(year, month)[1]
    start = f"{year}-{month:02d}-01"
    end   = f"{year}-{month:02d}-{last_day:02d}"
    url   = f"https://api.frankfurter.dev/{start}..{end}?from=EUR&to=USD"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
        usd_values = [v["USD"] for v in data.get("rates", {}).values() if "USD" in v]
        if usd_values:
            rate = round(sum(usd_values) / len(usd_values), 4)
            logger.info("Frankfurter EUR/USD %s: %.4f (avg %d days)", key, rate, len(usd_values))
            return rate
    except Exception as exc:
        logger.warning("Frankfurter rate fetch failed for %s: %s — using 1.10 fallback", key, exc)
    return 1.10
