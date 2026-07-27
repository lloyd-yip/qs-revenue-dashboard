"""Async Retell AI client — lists voice calls (with recordings/transcripts) via the v2 API.

Retell telephony is external to GHL, so this is the sole source of call volume, minutes,
and recordings. Keyed off the API key stored in app_settings (Settings → Connectors →
Retell). Modeled on sync/ghl_client.py.
"""

import logging
from typing import AsyncIterator

import httpx

from api.utils.retell_utils import RETELL_API_BASE

logger = logging.getLogger(__name__)


class RetellClient:
    """Async Retell API client. Instantiate with a resolved API key (get_retell_config)."""

    def __init__(self, api_key: str) -> None:
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        self._base = RETELL_API_BASE

    async def list_calls(
        self,
        limit_per_page: int = 1000,
        max_calls: int | None = None,
        after_ms: int | None = None,
    ) -> AsyncIterator[dict]:
        """Yield raw Retell call dicts, newest first, paginating via pagination_key.

        after_ms: only calls with start_timestamp >= this (epoch ms) — for incremental pulls.
        """
        collected = 0
        pagination_key: str | None = None
        async with httpx.AsyncClient(timeout=60.0) as client:
            while True:
                body: dict = {"sort_order": "descending", "limit": limit_per_page}
                if pagination_key:
                    body["pagination_key"] = pagination_key
                if after_ms:
                    body["filter_criteria"] = {
                        "start_timestamp": {"lower_threshold": after_ms}
                    }
                resp = await client.post(
                    f"{self._base}/v2/list-calls", headers=self._headers, json=body
                )
                resp.raise_for_status()
                payload = resp.json()
                # v2 returns a bare list; be defensive about a wrapped shape.
                calls = payload if isinstance(payload, list) else payload.get("calls", [])
                if not calls:
                    break
                for call in calls:
                    yield call
                    collected += 1
                    if max_calls and collected >= max_calls:
                        return
                if len(calls) < limit_per_page:
                    break
                pagination_key = calls[-1].get("call_id")
                if not pagination_key:
                    break

    async def list_agents(self) -> dict[str, str]:
        """Return {agent_id: agent_name} for naming the per-agent breakdown. {} on failure."""
        async with httpx.AsyncClient(timeout=15.0) as client:
            try:
                resp = await client.get(f"{self._base}/list-agents", headers=self._headers)
                resp.raise_for_status()
                data = resp.json()
                agents = data if isinstance(data, list) else data.get("agents", [])
                return {a.get("agent_id"): a.get("agent_name") for a in agents if a.get("agent_id")}
            except httpx.HTTPError as exc:
                logger.warning("Retell list_agents failed: %s", exc)
                return {}

    async def get_call(self, call_id: str) -> dict | None:
        """Fetch a single call (used to refresh an expired recording URL). None on failure."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                resp = await client.get(
                    f"{self._base}/v2/get-call/{call_id}", headers=self._headers
                )
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPError as exc:
                logger.warning("Retell get_call %s failed: %s", call_id, exc)
                return None
