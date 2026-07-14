"""Per-run contact cache — deduplicates the expensive per-contact GHL fetches.

Many opportunities share the same ``contactId``. Without caching, every opportunity
independently re-fetches that contact's appointments, contact record, and notes — the
dominant cost of a sync (~2-3 GHL calls per opportunity, thousands of redundant calls
per run). This cache memoizes each of the three fetches keyed by ``contact_id`` and
coalesces concurrent in-flight fetches (the concurrent builders await a single shared
task per contact) so the same contact is fetched at most once per run.

Scope is ONE sync run: instantiate a fresh ``ContactCache(ghl_client)`` inside
``run_sync()`` so the cache never leaks stale data across runs. It duck-types the three
``GHLClient`` methods it wraps, so it can be passed anywhere those methods are called.
"""

import asyncio
import logging
from typing import Awaitable, Callable, TypeVar

from sync.ghl_client import GHLClient

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ContactCache:
    """Memoizes get_contact_appointments / get_contact / get_contact_notes per run."""

    def __init__(self, ghl_client: GHLClient) -> None:
        self._client = ghl_client
        # contact_id -> in-flight/completed asyncio task holding the fetch result
        self._appointments: dict[str, asyncio.Task] = {}
        self._contacts: dict[str, asyncio.Task] = {}
        self._notes: dict[str, asyncio.Task] = {}

    @staticmethod
    async def _memoize(
        store: dict[str, asyncio.Task],
        key: str,
        factory: Callable[[], Awaitable[T]],
    ) -> T:
        """Return the shared task for ``key``, creating it once. Concurrent callers
        for the same key await the same task, so the fetch happens exactly once.

        A failed fetch is NOT cached: the key is evicted so a later opportunity for the
        same contact can retry (matches the pre-cache per-opportunity retry behavior).
        """
        task = store.get(key)
        if task is None:
            task = asyncio.ensure_future(factory())
            store[key] = task
        try:
            return await task
        except Exception:
            # Don't poison the whole run's opps for this contact on a transient error.
            store.pop(key, None)
            raise

    async def get_contact_appointments(self, contact_id: str) -> list[dict]:
        return await self._memoize(
            self._appointments, contact_id,
            lambda: self._client.get_contact_appointments(contact_id),
        )

    async def get_contact(self, contact_id: str) -> dict | None:
        return await self._memoize(
            self._contacts, contact_id,
            lambda: self._client.get_contact(contact_id),
        )

    async def get_contact_notes(self, contact_id: str) -> list[dict]:
        return await self._memoize(
            self._notes, contact_id,
            lambda: self._client.get_contact_notes(contact_id),
        )

    def stats(self) -> dict[str, int]:
        """Unique-contact fetch counts — for logging cache effectiveness."""
        return {
            "unique_appointment_fetches": len(self._appointments),
            "unique_contact_fetches": len(self._contacts),
            "unique_notes_fetches": len(self._notes),
        }
