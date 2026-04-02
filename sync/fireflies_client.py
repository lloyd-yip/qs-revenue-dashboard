"""Fireflies GraphQL API client — transcript fetching for appointment resolution."""

import logging
from datetime import date

import httpx

from config import settings

logger = logging.getLogger(__name__)

FIREFLIES_API_URL = "https://api.fireflies.ai/graphql"

# Minimum sentences spoken by the prospect to count the call as "Showed"
MIN_PROSPECT_SENTENCES = 5


class FirefliesClient:
    def __init__(self) -> None:
        self._headers = {
            "Authorization": f"Bearer {settings.fireflies_api_key}",
            "Content-Type": "application/json",
        }

    async def get_transcripts_for_date(
        self,
        target_date: date,
        organizer_email: str,
    ) -> list[dict]:
        """Return transcript metadata for a specific date filtered to one organizer.

        Fireflies fromDate/toDate are inclusive ISO date strings.
        """
        date_str = target_date.isoformat()
        query = """
        query GetTranscripts($fromDate: String, $toDate: String) {
            transcripts(fromDate: $fromDate, toDate: $toDate) {
                id
                title
                duration
                organizer_email
            }
        }
        """
        result = await self._query(query, {"fromDate": date_str, "toDate": date_str})
        transcripts = (result.get("data") or {}).get("transcripts") or []
        return [
            t for t in transcripts
            if (t.get("organizer_email") or "").lower() == organizer_email.lower()
        ]

    async def get_sentences(self, transcript_id: str) -> list[dict]:
        """Return sentences for a transcript. Each sentence has speaker_name and text."""
        query = """
        query GetTranscript($transcriptId: String!) {
            transcript(id: $transcriptId) {
                sentences {
                    speaker_name
                    text
                }
            }
        }
        """
        result = await self._query(query, {"transcriptId": transcript_id})
        transcript = (result.get("data") or {}).get("transcript") or {}
        return transcript.get("sentences") or []

    async def _query(self, query: str, variables: dict) -> dict:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                FIREFLIES_API_URL,
                headers=self._headers,
                json={"query": query, "variables": variables},
            )
            response.raise_for_status()
            return response.json()
