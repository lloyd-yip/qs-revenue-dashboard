"""GHL API v2 client — handles authentication, pagination, and rate limiting."""

import asyncio
import logging
from datetime import datetime, timezone
from typing import AsyncGenerator

import httpx

from config import settings

logger = logging.getLogger(__name__)

# Excluded pipeline stage IDs — never returned as usable records
EXCLUDED_STAGE_IDS = {
    "c2315e44-4992-49e6-a2da-f177c884838e",  # Duplicates
    "8ffe9c93-0dc9-4a36-8241-f1252c6a425d",  # Application, No Booking
}

# Stage IDs that start with "Temp Ryan" — identified by name check at sync time
TEMP_RYAN_STAGE_PREFIX = "Temp Ryan"

# "Showed" pipeline stage IDs — used as secondary show signal
SHOWED_STAGE_IDS = {
    "45a0608f-7648-4509-8f3a-d93b21cc9d41",  # 1st Call Done
    "10e6b1ef-0685-4f73-b3c7-b5006b7bc311",  # 2nd Call Done (In Prog)
    "544b178f-d1f2-4186-a8c2-00c3b0eeefe8",  # Deal Won
    "80cba97d-2f60-4485-8953-4b9569b1ddc1",  # Deal Lost
    "38aac258-cb3d-447a-828c-03b623ee5d05",  # FU Call Ghost
    # Disqualified intentionally excluded — DQ can happen after a no-show,
    # so stage alone is not a reliable "showed" signal. Use call1_appointment_status.
}

# Upcoming 1st Call Booked stage ID — used for compliance flag detection
UPCOMING_1ST_CALL_BOOKED_STAGE_ID = "e82907fd-4d76-4c1a-a867-b82c1093a88d"

# Deal Won stage ID — used for close rate / units closed metrics
DEAL_WON_STAGE_ID = "544b178f-d1f2-4186-a8c2-00c3b0eeefe8"

# Disqualified stage ID
DISQUALIFIED_STAGE_ID = "62448525-88ab-4e82-b414-b6880e69e2de"

# 1st Call No-Show / Cancelled stage IDs
NO_SHOW_STAGE_ID = "1201d3c3-166e-4c01-90b5-7f02e02a77c4"
CANCELLED_STAGE_ID = "b9624f39-9697-418c-864b-bd28c1db6182"

# Opportunity custom field IDs
CUSTOM_FIELD_IDS = {
    "call1_appointment_status": "V82ErbW24izA5aQUzRUv",
    "call2_appointment_status": "WMj5zj7G8wBTtp3OqjKp",
    "call1_initial_appointment_date": "We5c2Oiz8kC3FgjOO2XD",
    "call1_appointment_date": "bFDWu3koncdxn26h6nAm",
    "call2_appointment_date": "oRRLUFWNYEeYSDVqV3DK",
    "lead_quality": "M8RuTSXsLhZMvdMWAlLr",
    "financial_qual": "BLtbMbfQhd0ODu7ywNIu",
    "intent_to_transform": "IY2SCImbFeg5qkGRpCmy",
    "pre_call_indoctrination": "ogT4HksPoylcBN7vNgtX",
    "business_fit": "WugaBcJwKZzXaxrXlGg4",
    "pain_goal_oriented": "WJddOo1awmnVDVlKgf8Q",
    "dq_reason": "zVSqT9ogJzXIBUi49F1F",
    "deal_lost_reasons": "PDM9cXyNljhX9qeQpSAH",
    "business_industry": "fyYxLA4EvjZpifanMBm2",
    "current_revenue": "E2xd173q56x3GB5m1qm1",
    "lead_source_a": "D6fhQo9zfk53LHMCksye",
    "op_book_campaign_source": "siKjWZIScNTHSk38LJqt",
    "op_book_campaign_medium": "itX1JvyAAUtxbHuXtMOB",
    "op_book_campaign_name": "MYzEZQzFw8G42mrTJVKy",
    "projected_deal_size": "Oo9ktilF7QwTNBzksT3k",
}

# Reverse lookup: field ID → our internal key name
FIELD_ID_TO_KEY = {v: k for k, v in CUSTOM_FIELD_IDS.items()}


class GHLClient:
    """Async GHL API v2 client scoped to a single location."""

    def __init__(self) -> None:
        self._headers = {
            "Authorization": f"Bearer {settings.ghl_api_key}",
            "Version": "2021-07-28",
            "Accept": "application/json",
        }
        self._base_url = settings.ghl_api_base_url
        self._location_id = settings.ghl_location_id
        self._pipeline_id = settings.ghl_pipeline_id
        self._page_delay_s = settings.ghl_page_delay_ms / 1000.0

    async def get_users(self) -> dict[str, str]:
        """Fetch all users for this location. Returns {user_id: name} map.

        Called once at sync start to resolve assignedTo IDs to names.
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                data = await self._get(client, "/users/", {"locationId": self._location_id})
                return {u["id"]: u.get("name", "") for u in data.get("users", [])}
            except Exception as exc:
                logger.warning("Failed to fetch users: %s", exc)
                return {}

    async def get_calendars(self) -> dict[str, str]:
        """Fetch all calendars for this location. Returns {calendar_id: name} map.

        Used to resolve calendar IDs from appointment data to human-readable names.
        Falls back to empty dict if the token lacks calendars scope.
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                data = await self._get(client, "/calendars/", {"locationId": self._location_id})
                calendars = data.get("calendars", [])
                return {c["id"]: c.get("name", "") for c in calendars}
            except Exception as exc:
                logger.warning("Failed to fetch calendars: %s — check PIT scopes include 'calendars.readonly'", exc)
                return {}

    async def get_pipeline_stages(self) -> dict[str, str]:
        """Fetch all pipeline stages for this location. Returns {stage_id: stage_name} map.

        Used to resolve stage IDs to human-readable names dynamically.
        Falls back to empty dict if the token lacks opportunities scope.
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                data = await self._get(
                    client, "/opportunities/pipelines", {"locationId": self._location_id}
                )
                stage_map = {}
                for pipeline in data.get("pipelines", []):
                    for stage in pipeline.get("stages", []):
                        stage_map[stage["id"]] = stage.get("name", "")
                return stage_map
            except Exception as exc:
                logger.warning("Failed to fetch pipeline stages: %s — check PIT scopes include 'opportunities.readonly'", exc)
                return {}

    async def get_user_email_map(self) -> dict[str, str]:
        """Fetch all users for this location. Returns {user_id: email} map.

        Used by appointment_resolver to match opportunity owner IDs to Fireflies
        organizer emails — covers any rep automatically without manual config.
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                data = await self._get(client, "/users/", {"locationId": self._location_id})
                return {u["id"]: u.get("email", "") for u in data.get("users", []) if u.get("email")}
            except Exception as exc:
                logger.warning("Failed to fetch user email map: %s", exc)
                return {}

    async def get_contact_appointments(self, contact_id: str) -> list[dict]:
        """Fetch all appointments for a contact. Returns empty list on any failure.

        Called during sync to read calendar appointment status (the field reps
        actually use) rather than the opportunity custom field.
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                data = await self._get(
                    client,
                    f"/contacts/{contact_id}/appointments",
                    {"locationId": self._location_id},
                )
                await asyncio.sleep(self._page_delay_s)
                # GHL returns appointments under "appointments" or "events"
                return data.get("appointments") or data.get("events") or []
            except Exception as exc:
                logger.warning("Failed to fetch appointments for contact %s: %s", contact_id, exc)
                return []

    async def update_appointment_status(self, appointment_id: str, status: str) -> None:
        """Update a calendar appointment status.

        status must be one of: 'showed' | 'noshow' | 'confirmed' | 'cancelled'
        """
        url = f"{self._base_url}/calendars/events/appointments/{appointment_id}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.put(
                url,
                headers=self._headers,
                json={"appointmentStatus": status},
            )
            response.raise_for_status()

    async def get_contact_notes(self, contact_id: str) -> list[dict]:
        """Fetch all notes for a contact. Returns empty list on any failure.

        Called during sync for showed opps only — one API call per contact.
        Includes the same page-delay as opportunity pagination for rate safety.
        """
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                data = await self._get(client, f"/contacts/{contact_id}/notes", {})
                await asyncio.sleep(self._page_delay_s)
                notes: list[dict] = data.get("notes", [])
            except Exception as exc:
                logger.warning("Failed to fetch notes for contact %s: %s", contact_id, exc)
                notes = []
        return notes

    async def _get(self, client: httpx.AsyncClient, path: str, params: dict) -> dict:
        """Single GET request with basic error handling."""
        url = f"{self._base_url}{path}"
        response = await client.get(url, headers=self._headers, params=params)
        response.raise_for_status()
        return response.json()

    async def stream_opportunities(
        self,
        updated_after: datetime | None = None,
    ) -> AsyncGenerator[dict, None]:
        """Async generator that yields raw GHL opportunity dicts one at a time.

        Args:
            updated_after: If set, only fetch opportunities updated after this timestamp.
                           If None, fetches all opportunities (full sync).
        """
        params: dict = {
            "location_id": self._location_id,
            "pipeline_id": self._pipeline_id,
            "limit": settings.ghl_page_size,
        }
        if updated_after:
            # GHL expects milliseconds epoch
            params["startAfter"] = int(updated_after.timestamp() * 1000)

        # GHL provides its own cursor values in meta — do NOT compute from last record.
        # The meta cursor may differ from the last record's id/updatedAt due to
        # GHL's internal sort logic. Using the wrong values causes cursor stall.
        cursor_id: str | None = None
        cursor_after: int | None = None
        page = 0

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                page += 1
                page_params = dict(params)
                if cursor_id and cursor_after is not None:
                    page_params["startAfterId"] = cursor_id
                    page_params["startAfter"] = cursor_after

                try:
                    data = await self._get(client, "/opportunities/search", page_params)
                except httpx.HTTPStatusError as exc:
                    logger.error("GHL API error on page %d: %s", page, exc.response.text)
                    raise
                except httpx.RequestError as exc:
                    logger.error("GHL network error on page %d: %s", page, exc)
                    raise

                opportunities = data.get("opportunities", [])
                if not opportunities:
                    break

                for opp in opportunities:
                    yield opp

                meta = data.get("meta", {})
                total = meta.get("total", 0)
                fetched_so_far = (page - 1) * settings.ghl_page_size + len(opportunities)
                logger.info("GHL sync: fetched %d / %d opportunities", fetched_so_far, total)

                cursor_id = meta.get("startAfterId")
                cursor_after = meta.get("startAfter")

                if not cursor_id or fetched_so_far >= total:
                    break

                # Rate limit protection
                await asyncio.sleep(self._page_delay_s)

    async def update_opportunity_custom_fields(
        self,
        opportunity_id: str,
        fields: list[dict],
    ) -> None:
        """Update custom fields on a GHL opportunity.

        fields: list of {"id": field_id, "field_value": value}
        GHL endpoint: PUT /opportunities/{id}
        """
        url = f"{self._base_url}/opportunities/{opportunity_id}"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.put(
                url,
                headers=self._headers,
                json={"customFields": fields},
            )
            response.raise_for_status()


def extract_custom_fields(opportunity: dict) -> dict:
    """Extract our tracked custom fields from a GHL opportunity payload.

    GHL returns custom fields as a list of objects. Field value keys vary by type:
      - String/dropdown fields: {"id": "...", "fieldValueString": "...", "type": "string"}
      - Date fields:            {"id": "...", "fieldValueDate": <ms_epoch_int>, "type": "date"}

    Returns a flat dict keyed by our internal field names. Date fields are returned
    as ISO 8601 strings so parse_ghl_datetime() in the normalizer can handle them.
    """
    result: dict = {}
    raw_fields = opportunity.get("customFields") or []

    for field in raw_fields:
        field_id = field.get("id", "")
        key = FIELD_ID_TO_KEY.get(field_id)
        if key:
            value = (
                field.get("fieldValueString")
                or field.get("fieldValue")
                or field.get("value")
                or None
            )
            if value is None and field.get("fieldValueDate"):
                value = datetime.fromtimestamp(field["fieldValueDate"] / 1000, tz=timezone.utc).isoformat()
            result[key] = value

    return result


def extract_attributions(opportunity: dict) -> dict:
    """Extract first-touch and last-touch UTM attribution from the attributions array."""
    attributions = opportunity.get("attributions") or []
    result: dict = {
        "attr_first_utm_source": None,
        "attr_first_utm_medium": None,
        "attr_first_utm_campaign": None,
        "attr_last_utm_source": None,
        "attr_last_utm_medium": None,
        "attr_last_utm_campaign": None,
    }

    if not attributions:
        return result

    def _pull(attr: dict) -> tuple[str | None, str | None, str | None]:
        return (
            attr.get("utmSource") or attr.get("utm_source") or None,
            attr.get("utmMedium") or attr.get("utm_medium") or None,
            attr.get("utmCampaign") or attr.get("utm_campaign") or attr.get("campaignName") or None,
        )

    first = attributions[0]
    result["attr_first_utm_source"], result["attr_first_utm_medium"], result["attr_first_utm_campaign"] = _pull(first)

    if len(attributions) > 1:
        last = attributions[-1]
        result["attr_last_utm_source"], result["attr_last_utm_medium"], result["attr_last_utm_campaign"] = _pull(last)
    else:
        result["attr_last_utm_source"] = result["attr_first_utm_source"]
        result["attr_last_utm_medium"] = result["attr_first_utm_medium"]
        result["attr_last_utm_campaign"] = result["attr_first_utm_campaign"]

    return result
