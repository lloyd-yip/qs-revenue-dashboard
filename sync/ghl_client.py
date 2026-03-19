"""GHL API v2 client — handles authentication, pagination, and rate limiting."""

import asyncio
import logging
from datetime import datetime
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
    # Warm List, Hot List — IDs to be confirmed; match by name as fallback
    "544b178f-d1f2-4186-a8c2-00c3b0eeefe8",  # Deal Won
    "62448525-88ab-4e82-b414-b6880e69e2de",  # Disqualified
    "80cba97d-2f60-4485-8953-4b9569b1ddc1",  # Deal Lost
    "38aac258-cb3d-447a-828c-03b623ee5d05",  # FU Call Ghost
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

        last_id: str | None = None
        page = 0

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                page += 1
                page_params = dict(params)
                if last_id:
                    page_params["startAfterId"] = last_id

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

                last_id = opportunities[-1].get("id")
                total = data.get("meta", {}).get("total", 0)
                fetched_so_far = (page - 1) * settings.ghl_page_size + len(opportunities)
                logger.info("GHL sync: fetched %d / %d opportunities", fetched_so_far, total)

                if len(opportunities) < settings.ghl_page_size:
                    # Last page
                    break

                # Rate limit protection
                await asyncio.sleep(self._page_delay_s)

def extract_custom_fields(opportunity: dict) -> dict:
    """Extract our tracked custom fields from a GHL opportunity payload.

    GHL returns custom fields as a list:
      [{"id": "<field_id>", "value": "<value>", "fieldValue": "<value>"}, ...]

    Returns a flat dict keyed by our internal field names.
    """
    result: dict = {}
    raw_fields = opportunity.get("customFields") or []

    for field in raw_fields:
        field_id = field.get("id", "")
        key = FIELD_ID_TO_KEY.get(field_id)
        if key:
            # GHL uses both "value" and "fieldValue" depending on field type
            result[key] = field.get("fieldValue") or field.get("value") or None

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
