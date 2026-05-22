"""
Apollo People API Search client with rate limiting and retry logic.

Endpoint: POST https://api.apollo.io/api/v1/mixed_people/api_search
Credits:  FREE — this endpoint does not consume Apollo credits.
Rate limits: 200 req/min, 600 req/hour, 6000 req/day (plan-dependent).

Strategy: pace at 1 request/sec, track hourly count, exponential backoff on 429.
"""

import asyncio
import logging
import time
from typing import Any

import httpx

logger = logging.getLogger("discovery.apollo")

APOLLO_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"

_ARRAY_KEYS = [
    "person_titles",
    "person_seniorities",
    "person_locations",
    "organization_locations",
    "organization_num_employees_ranges",
    "contact_email_status",
    "currently_using_any_of_technology_uids",
    "currently_using_all_of_technology_uids",
    "currently_not_using_any_of_technology_uids",
    "q_organization_job_titles",
    "organization_job_locations",
    "q_organization_domains_list",
]

_RANGE_KEYS = [
    ("organization_num_jobs_range_min", "organization_num_jobs_range[min]"),
    ("organization_num_jobs_range_max", "organization_num_jobs_range[max]"),
    ("revenue_range_min",               "revenue_range[min]"),
    ("revenue_range_max",               "revenue_range[max]"),
]


class ApolloRateLimitError(Exception):
    pass


async def search_page(
    api_key: str,
    filters: dict[str, Any],
    page: int,
    per_page: int = 100,
) -> dict:
    """Fetch one page from Apollo People API Search. Raises ApolloRateLimitError on 429."""
    params: list[tuple[str, Any]] = [("page", page), ("per_page", per_page)]

    for key in _ARRAY_KEYS:
        val = filters.get(key)
        if val and isinstance(val, list):
            for item in val:
                params.append((f"{key}[]", item))

    if filters.get("q_keywords"):
        params.append(("q_keywords", filters["q_keywords"]))
    if filters.get("include_similar_titles") is not None:
        params.append(("include_similar_titles", str(filters["include_similar_titles"]).lower()))

    for flat_key, apollo_key in _RANGE_KEYS:
        if filters.get(flat_key) is not None:
            params.append((apollo_key, filters[flat_key]))

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            APOLLO_SEARCH_URL,
            headers={
                "x-api-key": api_key,
                "Content-Type": "application/json",
                "Cache-Control": "no-cache",
                "accept": "application/json",
            },
            params=params,
            json={},
        )

    if resp.status_code == 429:
        raise ApolloRateLimitError("Rate limit hit")
    if resp.status_code == 403:
        raise ValueError("Apollo key lacks access — ensure it is a master API key")
    if resp.status_code == 401:
        raise ValueError("Apollo API key is invalid")
    if resp.status_code not in (200, 422):
        raise ValueError(f"Apollo {resp.status_code}: {resp.text[:200]}")

    return resp.json()


class ApolloRateLimiter:
    """
    Paces Apollo requests to stay within free-tier limits:
      - 1 request per second (200/min limit = comfortable headroom)
      - Pauses ~1 hour when approaching 550 of the 600/hour ceiling
      - Exponential backoff on 429 (60s → 120s → 240s … max 600s)
    """

    def __init__(self, min_interval: float = 1.1):
        self._min_interval = min_interval
        self._last_call: float = 0.0
        self._hourly_count: int = 0
        self._hour_start: float = time.monotonic()
        self._backoff: float = 60.0

    async def wait(self) -> None:
        now = time.monotonic()

        # Reset hourly window
        if now - self._hour_start >= 3600:
            self._hourly_count = 0
            self._hour_start = now

        # Approaching hourly ceiling — sit out the remainder of the window
        if self._hourly_count >= 550:
            remaining = 3600 - (now - self._hour_start) + 10
            logger.info(f"Approaching hourly limit — pausing {remaining:.0f}s")
            await asyncio.sleep(remaining)
            self._hourly_count = 0
            self._hour_start = time.monotonic()

        # Enforce minimum interval between calls
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)

        self._last_call = time.monotonic()
        self._hourly_count += 1

    async def on_rate_limit(self) -> None:
        logger.warning(f"Apollo 429 — backing off {self._backoff:.0f}s")
        await asyncio.sleep(self._backoff)
        self._backoff = min(self._backoff * 2, 600)

    def reset_backoff(self) -> None:
        self._backoff = 60.0


async def paginate_profile(
    api_key: str,
    profile: dict[str, Any],
    limiter: ApolloRateLimiter,
    start_page: int = 1,
    max_pages: int = 500,
) -> list[tuple[int, list[str]]]:
    """
    Paginate Apollo for one ICP profile starting from start_page.
    Yields (page_number, [org_name, ...]) per page so the caller can
    checkpoint after each page.

    Returns a list of (page, org_names) tuples for all pages processed.
    """
    slug = profile["slug"]
    filters = profile["filters"]
    pages_data: list[tuple[int, list[str]]] = []

    logger.info(f"[{slug}] Starting from page {start_page}")

    for page in range(start_page, max_pages + 1):
        await limiter.wait()

        # Up to 2 retries on rate limit before giving up on this profile
        for attempt in range(3):
            try:
                data = await search_page(api_key, filters, page=page, per_page=100)
                limiter.reset_backoff()
                break
            except ApolloRateLimitError:
                if attempt == 2:
                    logger.error(f"[{slug}] 3 consecutive 429s on page {page} — aborting profile")
                    return pages_data
                await limiter.on_rate_limit()

        people = data.get("people", [])
        if not people:
            logger.info(f"[{slug}] No more results at page {page} — done")
            break

        org_names = [
            (person.get("organization") or {}).get("name", "").strip()
            for person in people
        ]
        org_names = [n for n in org_names if n]

        total = data.get("total_entries", "?")
        logger.info(f"[{slug}] page {page} — {len(org_names)} orgs (total_entries={total})")

        pages_data.append((page, org_names))

        if len(people) < 100:
            logger.info(f"[{slug}] Last page reached at {page}")
            break

    return pages_data
