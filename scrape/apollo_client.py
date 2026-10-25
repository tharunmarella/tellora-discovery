"""
Apollo People API Search client with a shared rate budget.

Endpoint: POST https://api.apollo.io/api/v1/mixed_people/api_search
Credits:  FREE — this endpoint does not consume Apollo credits.
Rate limits (this key, from /usage_stats/api_usage_stats, Oct 2026):
          200/minute, 6,000/hour, 50,000/day — fixed windows, no Retry-After.
          Every response carries x-rate-limit-{minute,hourly,24-hour} and
          x-{minute,hourly,24-hour}-usage; those headers are the source of truth.

Every caller (ICP fill, the per-company headcount proxy, headcount backfills)
goes through search_page → ApolloBudget, a Redis counter per window shared by
all worker processes using the key, kept 10% under Apollo's limits:
  - minute window full → wait for the next minute
  - hour / day window full → ApolloBudgetExhausted (callers stop for now)
  - 429 anyway (another client on the key) → wait out the minute, retry twice
"""

import asyncio
import hashlib
import logging
import time
from typing import Any, Optional

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
    "q_organization_keyword_tags",
]

_RANGE_KEYS = [
    ("organization_num_jobs_range_min", "organization_num_jobs_range[min]"),
    ("organization_num_jobs_range_max", "organization_num_jobs_range[max]"),
    ("revenue_range_min",               "revenue_range[min]"),
    ("revenue_range_max",               "revenue_range[max]"),
]


class ApolloRateLimitError(Exception):
    pass


class ApolloBudgetExhausted(ApolloRateLimitError):
    """The hourly or daily window is used up — retrying now won't help."""


_DEFAULT_LIMITS = {"minute": 200, "hour": 6000, "day": 50000}
_WINDOW_SECONDS = {"minute": 60, "hour": 3600, "day": 86400}
_LIMIT_HEADERS = {"minute": "x-rate-limit-minute", "hour": "x-rate-limit-hourly", "day": "x-rate-limit-24-hour"}
_USAGE_HEADERS = {"minute": "x-minute-usage", "hour": "x-hourly-usage", "day": "x-24-hour-usage"}
HEADROOM = 0.9


class ApolloBudget:
    """
    Fixed-window request counters shared through Redis (per API key), so
    concurrent enrichments, fills and backfills can't jointly exceed Apollo's
    limits. Falls back to in-process counters when Redis is unavailable.
    """

    def __init__(self, api_key: str, redis_url: Optional[str] = None):
        self._prefix = "apollo:rl:" + hashlib.sha256(api_key.encode()).hexdigest()[:12]
        self._redis_url = redis_url
        self._local: dict[str, int] = {}
        self._limits = dict(_DEFAULT_LIMITS)

    def _key(self, window: str, now: float) -> str:
        return f"{self._prefix}:{window}:{int(now // _WINDOW_SECONDS[window])}"

    async def _client(self):
        if not self._redis_url:
            return None
        try:
            import redis.asyncio as aioredis

            return aioredis.from_url(self._redis_url, socket_connect_timeout=2)
        except Exception:
            return None

    async def _incr(self, keys: dict[str, str], by: int) -> dict[str, int]:
        r = await self._client()
        if r is not None:
            try:
                pipe = r.pipeline()
                for window, key in keys.items():
                    pipe.incrby(key, by)
                    pipe.expire(key, _WINDOW_SECONDS[window] * 2)
                res = await pipe.execute()
                stored = await r.get(f"{self._prefix}:limits")
                await r.aclose()
                if stored:
                    import json

                    self._limits.update(json.loads(stored))
                return {w: int(res[i * 2]) for i, w in enumerate(keys)}
            except Exception as exc:
                logger.debug(f"Apollo budget: Redis unavailable, using local counters ({exc})")
        out = {}
        for window, key in keys.items():
            self._local[key] = self._local.get(key, 0) + by
            out[window] = self._local[key]
        return out

    async def acquire(self) -> None:
        while True:
            now = time.time()
            keys = {w: self._key(w, now) for w in _WINDOW_SECONDS}
            used = await self._incr(keys, 1)
            over = [w for w in ("day", "hour", "minute") if used[w] > int(self._limits[w] * HEADROOM)]
            if not over:
                return
            await self._incr(keys, -1)
            if "day" in over or "hour" in over:
                raise ApolloBudgetExhausted(f"Apollo {over[0]} budget used ({used[over[0]]}/{self._limits[over[0]]})")
            await asyncio.sleep(60 - (now % 60) + 0.5)

    async def sync(self, headers: httpx.Headers) -> None:
        """Adopt Apollo's own limits and usage (other clients may share the key)."""
        try:
            limits = {w: int(headers[h]) for w, h in _LIMIT_HEADERS.items() if headers.get(h)}
            usage = {w: int(headers[h]) for w, h in _USAGE_HEADERS.items() if headers.get(h)}
        except ValueError:
            return
        if not limits and not usage:
            return
        self._limits.update(limits)
        r = await self._client()
        if r is None:
            return
        try:
            import json

            now = time.time()
            if limits:
                await r.set(f"{self._prefix}:limits", json.dumps(self._limits), ex=86400)
            for window, n in usage.items():
                key = self._key(window, now)
                current = int(await r.get(key) or 0)
                if n > current:
                    await r.incrby(key, n - current)
                    await r.expire(key, _WINDOW_SECONDS[window] * 2)
            await r.aclose()
        except Exception:
            pass

    async def wait_for_next_minute(self) -> None:
        await asyncio.sleep(60 - (time.time() % 60) + 1)


_budgets: dict[str, ApolloBudget] = {}


def budget_for(api_key: str) -> ApolloBudget:
    if api_key not in _budgets:
        try:
            import settings as cfg

            redis_url = cfg.REDIS_URL
        except Exception:
            redis_url = None
        _budgets[api_key] = ApolloBudget(api_key, redis_url)
    return _budgets[api_key]


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

    budget = budget_for(api_key)
    for attempt in range(3):
        await budget.acquire()
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
        await budget.sync(resp.headers)
        if resp.status_code != 429:
            break
        if attempt == 2:
            raise ApolloRateLimitError("Rate limit hit (429 after waiting out the minute window twice)")
        logger.warning("Apollo 429 — waiting for the next minute window")
        await budget.wait_for_next_minute()

    if resp.status_code == 403:
        raise ValueError("Apollo key lacks access — ensure it is a master API key")
    if resp.status_code == 401:
        raise ValueError("Apollo API key is invalid")
    if resp.status_code not in (200, 422):
        raise ValueError(f"Apollo {resp.status_code}: {resp.text[:200]}")

    return resp.json()


class ApolloRateLimiter:
    """
    Per-caller pacing between pages (smooths bursts inside the minute window)
    plus backoff on repeated 429s. Window budgets live in ApolloBudget.
    """

    def __init__(self, min_interval: float = 0.35):
        self._min_interval = min_interval
        self._last_call: float = 0.0
        self._backoff: float = 60.0

    async def wait(self) -> None:
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_call = time.monotonic()

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
) -> list[tuple[int, list[tuple[str, str]]]]:
    """
    Paginate Apollo for one ICP profile starting from start_page.

    Returns a list of (page, [(org_name, ceo_first_name), ...]) tuples.
    ceo_first_name is the CEO's first name (free tier gives this unobfuscated)
    and is used to disambiguate Jina searches for companies with generic names.
    """
    slug = profile["slug"]
    filters = profile["filters"]
    pages_data: list[tuple[int, list[tuple[str, str]]]] = []

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

        orgs: list[tuple[str, str]] = []
        for person in people:
            org_name = (person.get("organization") or {}).get("name", "").strip()
            if org_name:
                orgs.append((org_name, person.get("first_name") or ""))

        total = data.get("total_entries", "?")
        logger.info(f"[{slug}] page {page} — {len(orgs)} orgs (total_entries={total})")

        pages_data.append((page, orgs))

        if len(people) < 100:
            logger.info(f"[{slug}] Last page reached at {page}")
            break

    return pages_data
