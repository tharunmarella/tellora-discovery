"""HTTP-mocked tests for Apollo client and headcount proxy."""

import httpx
import pytest
import respx

import scrape.apollo_client as apollo
from scrape.apollo_client import (
    APOLLO_SEARCH_URL,
    ApolloBudget,
    ApolloBudgetExhausted,
    ApolloRateLimitError,
    ApolloRateLimiter,
    paginate_profile,
    search_page,
)
from signals.pipeline import fetch_apollo_headcount


@pytest.fixture(autouse=True)
def local_budget(monkeypatch):
    """Fresh in-process budgets per test; never wait on a real minute window."""
    monkeypatch.setattr(apollo, "_budgets", {})
    monkeypatch.setattr(apollo, "budget_for", lambda key: apollo._budgets.setdefault(key, ApolloBudget(key)))

    async def _no_wait(self):
        return None

    monkeypatch.setattr(ApolloBudget, "wait_for_next_minute", _no_wait)


@pytest.mark.asyncio
@respx.mock
async def test_search_page_expands_domain_list_params():
    route = respx.post(APOLLO_SEARCH_URL).mock(
        return_value=httpx.Response(200, json={"people": [], "total_entries": 0})
    )
    await search_page(
        "test-key",
        {"q_organization_domains_list": ["stripe.com", "acme.com"]},
        page=2,
        per_page=1,
    )
    request = route.calls[0].request
    query = str(request.url)
    assert "q_organization_domains_list%5B%5D=stripe.com" in query or "q_organization_domains_list[]=stripe.com" in query
    assert "stripe.com" in query
    assert "acme.com" in query
    assert "page=2" in query
    assert "per_page=1" in query


@pytest.mark.asyncio
@respx.mock
@pytest.mark.parametrize(
    "status,exc_match",
    [
        (401, "invalid"),
        (403, "master"),
    ],
)
async def test_search_page_auth_errors(status, exc_match):
    respx.post(APOLLO_SEARCH_URL).mock(return_value=httpx.Response(status, text="error"))
    with pytest.raises(ValueError, match=exc_match):
        await search_page("bad-key", {}, page=1)


@pytest.mark.asyncio
@respx.mock
async def test_search_page_rate_limit_raises():
    route = respx.post(APOLLO_SEARCH_URL).mock(return_value=httpx.Response(429, text="rate limited"))
    with pytest.raises(ApolloRateLimitError):
        await search_page("key", {}, page=1)
    assert route.call_count == 3  # waited out the window twice before giving up


@respx.mock
async def test_search_page_recovers_after_one_429():
    respx.post(APOLLO_SEARCH_URL).mock(side_effect=[
        httpx.Response(429, text="rate limited"),
        httpx.Response(200, json={"people": [], "total_entries": 0}),
    ])
    assert (await search_page("key", {}, page=1))["total_entries"] == 0


@pytest.mark.asyncio
async def test_budget_hour_window_full_raises_without_calling():
    budget = ApolloBudget("k")
    budget._limits = {"minute": 200, "hour": 10, "day": 50000}
    for _ in range(9):  # 90% headroom
        await budget.acquire()
    with pytest.raises(ApolloBudgetExhausted):
        await budget.acquire()


@pytest.mark.asyncio
async def test_budget_minute_window_full_waits(monkeypatch):
    budget = ApolloBudget("k")
    budget._limits = {"minute": 2, "hour": 6000, "day": 50000}
    slept = []

    async def fake_sleep(seconds):
        slept.append(seconds)
        budget._local.clear()  # the next minute window starts empty

    monkeypatch.setattr(apollo.asyncio, "sleep", fake_sleep)
    await budget.acquire()   # 1 of int(2 × 0.9) = 1
    await budget.acquire()   # minute full → waits, then succeeds
    assert len(slept) == 1 and 0 < slept[0] <= 61


@pytest.mark.asyncio
async def test_budget_adopts_apollo_headers():
    budget = ApolloBudget("k")
    await budget.sync(httpx.Headers({"x-rate-limit-minute": "50", "x-rate-limit-hourly": "400",
                                     "x-rate-limit-24-hour": "2000"}))
    assert budget._limits == {"minute": 50, "hour": 400, "day": 2000}


@pytest.mark.asyncio
async def test_budget_shared_through_redis(fakeredis):
    a, b = ApolloBudget("k", "redis://fake"), ApolloBudget("k", "redis://fake")
    a._limits = b._limits = {"minute": 200, "hour": 4, "day": 50000}
    await a.acquire()
    await b.acquire()
    await a.acquire()  # 3 = int(4 × 0.9)
    with pytest.raises(ApolloBudgetExhausted):
        await b.acquire()


@respx.mock
async def test_headcount_skips_quietly_when_budget_spent(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "TELLORA_APOLLO_API_KEY", "key")
    route = respx.post(APOLLO_SEARCH_URL).mock(return_value=httpx.Response(200, json={"total_entries": 5}))

    async def spent(self):
        raise ApolloBudgetExhausted("day")

    monkeypatch.setattr(ApolloBudget, "acquire", spent)
    assert await fetch_apollo_headcount("acme.com") == {}
    assert route.call_count == 0


@pytest.mark.asyncio
@respx.mock
async def test_search_page_success_returns_json():
    payload = {"people": [{"first_name": "Jane"}], "total_entries": 42}
    respx.post(APOLLO_SEARCH_URL).mock(return_value=httpx.Response(200, json=payload))
    result = await search_page("key", {}, page=1)
    assert result == payload


@pytest.mark.asyncio
@respx.mock
async def test_fetch_apollo_headcount_parses_total_entries():
    respx.post(APOLLO_SEARCH_URL).mock(
        return_value=httpx.Response(200, json={"total_entries": 230, "people": []})
    )
    result = await fetch_apollo_headcount("stripe.com")
    assert result["apollo_people_count"] == 230
    assert result["headcount_estimate"] == 250


@pytest.mark.asyncio
@respx.mock
async def test_fetch_apollo_headcount_empty_when_zero_entries():
    respx.post(APOLLO_SEARCH_URL).mock(
        return_value=httpx.Response(200, json={"total_entries": 0, "people": []})
    )
    assert await fetch_apollo_headcount("unknown.com") == {}


@pytest.mark.asyncio
async def test_fetch_apollo_headcount_empty_without_api_key(monkeypatch):
    import settings as cfg

    monkeypatch.setattr(cfg, "TELLORA_APOLLO_API_KEY", "")
    assert await fetch_apollo_headcount("stripe.com") == {}


@pytest.mark.asyncio
@respx.mock
async def test_paginate_profile_extracts_orgs():
    respx.post(APOLLO_SEARCH_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "people": [
                    {
                        "first_name": "Jane",
                        "organization": {"name": "Acme Corp"},
                    },
                    {
                        "first_name": "Bob",
                        "organization": {"name": "Beta Inc"},
                    },
                ],
                "total_entries": 2,
            },
        )
    )
    profile = {"slug": "test", "filters": {"person_titles": ["CEO"]}}
    limiter = ApolloRateLimiter(min_interval=0)
    pages = await paginate_profile("key", profile, limiter, start_page=1, max_pages=1)
    assert len(pages) == 1
    page_num, orgs = pages[0]
    assert page_num == 1
    assert orgs == [("Acme Corp", "Jane"), ("Beta Inc", "Bob")]
