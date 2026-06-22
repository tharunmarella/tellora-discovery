"""HTTP-mocked tests for Apollo client and headcount proxy."""

import httpx
import pytest
import respx

from scrape.apollo_client import (
    APOLLO_SEARCH_URL,
    ApolloRateLimitError,
    ApolloRateLimiter,
    paginate_profile,
    search_page,
)
from signals.pipeline import fetch_apollo_headcount


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
    respx.post(APOLLO_SEARCH_URL).mock(return_value=httpx.Response(429, text="rate limited"))
    with pytest.raises(ApolloRateLimitError):
        await search_page("key", {}, page=1)


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
