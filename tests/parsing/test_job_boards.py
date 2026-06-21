"""HTTP-mocked tests for job board checks."""

import httpx
import pytest
import respx

from signals.constants import GREENHOUSE_API, LEVER_API
from signals.pipeline import check_job_boards


@pytest.mark.asyncio
@respx.mock
async def test_check_job_boards_greenhouse_hit():
    respx.get(GREENHOUSE_API.format(slug="acme")).mock(
        return_value=httpx.Response(200, json={"jobs": [{"title": "Engineer"}, {"title": "PM"}]})
    )
    result = await check_job_boards("Acme Corp", domain="acme.com")
    assert result["count"] == 2
    assert result["source"] == "greenhouse"


@pytest.mark.asyncio
@respx.mock
async def test_check_job_boards_lever_fallback():
    respx.get(GREENHOUSE_API.format(slug="acme")).mock(return_value=httpx.Response(404))
    respx.get(LEVER_API.format(slug="acme")).mock(
        return_value=httpx.Response(200, json=[{"text": "Designer"}])
    )
    result = await check_job_boards("Acme Corp", domain="acme.com")
    assert result["count"] == 1
    assert result["source"] == "lever"
