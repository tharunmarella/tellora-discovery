"""HTTP-mocked tests for tech stack detection."""

import httpx
import pytest
import respx

from signals.pipeline import detect_tech_stack


@pytest.mark.asyncio
@respx.mock
async def test_detect_tech_stack_finds_stripe():
    respx.get(url__regex=r"https://acme\.com.*").mock(
        return_value=httpx.Response(200, text='<script src="https://js.stripe.com/v3/"></script>')
    )
    detected = await detect_tech_stack("acme.com")
    assert "stripe" in detected


@pytest.mark.asyncio
@respx.mock
async def test_detect_tech_stack_fetch_error_returns_empty():
    respx.get(url__regex=r"https://acme\.com.*").mock(return_value=httpx.Response(500))
    assert await detect_tech_stack("acme.com") == []
