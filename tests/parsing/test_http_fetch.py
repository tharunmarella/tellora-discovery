"""Tests for optional httpcloak HTML fetch fallback."""

from unittest.mock import MagicMock, patch

import httpx
import pytest
import respx

from signals.http_fetch import FetchResult, fetch_html


@pytest.mark.asyncio
@respx.mock
async def test_fetch_html_httpx_success():
    respx.get("https://example.com/").mock(return_value=httpx.Response(200, text="<html>ok</html>"))
    async with httpx.AsyncClient() as client:
        result = await fetch_html(client, "https://example.com/")
    assert result.status_code == 200
    assert result.text == "<html>ok</html>"
    assert result.via == "httpx"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_html_no_fallback_when_disabled():
    respx.get("https://example.com/").mock(return_value=httpx.Response(403, text="blocked"))
    with patch("signals.http_fetch.httpcloak_enabled", return_value=False):
        async with httpx.AsyncClient() as client:
            result = await fetch_html(client, "https://example.com/")
    assert result.status_code == 403
    assert result.via == "httpx"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_html_httpcloak_on_403():
    respx.get("https://example.com/").mock(return_value=httpx.Response(403, text="blocked"))

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "<html>via cloak</html>"
    mock_resp.headers = {"content-type": "text/html"}

    mock_session = MagicMock()
    mock_session.get.return_value = mock_resp

    with patch("signals.http_fetch.httpcloak_enabled", return_value=True):
        with patch("signals.http_fetch._httpcloak_get_sync") as mock_cloak:
            mock_cloak.return_value = FetchResult(
                status_code=200, text="<html>via cloak</html>", via="httpcloak",
            )
            async with httpx.AsyncClient() as client:
                result = await fetch_html(client, "https://example.com/")
    assert result.status_code == 200
    assert result.via == "httpcloak"
    mock_cloak.assert_called_once()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_jazzhr_uses_httpcloak_on_403():
    from signals.ats_extended import fetch_jazzhr

    listing = """
    <tr id="row_job_1">
      <td><a class="job_title_link" href="/apply/jobs/details/abc123">Designer</a></td>
      <td>Austin</td>
    </tr>
    """
    respx.get("https://acme.applytojob.com/apply/jobs").mock(
        return_value=httpx.Response(403, text="cf challenge")
    )
    respx.get("https://acme.applytojob.com/apply/jobs/details/abc123").mock(
        return_value=httpx.Response(200, text="{}")
    )

    with patch("signals.http_fetch.httpcloak_enabled", return_value=True):
        with patch("signals.http_fetch._httpcloak_get_sync") as mock_cloak:
            mock_cloak.return_value = FetchResult(status_code=200, text=listing, via="httpcloak")
            async with httpx.AsyncClient() as client:
                posts = await fetch_jazzhr(client, "acme")
    assert len(posts) == 1
    assert posts[0]["title"] == "Designer"
