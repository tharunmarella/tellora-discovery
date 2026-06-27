"""Tests for iCIMS, JazzHR, and Rippling fetchers."""

import httpx
import pytest
import respx

from signals.ats_extended import (
    _parse_icims_page,
    _parse_jazzhr_listing,
    fetch_icims,
    fetch_jazzhr,
    fetch_rippling,
    icims_base_url,
)
from signals.job_posts import fetch_job_board_posts


def test_icims_base_url_variants():
    assert icims_base_url("acme") == "https://careers-acme.icims.com"
    assert icims_base_url("careers-acme") == "https://careers-acme.icims.com"
    assert icims_base_url("uscareers-rws") == "https://uscareers-rws.icims.com"


def test_parse_icims_page():
    html = """
    <li class="iCIMS_JobCardItem">
      <a href="https://careers-acme.icims.com/jobs/99/engineer/job" class="iCIMS_Anchor">
        <h3>Platform Engineer</h3>
      </a>
      <span class="sr-only field-label">Job Locations</span><span>US-CA-SF</span>
      <div class="col-xs-12 description"><p>Build APIs</p></div>
    </li>
    """
    posts = _parse_icims_page(html, max_posts=5)
    assert len(posts) == 1
    assert posts[0]["title"] == "Platform Engineer"
    assert posts[0]["external_id"] == "99"
    assert "Build APIs" in posts[0]["body_text"]


def test_parse_jazzhr_listing():
    html = """
    <tr id="row_job_1">
      <td><a class="job_title_link" href="/apply/jobs/details/ep3PtoGGEJ">Account Executive</a></td>
      <td>Remote</td>
    </tr>
    """
    posts = _parse_jazzhr_listing(html, "acme")
    assert len(posts) == 1
    assert posts[0]["title"] == "Account Executive"
    assert posts[0]["external_id"] == "ep3PtoGGEJ"
    assert posts[0]["location"] == "Remote"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_rippling_list_and_detail():
    respx.get("https://api.rippling.com/platform/api/ats/v1/board/acme/jobs").mock(
        return_value=httpx.Response(200, json={
            "items": [{"uuid": "job-1", "name": "SDR", "workLocation": {"displayName": "NYC"}}],
        })
    )
    respx.get("https://api.rippling.com/platform/api/ats/v1/board/acme/jobs/job-1").mock(
        return_value=httpx.Response(200, json={
            "description": {"role": "<p>Outbound sales</p>"},
            "companyName": "Acme Inc",
        })
    )
    async with httpx.AsyncClient() as client:
        posts = await fetch_rippling(client, "acme")
    assert len(posts) == 1
    assert posts[0]["title"] == "SDR"
    assert "Outbound sales" in posts[0]["body_text"]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_job_board_posts_rippling_from_careers_html():
    respx.get("https://api.rippling.com/platform/api/ats/v1/board/acme/jobs").mock(
        return_value=httpx.Response(200, json={
            "items": [{"uuid": "job-1", "name": "Engineer", "workLocation": "Remote"}],
        })
    )
    respx.get("https://api.rippling.com/platform/api/ats/v1/board/acme/jobs/job-1").mock(
        return_value=httpx.Response(200, json={"description": {"role": "<p>Ship features</p>"}})
    )
    posts, source, board = await fetch_job_board_posts(
        "Acme",
        domain="acme.com",
        careers_html="https://ats.rippling.com/acme/jobs",
    )
    assert source == "rippling"
    assert posts[0]["title"] == "Engineer"
    assert board["slug"] == "acme"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_icims_first_page():
    html = """
    <li class="iCIMS_JobCardItem">
      <a href="https://careers-acme.icims.com/jobs/1/role/job" class="iCIMS_Anchor">
        <h3>Analyst</h3>
      </a>
      <div class="col-xs-12 description"><p>Analyze data</p></div>
    </li>
    """
    respx.get(url__regex=r"https://careers-acme\.icims\.com/jobs/search.*").mock(
        return_value=httpx.Response(200, text=html)
    )
    async with httpx.AsyncClient() as client:
        posts = await fetch_icims(client, "acme")
    assert len(posts) == 1
    assert posts[0]["title"] == "Analyst"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_jazzhr_listing():
    listing = """
    <tr id="row_job_1">
      <td><a class="job_title_link" href="/apply/jobs/details/abc123">Designer</a></td>
      <td>Austin</td>
    </tr>
    """
    detail = """
    <script type="application/ld+json">
    {"@type": "JobPosting", "description": "<p>Design UI</p>"}
    </script>
    """
    respx.get("https://acme.applytojob.com/apply/jobs").mock(
        return_value=httpx.Response(200, text=listing)
    )
    respx.get("https://acme.applytojob.com/apply/jobs/details/abc123").mock(
        return_value=httpx.Response(200, text=detail)
    )
    async with httpx.AsyncClient() as client:
        posts = await fetch_jazzhr(client, "acme")
    assert len(posts) == 1
    assert posts[0]["title"] == "Designer"
    assert "Design UI" in posts[0]["body_text"]
