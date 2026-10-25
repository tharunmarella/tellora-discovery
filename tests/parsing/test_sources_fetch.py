"""HTTP-mocked tests for signal source fetchers."""

import httpx
import pytest
import respx

from signals.job_posts import _strip_html, fetch_job_board_posts
from signals.sources.edgar import fetch_recent_form_d, normalize_name
from signals.sources.github import fetch_github_signals
from signals.sources.gov import fetch_gov_awards
from signals.sources.hn import fetch_hn_signals
from signals.sources.news import fetch_company_news


def test_edgar_normalize_name():
    assert normalize_name("Acme Technologies Inc.") == "acme"


@pytest.mark.asyncio
async def test_fetch_recent_form_d_lists_index(monkeypatch):
    rows = [{"name": "Acme Inc", "cik": "1234567", "filed_at": "2026-06-01",
             "accession_no": "0001234567-26-000001", "form": "D", "doc": "primary_doc.xml"}]
    monkeypatch.setattr("signals.sources.edgar._list_filings_sync", lambda form, days: rows)
    filings = await fetch_recent_form_d(days=2)
    assert filings[0]["name"] == "Acme Inc"


def test_parse_form_d_maps_edgartools_object():
    from types import SimpleNamespace as NS

    from signals.sources.edgar import parse_form_d

    form_d = NS(
        offering_data=NS(
            industry_group=NS(industry_group_type="Other Technology"),
            date_of_first_sale="2026-10-15",
            offering_sales_amounts=NS(total_offering_amount="12000000", total_amount_sold="9500000"),
            revenue_range="Decline to Disclose",
            is_new=True,
            business_combination_transaction=NS(is_business_combination=False),
        ),
        primary_issuer=NS(primary_address=NS(state_or_country_description="CALIFORNIA")),
        related_persons=[NS(first_name="Jane", last_name="Doe", relationships=["Executive Officer", "Director"])],
    )
    out = parse_form_d(form_d)
    assert out["is_tech"] and out["is_new"]
    assert out["offering_amount"] == 12_000_000 and out["amount_sold"] == 9_500_000
    assert out["state"] == "CALIFORNIA"
    assert out["related_persons"] == [{"name": "Jane Doe", "relationship": "Executive Officer, Director"}]


def test_parse_form_d_indefinite_amount_and_pooled_fund():
    from types import SimpleNamespace as NS

    from signals.sources.edgar import parse_form_d

    form_d = NS(
        offering_data=NS(
            industry_group=NS(industry_group_type="Pooled Investment Fund"),
            date_of_first_sale="",
            offering_sales_amounts=NS(total_offering_amount="Indefinite", total_amount_sold="0"),
            revenue_range=None, is_new=False, business_combination_transaction=None,
        ),
        primary_issuer=None,
        related_persons=[],
    )
    out = parse_form_d(form_d)
    assert not out["is_tech"] and not out["is_new"]
    assert out["offering_amount"] is None and out["amount_sold"] is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_company_news_parses_rss():
    rss = """<?xml version="1.0"?>
    <rss><channel>
      <item><title>Acme raises funding</title><link>https://news.example/a</link>
        <pubDate>Mon, 01 Jun 2026 00:00:00 GMT</pubDate><source>TechCrunch</source></item>
    </channel></rss>"""
    respx.get(url__regex=r"https://news\.google\.com/rss/.*").mock(
        return_value=httpx.Response(200, text=rss)
    )
    items = await fetch_company_news("Acme")
    assert len(items) == 1
    assert "funding" in items[0]["title"].lower()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_github_signals_resolves_org():
    respx.get("https://api.github.com/orgs/acme").mock(
        return_value=httpx.Response(200, json={"login": "acme", "blog": "https://acme.com"})
    )
    respx.get("https://api.github.com/orgs/acme/repos").mock(
        return_value=httpx.Response(200, json=[])
    )
    result = await fetch_github_signals("acme.com")
    assert result["org"] == "acme"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_hn_signals_returns_mentions():
    respx.get("https://hn.algolia.com/api/v1/search_by_date").mock(
        return_value=httpx.Response(200, json={
            "hits": [
                {
                    "title": "Acme launches new API",
                    "url": "https://acme.com/blog",
                    "points": 42,
                    "num_comments": 3,
                    "objectID": "123",
                    "created_at": "2026-06-01T00:00:00Z",
                }
            ]
        })
    )
    result = await fetch_hn_signals("Acme", "acme.com")
    assert len(result["mentions"]) == 1


@pytest.mark.asyncio
@respx.mock
async def test_fetch_gov_awards_parses_results():
    respx.post("https://api.usaspending.gov/api/v2/search/spending_by_award/").mock(
        return_value=httpx.Response(200, json={
            "results": [
                {
                    "Award ID": "AWD1",
                    "Recipient Name": "Acme Corp",
                    "Award Amount": 1_000_000,
                    "Awarding Agency": "NASA",
                    "Start Date": "2026-01-01",
                }
            ]
        })
    )
    awards = await fetch_gov_awards("Acme Corp")
    assert len(awards) == 1
    assert awards[0]["amount"] == 1_000_000


@pytest.mark.asyncio
@respx.mock
async def test_fetch_gov_awards_drops_lookalike_recipients_and_returns_uei():
    route = respx.post("https://api.usaspending.gov/api/v2/search/spending_by_award/").mock(
        return_value=httpx.Response(200, json={"results": [
            {"Award ID": "A1", "Recipient Name": "ORIGINAL FOOTWEAR MANUFACTURING BR INC",
             "Recipient UEI": "ZZZ", "Award Amount": 50_000, "Awarding Agency": "DLA"},
            {"Award ID": "A2", "Recipient Name": "TAGUP, INC", "Recipient UEI": "SC7GGDPKXTY4",
             "Award Amount": 4_137_996, "Awarding Agency": "Department of Defense",
             "Base Obligation Date": "2026-09-01"},
        ]})
    )
    awards = await fetch_gov_awards("Tagup")
    assert [a["award_id"] for a in awards] == ["A2"]
    assert awards[0]["uei"] == "SC7GGDPKXTY4" and awards[0]["date"] == "2026-09-01"

    await fetch_gov_awards("Tagup", uei="SC7GGDPKXTY4")
    import json as _json
    assert _json.loads(route.calls[-1].request.content)["filters"]["recipient_search_text"] == ["SC7GGDPKXTY4"]


def test_strip_html():
    assert "Hello world" in _strip_html("<p>Hello <b>world</b></p>")


@pytest.mark.asyncio
@respx.mock
async def test_fetch_job_board_posts_greenhouse():
    respx.get(url__regex=r"https://boards-api\.greenhouse\.io/.*").mock(
        return_value=httpx.Response(200, json={
            "jobs": [
                {
                    "id": 1,
                    "title": "Engineer",
                    "location": {"name": "Remote"},
                    "content": "<p>Build APIs</p>",
                }
            ]
        })
    )
    posts, source, ats_board = await fetch_job_board_posts("Acme", domain="acme.com")
    assert source == "greenhouse"
    assert posts[0]["title"] == "Engineer"
    assert ats_board is not None
    assert ats_board["slug"] == "acme"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_job_board_posts_workable_details_true():
    respx.get(url__regex=r"https://apply\.workable\.com/api/v1/widget/accounts/acme.*").mock(
        return_value=httpx.Response(200, json={
            "name": "Acme Inc",
            "jobs": [
                {
                    "shortcode": "ABC123",
                    "title": "Designer",
                    "description": "<p>Design things</p>",
                    "location": {"location_str": "NYC"},
                }
            ],
        })
    )
    posts, source, _ = await fetch_job_board_posts(
        "Acme",
        careers_html="https://apply.workable.com/acme",
    )
    assert source == "workable"
    assert posts[0]["title"] == "Designer"
    assert "Design" in posts[0]["body_text"]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_job_board_posts_uses_jobhive_pre_enrich(monkeypatch):
    from pathlib import Path

    from signals import jobhive_import as jhi
    from signals.jobhive_import import parse_jobhive_csv

    fixtures = Path(__file__).resolve().parent.parent / "fixtures"
    index = parse_jobhive_csv("greenhouse", (fixtures / "jobhive_sample.csv").read_text())
    monkeypatch.setattr(jhi, "get_jobhive_index", lambda **_: index)

    respx.get(url__regex=r"https://boards-api\.greenhouse\.io/v1/boards/stripe/.*").mock(
        return_value=httpx.Response(200, json={
            "jobs": [{"id": 1, "title": "Engineer", "location": {"name": "SF"}, "content": ""}],
        })
    )
    posts, source, ats_board = await fetch_job_board_posts(
        "Stripe Inc",
        domain="stripe.com",
    )
    assert source == "greenhouse"
    assert len(posts) == 1
    assert ats_board is not None
    assert ats_board["slug"] == "stripe"
    assert ats_board["source"] == "greenhouse"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_job_board_posts_uses_cached_ats_board():
    respx.get(url__regex=r"https://boards-api\.greenhouse\.io/v1/boards/stripe/.*").mock(
        return_value=httpx.Response(200, json={
            "jobs": [{"id": 1, "title": "PM", "location": {"name": "SF"}, "content": ""}],
        })
    )
    posts, source, ats_board = await fetch_job_board_posts(
        "Stripe",
        domain="stripe.com",
        ats_board={"source": "greenhouse", "slug": "stripe"},
    )
    assert source == "greenhouse"
    assert len(posts) == 1
    assert ats_board["slug"] == "stripe"
