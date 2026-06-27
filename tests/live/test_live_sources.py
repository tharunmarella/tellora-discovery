"""
Per-source live smoke tests — proves each enrichment task works against the
REAL upstream API (not a canned fixture). Run with:

    pytest -m live tests/live/test_live_sources.py -v

Gating:
  * "Free" sources (EDGAR, Google News, Product Hunt, Hacker News, USAspending,
    job boards, tech-stack, DNS, Jina unauth) run with NO keys — they execute
    immediately under `-m live`.
  * Keyed sources skip cleanly until the relevant key is exported:
        SERPER_API_KEY          → Serper KG + exec-hire + funding news
        GITHUB_TOKEN            → GitHub org technographics (higher rate limit)
        TELLORA_APOLLO_API_KEY  → Apollo headcount

Assertions are intentionally tolerant about *content* (live data shifts) but
strict about *shape* and *not raising* — the point is to prove the wiring to
each real endpoint is intact.
"""

from __future__ import annotations

import os

import pytest


pytestmark = [pytest.mark.live, pytest.mark.asyncio]


# conftest.py sets placeholder keys via setdefault so imports don't blow up.
# Treat those placeholders as "no real key" for gating purposes.
_PLACEHOLDERS = {"", "test-gemini-key", "test-apollo-key", "your_apollo_master_key_here"}


def _has(*env_names: str) -> bool:
    return any((os.getenv(name) or "") not in _PLACEHOLDERS for name in env_names)


HAS_SERPER = _has("SERPER_API_KEY")
HAS_GEMINI = _has("GOOGLE_API_KEY", "GEMINI_API_KEY")
HAS_JINA = _has("JINA_API_KEY")
HAS_GITHUB = _has("GITHUB_TOKEN")
HAS_APOLLO = _has("TELLORA_APOLLO_API_KEY")


# ── Free sources (no key required) ───────────────────────────────────────────


async def test_live_edgar_form_d_index():
    """SEC EDGAR Form D full-text search returns the documented filing shape."""
    from signals.sources.edgar import fetch_recent_form_d

    filings = await fetch_recent_form_d(days=10)
    assert isinstance(filings, list)
    if filings:
        f = filings[0]
        assert {"name", "cik", "filed_at", "accession_no", "doc"} <= set(f)
        assert f["name"]


async def test_live_edgar_document_parse():
    """Fetch + parse at least one real Form D primary_doc.xml end-to-end."""
    from signals.sources.edgar import fetch_filing_details, fetch_recent_form_d

    filings = await fetch_recent_form_d(days=10)
    if not filings:
        pytest.skip("no recent Form D filings in window")
    detailed = await fetch_filing_details(filings[:5])
    parsed = [f for f in detailed if "industry_group" in f]
    # At least one of the first few should have parsed (EDGAR docs are stable).
    assert parsed, "no Form D documents parsed — EDGAR archive shape may have changed"


async def test_live_google_news_rss():
    """Google News RSS yields recent headlines for a well-known company."""
    from signals.sources.news import fetch_company_news

    items = await fetch_company_news("Microsoft", days=7)
    assert isinstance(items, list)
    assert items, "expected at least one Microsoft headline in the last 7 days"
    assert items[0]["title"]
    assert items[0]["url"]


async def test_live_product_hunt_feed():
    """Product Hunt public Atom feed parses into launch dicts."""
    from signals.sources.news import fetch_product_hunt_launches

    launches = await fetch_product_hunt_launches()
    assert isinstance(launches, list)
    if launches:
        assert {"title", "slug", "url", "date"} <= set(launches[0])


async def test_live_hacker_news_algolia():
    """Hacker News Algolia search returns the mentions/launches envelope."""
    from signals.sources.hn import fetch_hn_signals

    hn = await fetch_hn_signals("OpenAI", "openai.com")
    assert isinstance(hn, dict)
    assert "mentions" in hn and "launches" in hn
    assert isinstance(hn["mentions"], list)
    assert isinstance(hn["launches"], list)


async def test_live_usaspending_awards():
    """USAspending award search returns a tolerant award list for a big contractor."""
    from signals.sources.gov import fetch_gov_awards

    awards = await fetch_gov_awards("Booz Allen Hamilton", days=365)
    assert isinstance(awards, list)
    if awards:
        a = awards[0]
        assert {"award_id", "recipient", "amount", "agency", "date"} <= set(a)
        assert a["amount"] > 0


async def test_live_job_boards():
    """Greenhouse/Lever job-board probe returns the documented dict shape."""
    from signals.job_posts import check_job_boards

    result = await check_job_boards("Stripe", "stripe.com")
    assert isinstance(result, dict)
    assert "count" in result and "roles" in result
    assert isinstance(result["roles"], list)


async def test_live_tech_stack_detect():
    """Homepage HTML + header technographic regex runs against a real site."""
    from signals.pipeline import detect_tech_stack

    tech = await detect_tech_stack("stripe.com")
    assert isinstance(tech, list)


async def test_live_dns_tech_detect():
    """DNS TXT/MX vendor detection resolves a real domain."""
    from signals.pipeline import detect_dns_tech

    tech = await detect_dns_tech("google.com")
    assert isinstance(tech, list)
    # google.com MX is Google Workspace — strong, stable signal.
    assert "google_workspace" in tech


async def test_live_jina_company_context():
    """Jina Reader (unauth fallback OK) returns the 6-section context envelope."""
    from signals.pipeline import fetch_company_context

    ctx = await fetch_company_context("vercel.com")
    assert isinstance(ctx, dict)
    assert {"homepage", "about", "careers", "pricing", "customers", "changelog"} <= set(ctx)


# ── Serper (needs SERPER_API_KEY) ────────────────────────────────────────────


@pytest.mark.skipif(not HAS_SERPER, reason="SERPER_API_KEY required")
async def test_live_serper_kg():
    from signals.pipeline import fetch_serper_kg

    kg = await fetch_serper_kg("Vercel")
    assert isinstance(kg, dict)


@pytest.mark.skipif(not HAS_SERPER, reason="SERPER_API_KEY required")
async def test_live_serper_exec_hire_news():
    from signals.pipeline import fetch_exec_hire_news

    news = await fetch_exec_hire_news("Stripe")
    assert isinstance(news, list)
    if news:
        assert news[0]["title"]


# ── Funding news (Serper or Google News RSS) ─────────────────────────────────


async def test_live_funding_news():
    from signals.sources.funding_news import fetch_funding_news

    snippets = await fetch_funding_news("Stripe")
    assert isinstance(snippets, list)


# ── GitHub technographics (token optional; assert harder when authed) ─────────


async def test_live_github_signals():
    from signals.sources.github import fetch_github_signals

    gh = await fetch_github_signals("vercel.com")
    assert isinstance(gh, dict)
    assert {"org", "languages", "new_repos", "repo_count"} <= set(gh)
    if HAS_GITHUB:
        # Authenticated → not rate-limited; Vercel's verified org should resolve.
        assert gh["org"] == "vercel"
        assert isinstance(gh["languages"], list)


# ── Gemini synthesis + embeddings (needs GOOGLE_API_KEY) ─────────────────────


@pytest.mark.skipif(not HAS_GEMINI, reason="GOOGLE_API_KEY / GEMINI_API_KEY required")
async def test_live_gemini_synthesize():
    """Real Gemini call returns a populated CompanySignalResult, not _EMPTY_SIGNAL."""
    from signals.pipeline import CompanySignalResult, synthesize_company_signals

    homepage = (
        "Vercel is the platform for frontend developers, providing the speed and "
        "reliability innovators need to create at the moment of inspiration. We enable "
        "teams to deploy Next.js apps with zero configuration and global edge delivery."
    )
    result = synthesize_company_signals(
        company_name="Vercel",
        homepage_text=homepage,
        about_text="Vercel makes the Next.js framework and a deployment cloud.",
        careers_text="Hiring: Senior Software Engineer, Developer Advocate.",
        tech_stack=["next.js", "react"],
        job_board={"count": 2, "source": "greenhouse", "roles": ["Engineer", "DevRel"]},
        funding_news=[],
    )
    assert isinstance(result, CompanySignalResult)
    # A real model run on real homepage text should produce a summary.
    assert result.company_summary, "Gemini returned no company_summary — synthesis failed"


@pytest.mark.skipif(not HAS_GEMINI, reason="GOOGLE_API_KEY / GEMINI_API_KEY required")
async def test_live_gemini_embedding():
    from llm import embed_text

    vec = embed_text("Vercel is a developer tools and frontend cloud company.")
    assert isinstance(vec, list)
    assert len(vec) > 0
    assert all(isinstance(x, float) for x in vec[:5])


# ── Apollo headcount (needs real TELLORA_APOLLO_API_KEY) ─────────────────────


@pytest.mark.skipif(not HAS_APOLLO, reason="real TELLORA_APOLLO_API_KEY required")
async def test_live_apollo_headcount():
    from signals.pipeline import fetch_apollo_headcount

    result = await fetch_apollo_headcount("stripe.com")
    assert isinstance(result, dict)


@pytest.mark.skipif(not HAS_APOLLO, reason="real TELLORA_APOLLO_API_KEY required")
async def test_live_apollo_search_page():
    import settings as cfg
    from scrape.apollo_client import search_page

    data = await search_page(
        cfg.TELLORA_APOLLO_API_KEY,
        {"q_organization_domains_list": ["stripe.com"]},
        page=1,
        per_page=1,
    )
    assert isinstance(data, dict)
    total = data.get("total_entries") or (data.get("pagination") or {}).get("total_entries")
    assert total is not None
