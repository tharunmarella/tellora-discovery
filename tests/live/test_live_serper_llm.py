"""Live smoke tests — require real API keys. Run with: pytest -m live"""

import os

import pytest

from scrape.domain_lookup import EXTRACT_PROMPT, INDUSTRY_ENUM
from signals.diff import diff_snapshots, snapshot_from_result
from signals.pipeline import enrich_company_signals


pytestmark = [pytest.mark.live, pytest.mark.asyncio]

HAS_SERPER = bool(os.getenv("SERPER_API_KEY"))
HAS_OPENAI = (os.getenv("OPENAI_API_KEY") or "") not in ("", "test-openai-key")


@pytest.mark.skipif(not (HAS_SERPER and HAS_OPENAI), reason="SERPER_API_KEY and OPENAI_API_KEY required")
async def test_live_serper_llm_domain_extract():
    import httpx

    from llm import get_router

    import settings as cfg

    resp = httpx.post(
        "https://google.serper.dev/search",
        headers={"X-API-KEY": cfg.SERPER_API_KEY, "Content-Type": "application/json"},
        json={"q": "Vercel CEO: Guillermo", "gl": "us", "hl": "en", "num": 10},
        timeout=15,
    )
    resp.raise_for_status()
    serper_data = resp.json()
    assert serper_data.get("organic") or serper_data.get("knowledgeGraph")

    trimmed = {
        "knowledgeGraph": serper_data.get("knowledgeGraph"),
        "organic": serper_data.get("organic", [])[:7],
    }
    import json

    prompt = EXTRACT_PROMPT.format(
        company_name="Vercel",
        serper_json=json.dumps(trimmed, indent=2),
        industry_list=", ".join(INDUSTRY_ENUM),
    )
    router = get_router()
    raw = router.complete_text(prompt, models=router.enrichment_models, temperature=0.0, json_mode=True)
    result = json.loads(raw)
    assert result.get("domain") or result.get("name")


@pytest.mark.skipif(not HAS_OPENAI, reason="OPENAI_API_KEY required")
async def test_live_enrich_company_signals():
    result = await enrich_company_signals(
        company_id="live-test",
        company_name="Vercel",
        domain="vercel.com",
        description=None,
        industry="Developer Tools",
        raw_meta=None,
        existing_headcount=600,
    )
    assert result.get("signal_enrichment_status") in ("enriched", "partial")
    assert result.get("tech_stack") is not None
    curr = snapshot_from_result(result)
    events = diff_snapshots(
        {"page_fingerprints": {"pricing": "deadbeef"}, "recent_launches": [], "pricing_model": "enterprise"},
        curr,
    )
    assert isinstance(events, list)
