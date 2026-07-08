"""
End-to-end pipeline quality gate — runs real enrichment + LLM judge.

Requires SERPER_API_KEY, GOOGLE_API_KEY/GEMINI_API_KEY, Docker not required.
Pre-push hook runs this suite and blocks push if quality is below threshold.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone

import pytest

import settings as cfg
from llm import get_gemini_client, retry_llm, strip_json_fences
from signals.pipeline import enrich_company_signals


pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]

_PLACEHOLDERS = {"", "test-gemini-key", "test-apollo-key"}


def _has_real_key(*names: str) -> bool:
    return any((os.getenv(name) or "") not in _PLACEHOLDERS for name in names)


def _require_keys() -> None:
    missing = []
    if not _has_real_key("SERPER_API_KEY"):
        missing.append("SERPER_API_KEY")
    if not _has_real_key("GOOGLE_API_KEY", "GEMINI_API_KEY"):
        missing.append("GOOGLE_API_KEY or GEMINI_API_KEY")
    if missing:
        pytest.fail(
            "E2E pipeline quality gate requires real API keys: "
            + ", ".join(missing)
            + ". Set them in .env before pushing."
        )


GOLDEN_COMPANIES = [
    {"name": "Vercel", "domain": "vercel.com"},
    {"name": "Stripe", "domain": "stripe.com"},
    {"name": "Linear", "domain": "linear.app"},
]

SCORE_FLOOR = 7
MEAN_THRESHOLD = 8

JUDGE_PROMPT = """You are a strict data-quality auditor for a B2B company enrichment pipeline.

TODAY'S DATE IS {today}. You have a Google Search tool — USE IT to verify recent,
time-sensitive claims (funding rounds, launches, headcount, customers) before judging.

CRITICAL DATE RULE: Any event dated on or before {today} is a PAST event, not a
"future" one. Do NOT call a claim a hallucination merely because it is more recent
than your training data — search to confirm it. Only flag a claim as a hallucination
if Google Search contradicts it or finds no supporting evidence.

Company: {company_name} ({domain})

PIPELINE OUTPUT (JSON):
{payload}

Score the output 0-10 on:
1. factual_accuracy — claims match what Google Search confirms for this company
2. completeness — key fields populated (summary, industry signals, tech, HQ if known, hiring if public)
3. no_hallucination — no invented funding, customers, or products (verify via search)

Return ONLY valid JSON (no prose, no markdown):
{{
  "score": <integer 0-10>,
  "reasons": "<1-3 sentences, cite what search confirmed/contradicted>",
  "field_issues": ["<specific field problems, empty list if none>"]
}}

Be strict: generic/vague summaries score <= 6. Claims that search CONTRADICTS score <= 4.
Do NOT penalize accurate recent facts that postdate your training data."""


def _judge_payload(result: dict) -> dict:
    return {
        "signal_enrichment_status": result.get("signal_enrichment_status"),
        "company_summary": result.get("company_summary"),
        "buying_signals": result.get("buying_signals"),
        "signal_score": result.get("signal_score"),
        "tech_stack": result.get("tech_stack"),
        "hq_city": result.get("hq_city"),
        "hq_region": result.get("hq_region"),
        "hq_country": result.get("hq_country"),
        "headcount": result.get("headcount"),
        "hiring_roles": result.get("hiring_roles"),
        "hiring_count": result.get("hiring_count"),
        "funding_stage": result.get("funding_stage"),
        "total_raised": result.get("total_raised"),
        "recent_launches": result.get("recent_launches"),
        "known_customers": result.get("known_customers"),
        "pricing_model": result.get("pricing_model"),
    }


def _extract_json_obj(text: str) -> dict:
    """Parse the judge's JSON verdict, tolerating prose/citations around it."""
    cleaned = strip_json_fences(text or "")
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    # Grounded responses may wrap the JSON in commentary — grab the last {...} block.
    matches = re.findall(r"\{.*\}", text or "", re.DOTALL)
    for candidate in reversed(matches):
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    raise ValueError(f"Judge returned non-JSON output: {text!r}")


def judge_pipeline_output(company_name: str, domain: str, result: dict) -> dict:
    prompt = JUDGE_PROMPT.format(
        today=datetime.now(timezone.utc).date().isoformat(),
        company_name=company_name,
        domain=domain,
        payload=json.dumps(_judge_payload(result), indent=2),
    )
    client = get_gemini_client()

    from google.genai import types

    config = types.GenerateContentConfig(
        tools=[types.Tool(google_search=types.GoogleSearch())],
        temperature=0,
    )

    def _do() -> dict:
        resp = client.models.generate_content(
            model=cfg.E2E_JUDGE_MODEL,
            contents=prompt,
            config=config,
        )
        return _extract_json_obj(resp.text)

    return retry_llm(_do)


@pytest.fixture(autouse=True)
def _e2e_keys_required():
    _require_keys()

async def test_pipeline_quality_golden_companies():
    scores: list[int] = []
    verdicts: list[dict] = []

    for company in GOLDEN_COMPANIES:
        name = company["name"]
        domain = company["domain"]
        result = await enrich_company_signals(
            company_id=f"e2e-{domain}",
            company_name=name,
            domain=domain,
            description=None,
            industry=None,
            raw_meta=None,
        )
        assert result.get("signal_enrichment_status") in ("enriched", "partial"), (
            f"{name}: pipeline returned status={result.get('signal_enrichment_status')}"
        )

        verdict = judge_pipeline_output(name, domain, result)
        score = int(verdict.get("score", 0))
        scores.append(score)
        verdicts.append({"company": name, "domain": domain, **verdict})
        print(f"\n[E2E judge] {name} ({domain}): score={score}")
        print(f"  reasons: {verdict.get('reasons')}")
        if verdict.get("field_issues"):
            print(f"  issues: {verdict.get('field_issues')}")

    mean_score = sum(scores) / len(scores)
    print(f"\n[E2E judge] mean={mean_score:.2f} scores={scores}")

    for v in verdicts:
        assert v["score"] >= SCORE_FLOOR, (
            f"{v['company']}: score {v['score']} < floor {SCORE_FLOOR}. "
            f"reasons={v.get('reasons')} issues={v.get('field_issues')}"
        )
    assert mean_score >= MEAN_THRESHOLD, (
        f"Mean judge score {mean_score:.2f} < {MEAN_THRESHOLD}. Verdicts: {verdicts}"
    )
