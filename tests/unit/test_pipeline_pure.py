"""Unit tests for pure helpers in signals/pipeline.py."""

import json

from signals.pipeline import (
    CompanySignalResult,
    _determine_enrichment_status,
    _items_to_hq_map,
    _parse_hq_batch_response,
    _round_headcount,
    _sources_had_data,
    build_search_tsv,
    page_fingerprint,
)


def test_round_headcount_small():
    assert _round_headcount(47) == 50


def test_round_headcount_medium():
    assert _round_headcount(230) == 250


def test_round_headcount_large():
    assert _round_headcount(1234) == 1200


def test_page_fingerprint_empty():
    assert page_fingerprint("") == ""
    assert page_fingerprint("short") == ""


def test_page_fingerprint_stable():
    text = "x" * 120
    assert page_fingerprint(text) == page_fingerprint(text)
    assert len(page_fingerprint(text)) == 16


def test_build_search_tsv():
    tsv = build_search_tsv(
        company_summary="AI sales platform",
        description="Desc",
        industry="SaaS",
        tech_stack=["stripe", "hubspot"],
        raw_meta={"keywords": ["crm"], "use_case": "outbound"},
        recent_launches=["Launch 1"],
        known_customers=["Acme"],
    )
    assert "AI sales platform" in tsv
    assert "stripe" in tsv
    assert "Launch 1" in tsv


def test_sources_had_data_from_job_board():
    assert _sources_had_data(
        ctx={"homepage": ""},
        job_board={"count": 3},
        tech_stack=[],
        funding_news=[],
        serper_kg={},
        github={},
        rss_news=[],
        hn_data={},
        gov_awards=[],
    )


def test_determine_enrichment_status_enriched():
    result = CompanySignalResult(company_summary="We build CRM software")
    assert _determine_enrichment_status(result, sources_had_data=False, extra_events=[]) == "enriched"


def test_determine_enrichment_status_partial():
    result = CompanySignalResult()
    assert _determine_enrichment_status(result, sources_had_data=True, extra_events=[]) == "partial"


def test_determine_enrichment_status_failed():
    result = CompanySignalResult()
    assert _determine_enrichment_status(result, sources_had_data=False, extra_events=[]) == "failed"


def test_parse_hq_batch_response_object():
    raw = json.dumps({
        "items": [
            {"raw": "San Francisco, CA", "hq_city": "San Francisco", "hq_region": "CA", "hq_country": "US"},
        ]
    })
    items = _parse_hq_batch_response(raw)
    mapped = _items_to_hq_map(items)
    assert mapped["San Francisco, CA"]["hq_country"] == "US"
