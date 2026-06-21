"""Integration tests for worker.enrich_company_task."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import text

import settings as cfg
from worker import enrich_company_task


pytestmark = pytest.mark.integration


def _enriched_result(status: str = "enriched"):
    return {
        "company_summary": "Acme builds tools" if status == "enriched" else None,
        "buying_signals": ["Hiring"],
        "signal_score": 50,
        "funding_stage": None,
        "total_raised": None,
        "headcount": 100,
        "hiring_roles": ["Engineer"],
        "hiring_count": 1,
        "tech_stack": ["react"],
        "description_embedding": [0.1] * 768,
        "tsv_text": "Acme builds tools",
        "hq_city": None,
        "hq_region": None,
        "hq_country": None,
        "signal_enriched_at": datetime.now(timezone.utc),
        "signal_enrichment_status": status,
        "job_posts": [],
        "extra_events": [],
    }


async def _ready_queue(fake) -> list[str]:
    raw = await fake.lrange(cfg.SIGNALS_READY_KEY, 0, -1)
    return [v.decode() if isinstance(v, bytes) else v for v in raw]


@pytest.mark.asyncio
async def test_enrich_company_task_claims_and_persists(db_session, company_factory, fakeredis):
    company_id = company_factory(status="pending", domain="acme.com")
    with patch("signals.pipeline.enrich_company_signals", new_callable=AsyncMock) as mock_enrich:
        mock_enrich.return_value = _enriched_result()
        result = await enrich_company_task({"job_try": 1, "max_tries": 3}, company_id)

    assert result["ok"] is True
    row = db_session.execute(
        text("SELECT signal_enrichment_status, signal_score FROM discovery_company WHERE id = :id"),
        {"id": company_id},
    ).mappings().first()
    assert row["signal_enrichment_status"] == "enriched"
    assert row["signal_score"] == 50

    # Value moment: backend is notified the domain's signals are ready.
    assert "acme.com" in await _ready_queue(fakeredis)


@pytest.mark.asyncio
async def test_enrich_company_task_partial_status_pushes(db_session, company_factory, fakeredis):
    company_id = company_factory(status="pending", domain="partial.com")
    with patch("signals.pipeline.enrich_company_signals", new_callable=AsyncMock) as mock_enrich:
        mock_enrich.return_value = _enriched_result(status="partial")
        await enrich_company_task({"job_try": 1, "max_tries": 3}, company_id)

    assert "partial.com" in await _ready_queue(fakeredis)


@pytest.mark.asyncio
async def test_enrich_company_task_skip_does_not_push(db_session, company_factory, fakeredis):
    company_id = company_factory(status="enriched", domain="skip.com")
    result = await enrich_company_task({"job_try": 1, "max_tries": 3}, company_id)
    assert result.get("skipped") is True
    assert await _ready_queue(fakeredis) == []


@pytest.mark.asyncio
async def test_enrich_company_task_skips_already_enriched(db_session, company_factory):
    company_id = company_factory(status="enriched")
    result = await enrich_company_task({"job_try": 1, "max_tries": 3}, company_id)
    assert result.get("skipped") is True


@pytest.mark.asyncio
async def test_enrich_company_task_marks_failed_at_max_tries(db_session, company_factory):
    company_id = company_factory(status="pending")
    with patch("signals.pipeline.enrich_company_signals", new_callable=AsyncMock) as mock_enrich:
        mock_enrich.side_effect = RuntimeError("boom")
        with pytest.raises(RuntimeError):
            await enrich_company_task({"job_try": 3, "max_tries": 3}, company_id)

    row = db_session.execute(
        text("SELECT signal_enrichment_status FROM discovery_company WHERE id = :id"),
        {"id": company_id},
    ).scalar()
    assert row == "failed"


@pytest.mark.asyncio
async def test_enrich_company_task_leaves_processing_under_max_tries(db_session, company_factory):
    company_id = company_factory(status="pending")
    with patch("signals.pipeline.enrich_company_signals", new_callable=AsyncMock) as mock_enrich:
        mock_enrich.side_effect = RuntimeError("transient")
        with pytest.raises(RuntimeError):
            await enrich_company_task({"job_try": 1, "max_tries": 3}, company_id)

    row = db_session.execute(
        text("SELECT signal_enrichment_status FROM discovery_company WHERE id = :id"),
        {"id": company_id},
    ).scalar()
    assert row == "processing"
