"""Integration tests for signals.runner --reset-enriched sampling."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import text

from signals.runner import run

pytestmark = pytest.mark.integration


def _enriched_result():
    return {
        "signal_enrichment_status": "enriched",
        "signal_score": 50,
        "buying_signals": [],
        "company_summary": "summary",
        "funding_stage": None,
        "total_raised": None,
        "headcount": None,
        "hiring_roles": [],
        "hiring_count": 0,
        "tech_stack": [],
        "description_embedding": None,
        "tsv_text": "x",
    }


@pytest.mark.asyncio
async def test_reset_enriched_sample_only_flips_limit_rows(db_session, company_factory):
    company_factory(name="A", domain="a-reset.com", status="enriched")
    company_factory(name="B", domain="b-reset.com", status="enriched")
    company_factory(name="C", domain="c-reset.com", status="enriched")

    with (
        patch("signals.runner.enrich_company_signals", new_callable=AsyncMock) as mock_enrich,
        patch("signals.runner.persist_result", MagicMock(return_value=True)) as mock_persist,
        patch("signals.runner._notify_signals_ready"),
        patch("signals.runner.asyncio.sleep", new_callable=AsyncMock),
    ):
        mock_enrich.return_value = _enriched_result()
        await run(limit=2, concurrency=2, batch_size=10, reset_failed=False, reset_enriched=True)

    # Only 2 of the 3 enriched rows were sampled + processed.
    assert mock_persist.call_count == 2
    remaining_enriched = db_session.execute(
        text("SELECT COUNT(*) FROM discovery_company WHERE signal_enrichment_status = 'enriched'")
    ).scalar()
    assert remaining_enriched == 1


@pytest.mark.asyncio
async def test_reset_enriched_noop_when_flag_off(db_session, company_factory):
    company_factory(name="A", domain="a-noop.com", status="enriched")

    with (
        patch("signals.runner.enrich_company_signals", new_callable=AsyncMock) as mock_enrich,
        patch("signals.runner.persist_result", MagicMock(return_value=True)),
        patch("signals.runner._notify_signals_ready"),
        patch("signals.runner.asyncio.sleep", new_callable=AsyncMock),
    ):
        await run(limit=None, concurrency=2, batch_size=10, reset_failed=False, reset_enriched=False)

    mock_enrich.assert_not_awaited()
    still_enriched = db_session.execute(
        text("SELECT COUNT(*) FROM discovery_company WHERE signal_enrichment_status = 'enriched'")
    ).scalar()
    assert still_enriched == 1
