"""Integration tests for refresh_* monitoring tasks."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from signals.monitoring import refresh_stale_index_task, refresh_watched_companies_task


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_refresh_watched_companies_selects_stale(db_session, watched_company_factory):
    stale = datetime.now(timezone.utc) - timedelta(days=7)
    company_id = watched_company_factory(
        name="Watched Co",
        domain="watched.com",
        status="enriched",
        signal_enriched_at=stale,
    )

    with patch("signals.monitoring.enrich_company_signals", new_callable=AsyncMock) as mock_enrich:
        mock_enrich.return_value = {
            "company_summary": "Summary",
            "buying_signals": [],
            "signal_score": 40,
            "signal_enrichment_status": "enriched",
            "tech_stack": [],
            "hiring_count": 0,
            "extra_events": [],
        }
        result = await refresh_watched_companies_task({})

    assert result["refreshed"] >= 1
    mock_enrich.assert_awaited()


@pytest.mark.asyncio
async def test_refresh_stale_index_skips_fresh(db_session, company_factory):
    fresh = datetime.now(timezone.utc) - timedelta(days=1)
    company_factory(
        name="Fresh Co",
        domain="fresh.com",
        status="enriched",
        signal_enriched_at=fresh,
    )

    with patch("signals.monitoring.enrich_company_signals", new_callable=AsyncMock) as mock_enrich:
        result = await refresh_stale_index_task({})

    assert result["refreshed"] == 0
    mock_enrich.assert_not_awaited()


@pytest.mark.asyncio
async def test_refresh_company_captures_sentry_on_failure(db_session, watched_company_factory):
    stale = datetime.now(timezone.utc) - timedelta(days=7)
    watched_company_factory(
        name="Fail Co",
        domain="fail-refresh.com",
        status="enriched",
        signal_enriched_at=stale,
    )

    boom = RuntimeError("enrich blew up")
    with (
        patch("signals.monitoring.enrich_company_signals", new_callable=AsyncMock) as mock_enrich,
        patch("signals.monitoring.capture_task_failure") as mock_sentry,
    ):
        mock_enrich.side_effect = boom
        result = await refresh_watched_companies_task({})

    assert result["refreshed"] >= 1
    mock_sentry.assert_called_once()
    assert mock_sentry.call_args.kwargs["task_name"] == "refresh_company"
    assert mock_sentry.call_args.args[0] is boom
