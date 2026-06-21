"""Integration tests for cron/weekly.py job orchestration."""

from unittest.mock import AsyncMock, patch

import pytest

from cron.weekly import (
    WeeklyCronArgs,
    execute,
    run_headcount_backfill,
    scrape_and_enrich,
)


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_scrape_and_enrich_dry_run_short_circuits():
    with patch("scrape.service.run_discovery_scrape", new_callable=AsyncMock) as mock_scrape:
        mock_scrape.return_value = {"total": 5}
        await scrape_and_enrich(dry_run=True)
    mock_scrape.assert_awaited_once()


@pytest.mark.asyncio
async def test_scrape_and_enrich_runs_enrichment_when_new_companies():
    with (
        patch("scrape.service.run_discovery_scrape", new_callable=AsyncMock) as mock_scrape,
        patch("signals.runner.run", new_callable=AsyncMock) as mock_enrich,
        patch("cron.weekly.run_headcount_backfill", new_callable=AsyncMock) as mock_hc,
    ):
        mock_scrape.return_value = {"total": 3}
        await scrape_and_enrich(dry_run=False)
    mock_enrich.assert_awaited_once()
    mock_hc.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_headcount_backfill():
    with patch("scrape.headcount_backfill.backfill_apollo_headcounts", new_callable=AsyncMock) as mock_bf:
        mock_bf.return_value = {"updated": 2}
        await run_headcount_backfill(run_all=True)
    mock_bf.assert_awaited_once_with(run_all=True)


@pytest.mark.asyncio
async def test_execute_headcount_only():
    with patch("cron.weekly.run_headcount_backfill", new_callable=AsyncMock) as mock_hc:
        await execute(WeeklyCronArgs(headcount_only=True))
    mock_hc.assert_awaited_once_with(run_all=True)
