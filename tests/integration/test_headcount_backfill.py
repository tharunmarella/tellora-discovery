"""Integration tests for scrape/headcount_backfill.py."""

from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import text

from scrape.headcount_backfill import backfill_apollo_headcounts


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_backfill_fills_missing_headcount(db_session, company_factory):
    company_factory(name="Acme", domain="acme-fill.com", headcount=None)
    company_factory(name="Beta", domain="beta-fill.com", headcount=None)

    with (
        patch(
            "scrape.headcount_backfill.fetch_apollo_headcount",
            new_callable=AsyncMock,
        ) as mock_fetch,
        patch("scrape.headcount_backfill.asyncio.sleep", new_callable=AsyncMock),
    ):
        mock_fetch.return_value = {"headcount_estimate": 250}
        stats = await backfill_apollo_headcounts(limit=10)

    assert stats["filled"] == 2
    assert stats["skipped"] == 0
    rows = db_session.execute(
        text("SELECT headcount FROM discovery_company WHERE domain IN (:a, :b)"),
        {"a": "acme-fill.com", "b": "beta-fill.com"},
    ).scalars().all()
    assert rows == [250, 250]


@pytest.mark.asyncio
async def test_backfill_skips_when_apollo_returns_empty(db_session, company_factory):
    company_factory(name="Skip Co", domain="skip-hc.com", headcount=None)

    with (
        patch(
            "scrape.headcount_backfill.fetch_apollo_headcount",
            new_callable=AsyncMock,
        ) as mock_fetch,
        patch("scrape.headcount_backfill.asyncio.sleep", new_callable=AsyncMock),
    ):
        mock_fetch.return_value = {}
        stats = await backfill_apollo_headcounts(limit=10)

    assert stats["filled"] == 0
    assert stats["skipped"] == 1
    hc = db_session.execute(
        text("SELECT headcount FROM discovery_company WHERE domain = :d"),
        {"d": "skip-hc.com"},
    ).scalar()
    assert hc is None


@pytest.mark.asyncio
async def test_backfill_no_eligible_rows(db_session):
    with patch("scrape.headcount_backfill.asyncio.sleep", new_callable=AsyncMock):
        stats = await backfill_apollo_headcounts(limit=10)

    assert stats == {"filled": 0, "skipped": 0, "processed": 0, "batches": 1}
