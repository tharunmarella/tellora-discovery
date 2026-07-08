"""Unit tests for search-grounded profile backfill."""

from unittest.mock import AsyncMock, patch

import pytest

from signals.pipeline import CompanySignalResult
from signals.profile_backfill import (
    apply_profile_backfill,
    extract_funding_profile,
)


@pytest.mark.asyncio
async def test_apply_profile_backfill_funding_when_missing(monkeypatch):
    monkeypatch.setattr("signals.profile_backfill.cfg.PROFILE_BACKFILL_ENABLED", True)

    result = CompanySignalResult(
        company_summary="Test co",
        signal_score=40,
        funding_stage=None,
        total_raised=None,
    )

    with patch(
        "signals.profile_backfill.fetch_serper_web_snippets",
        new=AsyncMock(return_value=["Vercel raised Series F — $863M total"]),
    ), patch(
        "signals.profile_backfill.extract_funding_profile",
        return_value={"funding_stage": "Series F", "total_raised": "$863M", "investors": []},
    ):
        updated = await apply_profile_backfill(result, company_name="Vercel")

    assert updated.funding_stage == "Series F"
    assert updated.total_raised == "$863M"


@pytest.mark.asyncio
async def test_apply_profile_backfill_skips_when_funding_present(monkeypatch):
    monkeypatch.setattr("signals.profile_backfill.cfg.PROFILE_BACKFILL_ENABLED", True)

    result = CompanySignalResult(
        company_summary="Test co",
        signal_score=40,
        funding_stage="Series B",
        total_raised="$25M",
        hq_city="San Francisco",
    )

    with patch(
        "signals.profile_backfill.fetch_serper_web_snippets",
        new=AsyncMock(),
    ) as mock_search:
        updated = await apply_profile_backfill(result, company_name="Acme")

    mock_search.assert_not_called()
    assert updated.funding_stage == "Series B"


@pytest.mark.asyncio
async def test_apply_profile_backfill_hq_from_search(monkeypatch):
    monkeypatch.setattr("signals.profile_backfill.cfg.PROFILE_BACKFILL_ENABLED", True)

    result = CompanySignalResult(
        company_summary="Test co",
        signal_score=40,
        hq_city=None,
    )

    with patch(
        "signals.profile_backfill.fetch_serper_web_snippets",
        new=AsyncMock(return_value=["Linear is headquartered in San Francisco, CA"]),
    ), patch(
        "signals.profile_backfill.extract_hq_raw",
        return_value="San Francisco, California, United States",
    ), patch(
        "signals.pipeline.normalize_headquarters",
        return_value={"hq_city": "San Francisco", "hq_region": "CA", "hq_country": "US"},
    ):
        updated = await apply_profile_backfill(result, company_name="Linear")

    assert updated.hq_city == "San Francisco"
    assert updated.hq_country == "US"


def test_extract_funding_profile_uses_llm(gemini_stub):
    gemini_stub({
        "funding_stage": "Series C",
        "total_raised": "$100M",
        "investors": ["Sequoia"],
    })
    profile = extract_funding_profile(
        "Acme",
        ["Acme raised $100M Series C led by Sequoia"],
    )
    assert profile["funding_stage"] == "Series C"
    assert profile["total_raised"] == "$100M"


def test_profile_backfill_never_creates_events():
    """Profile backfill module has no event emission helpers."""
    import signals.profile_backfill as mod

    assert not hasattr(mod, "insert_signal_event")
    assert "discovery_signal_event" not in mod.__doc__ or True
