"""Unit tests for scheduler health metrics SQL."""

import pytest

from signals.scheduler_metrics import collect_scheduler_metrics

pytestmark = [pytest.mark.unit, pytest.mark.integration]


def test_collect_scheduler_metrics_returns_expected_keys(db_session):
    metrics = collect_scheduler_metrics(db_session)

    for key in (
        "pending_enrich",
        "processing_stale",
        "failed_retryable",
        "stale_index",
        "ats_board_cached",
        "last_scrape_completed_at",
        "last_scrape_started_at",
    ):
        assert key in metrics
