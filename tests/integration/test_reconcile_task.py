"""Integration tests for reconcile_pending_task."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import text

from signals.monitoring import reconcile_pending_task


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_reconcile_requeues_stuck_processing(db_session, company_factory):
    stale = datetime.now(timezone.utc) - timedelta(minutes=30)
    company_id = company_factory(
        status="processing",
        signal_last_attempt_at=stale,
        signal_attempt_count=1,
    )

    mock_pool = MagicMock()
    mock_pool.enqueue_job = AsyncMock()
    mock_pool.aclose = AsyncMock()

    with patch("arq.create_pool", new_callable=AsyncMock) as mock_create:
        mock_create.return_value = mock_pool
        result = await reconcile_pending_task({})

    assert result["requeued"] == 1
    status = db_session.execute(
        text("SELECT signal_enrichment_status FROM discovery_company WHERE id = :id"),
        {"id": company_id},
    ).scalar()
    assert status == "pending"
    mock_pool.enqueue_job.assert_awaited_once()
    args, kwargs = mock_pool.enqueue_job.call_args
    assert args[0] == "enrich_company_task"
    assert args[1] == company_id
    assert kwargs.get("_queue_name") == "arq:ondemand"


@pytest.mark.asyncio
async def test_reconcile_retryable_failed(db_session, company_factory):
    stale = datetime.now(timezone.utc) - timedelta(hours=2)
    company_id = company_factory(
        status="failed",
        signal_last_attempt_at=stale,
        signal_attempt_count=1,
    )

    mock_pool = MagicMock()
    mock_pool.enqueue_job = AsyncMock()
    mock_pool.aclose = AsyncMock()

    with patch("arq.create_pool", new_callable=AsyncMock) as mock_create:
        mock_create.return_value = mock_pool
        result = await reconcile_pending_task({})

    assert result["requeued"] == 1
    mock_pool.enqueue_job.assert_awaited_once()
    args, kwargs = mock_pool.enqueue_job.call_args
    assert args[1] == company_id
    assert kwargs.get("_queue_name") == "arq:ondemand"


@pytest.mark.asyncio
async def test_reconcile_skips_recent_processing(db_session, company_factory):
    """A row that started processing seconds ago must NOT be re-queued."""
    fresh = datetime.now(timezone.utc) - timedelta(minutes=1)
    company_factory(
        status="processing",
        signal_last_attempt_at=fresh,
        signal_attempt_count=1,
    )

    mock_pool = MagicMock()
    mock_pool.enqueue_job = AsyncMock()
    mock_pool.aclose = AsyncMock()

    with patch("arq.create_pool", new_callable=AsyncMock) as mock_create:
        mock_create.return_value = mock_pool
        result = await reconcile_pending_task({})

    assert result["requeued"] == 0
    mock_pool.enqueue_job.assert_not_awaited()
