"""Unit tests for pending enrichment enqueue helper."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from signals import enqueue as enqueue_mod

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_enqueue_pending_enrichment_empty():
    mock_session = MagicMock()
    mock_session.execute.return_value.mappings.return_value.all.return_value = []

    with patch.object(enqueue_mod, "Session") as session_cls:
        session_cls.return_value.__enter__.return_value = mock_session
        result = await enqueue_mod.enqueue_pending_enrichment(limit=10)

    assert result == {"enqueued": 0}


@pytest.mark.asyncio
async def test_enqueue_pending_enrichment_enqueues_jobs():
    mock_session = MagicMock()
    mock_session.execute.return_value.mappings.return_value.all.return_value = [
        {"id": "c1"},
        {"id": "c2"},
    ]
    pool = AsyncMock()

    with (
        patch.object(enqueue_mod, "Session") as session_cls,
        patch("signals.enqueue.arq.create_pool", AsyncMock(return_value=pool)),
    ):
        session_cls.return_value.__enter__.return_value = mock_session
        result = await enqueue_mod.enqueue_pending_enrichment(limit=10)

    assert result == {"enqueued": 2}
    assert pool.enqueue_job.await_count == 2
    pool.aclose.assert_awaited_once()
