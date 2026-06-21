"""Integration tests for poll_* monitoring tasks."""

from unittest.mock import AsyncMock, patch

import pytest

from signals.monitoring import poll_edgar_form_d_task, poll_job_posts_task, poll_product_hunt_task


pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_poll_edgar_form_d_task_no_filings():
    with patch("signals.sources.edgar.fetch_recent_form_d", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = []
        result = await poll_edgar_form_d_task({})
    assert result == {"filings": 0, "matched": 0, "created": 0}


@pytest.mark.asyncio
async def test_poll_product_hunt_task_no_launches():
    with patch("signals.sources.news.fetch_product_hunt_launches", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = []
        result = await poll_product_hunt_task({})
    assert result == {"launches": 0, "matched": 0, "created": 0}


@pytest.mark.asyncio
async def test_poll_job_posts_task_no_watched(db_session):
    with patch("signals.job_posts.fetch_job_board_posts", new_callable=AsyncMock) as mock_fetch:
        mock_fetch.return_value = ([], "none")
        result = await poll_job_posts_task({})
    assert result["polled"] == 0
