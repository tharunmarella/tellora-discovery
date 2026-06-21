"""End-to-end on-demand worker loop.

Proves the full crash-recovery + on-demand enrichment path that the always-on
worker serves:

    stuck 'processing' row
        -> reconcile_pending_task resets it to 'pending' and re-enqueues
           enrich_company_task(company_id) onto arq:ondemand
        -> worker picks up that exact company_id, enriches, persists,
           and pushes the domain to the signals_ready queue for the backend.

The only thing mocked is the expensive enrichment compute
(enrich_company_signals) and the ARQ Redis pool; the claim SQL, status
transitions, persistence, and Redis notification all run for real.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import text

import settings as cfg
from signals.monitoring import reconcile_pending_task
from worker import enrich_company_task


pytestmark = pytest.mark.integration


def _enriched_result():
    return {
        "company_summary": "Acme builds tools",
        "buying_signals": ["Hiring surge"],
        "signal_score": 72,
        "funding_stage": None,
        "total_raised": None,
        "headcount": 120,
        "hiring_roles": ["Engineer"],
        "hiring_count": 3,
        "tech_stack": ["react"],
        "description_embedding": [0.1] * 768,
        "tsv_text": "Acme builds tools",
        "hq_city": None,
        "hq_region": None,
        "hq_country": None,
        "signal_enriched_at": datetime.now(timezone.utc),
        "signal_enrichment_status": "enriched",
        "job_posts": [],
        "extra_events": [],
    }


@pytest.mark.asyncio
async def test_stuck_job_recovered_and_enriched_end_to_end(
    db_session, company_factory, fakeredis
):
    # 1. A company is stuck mid-enrichment (worker died), last attempt is stale.
    stale = datetime.now(timezone.utc) - timedelta(minutes=30)
    company_id = company_factory(
        name="Acme Corp",
        domain="acme.com",
        status="processing",
        signal_last_attempt_at=stale,
        signal_attempt_count=1,
    )

    # 2. Reconcile cron re-queues it. Capture what gets enqueued.
    enqueued: list[tuple] = []
    mock_pool = MagicMock()

    async def _capture(*args, **kwargs):
        enqueued.append((args, kwargs))

    mock_pool.enqueue_job = AsyncMock(side_effect=_capture)
    mock_pool.aclose = AsyncMock()

    with patch("arq.create_pool", new_callable=AsyncMock) as mock_create:
        mock_create.return_value = mock_pool
        recon = await reconcile_pending_task({})

    assert recon["requeued"] == 1
    assert db_session.execute(
        text("SELECT signal_enrichment_status FROM discovery_company WHERE id = :id"),
        {"id": company_id},
    ).scalar() == "pending"

    # The reconcile enqueued enrich_company_task for THIS company on the on-demand queue.
    assert len(enqueued) == 1
    args, kwargs = enqueued[0]
    assert args[0] == "enrich_company_task"
    requeued_id = args[1]
    assert requeued_id == company_id
    assert kwargs.get("_queue_name") == "arq:ondemand"

    # 3. The worker pulls that exact job id and runs it.
    with patch("signals.pipeline.enrich_company_signals", new_callable=AsyncMock) as mock_enrich:
        mock_enrich.return_value = _enriched_result()
        result = await enrich_company_task({"job_try": 1, "max_tries": 3}, requeued_id)

    # 4. Result persisted + backend notified.
    assert result["ok"] is True
    assert result["status"] == "enriched"
    row = db_session.execute(
        text("SELECT signal_enrichment_status, signal_score FROM discovery_company WHERE id = :id"),
        {"id": company_id},
    ).mappings().first()
    assert row["signal_enrichment_status"] == "enriched"
    assert row["signal_score"] == 72

    ready_raw = await fakeredis.lrange(cfg.SIGNALS_READY_KEY, 0, -1)
    ready = [v.decode() if isinstance(v, bytes) else v for v in ready_raw]
    assert "acme.com" in ready
