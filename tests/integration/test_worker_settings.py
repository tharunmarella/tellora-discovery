"""Wiring tests for the on-demand ARQ WorkerSettings.

These prove the worker process would actually route on-demand enrichment jobs
correctly (correct queue, registered functions, lifecycle + failure hooks),
without needing a live ARQ runtime.
"""

import pytest

import worker
from infra.axiom_arq import after_job_end, on_job_failure
from worker import WorkerSettings, enrich_company_task


pytestmark = pytest.mark.integration


def test_enrich_task_registered():
    assert enrich_company_task in WorkerSettings.functions


def test_listens_on_ondemand_queue():
    assert WorkerSettings.queue_name == "arq:ondemand"


def test_lifecycle_and_failure_hooks_wired():
    assert WorkerSettings.on_startup is worker.startup
    assert WorkerSettings.on_shutdown is worker.shutdown
    assert WorkerSettings.on_job_failure is on_job_failure
    assert WorkerSettings.after_job_end is after_job_end


def test_retry_and_concurrency_bounds():
    assert WorkerSettings.max_tries >= 1
    assert WorkerSettings.max_jobs >= 1
    assert WorkerSettings.job_timeout > 0


def test_monitoring_crons_registered():
    cron_funcs = {c.coroutine.__name__ for c in WorkerSettings.cron_jobs}
    assert "reconcile_pending_task" in cron_funcs
    assert "poll_edgar_form_d_task" in cron_funcs
    assert "refresh_watched_companies_task" in cron_funcs
    assert "refresh_stale_index_task" in cron_funcs
    assert "refresh_headcounts_task" in cron_funcs
    assert "run_discovery_scrape_task" in cron_funcs


def test_scrape_fallback_cron_calculates_next_run():
    """arq cron weekday must be set/list/tuple — frozenset crashes at runtime."""
    from datetime import datetime, timezone

    from arq.cron import next_cron

    scrape_cron = next(
        c for c in WorkerSettings.cron_jobs if c.coroutine.__name__ == "run_discovery_scrape_task"
    )
    now = datetime(2026, 6, 22, 2, 0, tzinfo=timezone.utc)  # Monday
    next_run = next_cron(
        now,
        weekday=scrape_cron.weekday,
        hour=scrape_cron.hour,
        minute=scrape_cron.minute,
    )
    assert next_run.weekday() in scrape_cron.weekday
