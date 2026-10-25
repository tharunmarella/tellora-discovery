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
    assert "daily_discovery_maintenance_task" in cron_funcs
    assert "log_scheduler_health_task" in cron_funcs
    assert "import_jobhive_slugs_task" in cron_funcs
    assert "run_discovery_scrape_task" not in cron_funcs
    assert "enqueue_scrape_pending_task" not in cron_funcs


def test_maintenance_does_not_repeat_jobs_that_have_their_own_cron():
    import inspect

    from signals.monitoring import daily_discovery_maintenance_task

    src = inspect.getsource(daily_discovery_maintenance_task)
    cron_funcs = {c.coroutine.__name__ for c in WorkerSettings.cron_jobs}
    assert "poll_edgar_form_d_task" in cron_funcs and "poll_edgar_form_d_task(" not in src
    assert "log_scheduler_health_task" in cron_funcs and "log_scheduler_health(" not in src


def test_max_jobs_uses_setting():
    import settings as cfg
    assert WorkerSettings.max_jobs == cfg.SIGNAL_ENRICH_MAX_JOBS
