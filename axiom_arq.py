"""ARQ hooks for discovery worker → Axiom."""

from __future__ import annotations

import logging
from typing import Any

from arq.jobs import Job

from axiom_logger import axiom_logger
from sentry_telemetry import capture_job_failure

logger = logging.getLogger(__name__)

SERVICE = "tellora-discovery-worker"


async def after_job_end(ctx: dict[str, Any]) -> None:
    job_id = ctx.get("job_id")
    redis = ctx.get("redis")
    if not job_id or not redis:
        return
    try:
        info = await Job(job_id, redis=redis).result_info()
        if info is None:
            return
        duration_ms = (info.finish_time - info.start_time).total_seconds() * 1000
        exc = info.result if not info.success and isinstance(info.result, BaseException) else None
        await axiom_logger.log_job_run(
            job_id=job_id,
            job_name=info.function,
            success=info.success,
            duration_ms=duration_ms,
            job_try=info.job_try,
            exception_type=type(exc).__name__ if exc else None,
            exception_message=str(exc) if exc else None,
        )
    except Exception as e:
        logger.debug("[axiom] after_job_end failed: %s", e)


async def on_job_failure(ctx, job_id: str, job_name: str, exc: BaseException) -> None:
    capture_job_failure(
        exc,
        service=SERVICE,
        job_id=job_id,
        job_name=job_name,
    )
    await axiom_logger.log_job_failure(job_id=job_id, job_name=job_name, exc=exc)
    logger.exception("[%s] job %s (%s) failed", SERVICE, job_name, job_id)
