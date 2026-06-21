"""Sentry capture helpers — aligned with Axiom job/task telemetry."""

from __future__ import annotations

from typing import Any

import sentry_sdk


def capture_job_failure(
    exc: BaseException,
    *,
    service: str,
    job_id: str,
    job_name: str,
    job_try: int | None = None,
) -> None:
    if not sentry_sdk.is_initialized():
        return
    with sentry_sdk.new_scope() as scope:
        scope.set_tag("service", service)
        scope.set_tag("job_name", job_name)
        scope.set_tag("type", "job_failure")
        scope.set_context(
            "job",
            {"job_id": job_id, "job_name": job_name, "job_try": job_try},
        )
        sentry_sdk.capture_exception(exc)


def capture_task_failure(
    exc: BaseException,
    *,
    service: str,
    task_name: str,
    stats: dict[str, Any] | None = None,
) -> None:
    if not sentry_sdk.is_initialized():
        return
    with sentry_sdk.new_scope() as scope:
        scope.set_tag("service", service)
        scope.set_tag("task_name", task_name)
        scope.set_tag("type", "task_failure")
        ctx: dict[str, Any] = {"task_name": task_name}
        if stats:
            ctx["stats"] = stats
        scope.set_context("task", ctx)
        sentry_sdk.capture_exception(exc)
