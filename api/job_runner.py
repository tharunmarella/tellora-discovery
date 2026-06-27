"""In-process background job runner for admin-triggered maintenance tasks."""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger("discovery.admin.jobs")

JobFn = Callable[[], Awaitable[dict[str, Any]]]


@dataclass
class JobRecord:
    id: str
    job_type: str
    params: dict[str, Any]
    status: str = "queued"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "job_type": self.job_type,
            "status": self.status,
            "params": self.params,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "result": self.result,
            "error": self.error,
        }


_jobs: dict[str, JobRecord] = {}
_lock = asyncio.Lock()
_heavy_lock = asyncio.Lock()


def get_job(job_id: str) -> Optional[JobRecord]:
    return _jobs.get(job_id)


def list_jobs(*, limit: int = 50) -> list[JobRecord]:
    rows = sorted(_jobs.values(), key=lambda j: j.created_at, reverse=True)
    return rows[:limit]


async def submit_job(job_type: str, params: dict[str, Any], runner: JobFn) -> JobRecord:
    job = JobRecord(id=str(uuid.uuid4()), job_type=job_type, params=params)
    async with _lock:
        _jobs[job.id] = job

    async def _run() -> None:
        async with _heavy_lock:
            job.status = "running"
            job.started_at = datetime.now(timezone.utc)
            try:
                job.result = await runner()
                job.status = "completed"
            except Exception as exc:
                job.error = str(exc)
                job.status = "failed"
                logger.exception("[%s] job %s failed", job_type, job.id)
            finally:
                job.finished_at = datetime.now(timezone.utc)

    asyncio.create_task(_run())
    return job
