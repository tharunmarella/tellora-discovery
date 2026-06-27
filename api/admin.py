"""
Token-protected admin API for discovery maintenance jobs.

Start (local or Railway sidecar on signal-worker):

    uvicorn api.admin:app --host 0.0.0.0 --port 8080

Requires DISCOVERY_ADMIN_SECRET on every request (Bearer token).
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import Depends, FastAPI, HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

import settings as cfg
from api.job_runner import get_job, list_jobs, submit_job
from infra.lifespan import SERVICE_ADMIN, bootstrap, lifespan

logger = logging.getLogger("discovery.admin")
bootstrap(server_name=SERVICE_ADMIN)

_bearer = HTTPBearer(auto_error=False)


class ScrapeFieldsBackfillRequest(BaseModel):
    limit: Optional[int] = Field(default=None, ge=1, le=10_000)
    run_all: bool = False
    dry_run: bool = False
    concurrency: int = Field(default=8, ge=1, le=32)
    source: str = "apollo"


class SignalReenrichRequest(BaseModel):
    limit: Optional[int] = Field(default=None, ge=1, le=10_000)
    concurrency: int = Field(default=5, ge=1, le=32)
    batch_size: int = Field(default=50, ge=1, le=500)
    reset_enriched: bool = True
    reset_failed: bool = False


class WeeklyScrapeRequest(BaseModel):
    dry_run: bool = False
    headcount_only: bool = False
    force: bool = True


class HeadcountBackfillRequest(BaseModel):
    run_all: bool = False
    limit: Optional[int] = Field(default=None, ge=1, le=10_000)


def _require_admin_secret(
    credentials: Optional[HTTPAuthorizationCredentials] = Security(_bearer),
) -> None:
    secret = (cfg.DISCOVERY_ADMIN_SECRET or "").strip()
    if not secret:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="DISCOVERY_ADMIN_SECRET is not configured",
        )
    token = credentials.credentials if credentials else ""
    if token != secret:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing admin token",
        )


@asynccontextmanager
async def _app_lifespan(app: FastAPI):
    async with lifespan(server_name=SERVICE_ADMIN, axiom_background_flush=True):
        yield


app = FastAPI(
    title="Tellora Discovery Admin",
    version="1.0.0",
    lifespan=_app_lifespan,
)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_ADMIN}


@app.get("/admin/jobs", dependencies=[Depends(_require_admin_secret)])
async def admin_list_jobs(limit: int = 50) -> dict[str, Any]:
    jobs = [j.to_dict() for j in list_jobs(limit=limit)]
    return {"jobs": jobs, "count": len(jobs)}


@app.get("/admin/jobs/{job_id}", dependencies=[Depends(_require_admin_secret)])
async def admin_get_job(job_id: str) -> dict[str, Any]:
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job.to_dict()


@app.get(
    "/admin/scrape-fields/eligible",
    dependencies=[Depends(_require_admin_secret)],
)
async def scrape_fields_eligible(source: str = "apollo") -> dict[str, Any]:
    from scrape.scrape_fields_backfill import count_eligible_rows

    return {"source": source, "eligible": count_eligible_rows(source=source)}


@app.post(
    "/admin/jobs/scrape-fields-backfill",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(_require_admin_secret)],
)
async def start_scrape_fields_backfill(body: ScrapeFieldsBackfillRequest) -> dict[str, Any]:
    from scrape.scrape_fields_backfill import backfill_scrape_fields

    async def _run() -> dict[str, Any]:
        return await backfill_scrape_fields(
            limit=body.limit,
            run_all=body.run_all,
            concurrency=body.concurrency,
            dry_run=body.dry_run,
            source=body.source,
        )

    job = await submit_job("scrape_fields_backfill", body.model_dump(), _run)
    logger.info("Queued scrape_fields_backfill job %s", job.id)
    return {"job": job.to_dict()}


@app.post(
    "/admin/jobs/signal-reenrich",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(_require_admin_secret)],
)
async def start_signal_reenrich(body: SignalReenrichRequest) -> dict[str, Any]:
    from signals.runner import run as run_signal_enrichment

    async def _run() -> dict[str, Any]:
        await run_signal_enrichment(
            limit=body.limit,
            concurrency=body.concurrency,
            batch_size=body.batch_size,
            reset_failed=body.reset_failed,
            reset_enriched=body.reset_enriched,
        )
        return {"ok": True}

    job = await submit_job("signal_reenrich", body.model_dump(), _run)
    logger.info("Queued signal_reenrich job %s", job.id)
    return {"job": job.to_dict()}


@app.post(
    "/admin/jobs/weekly-scrape",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(_require_admin_secret)],
)
async def start_weekly_scrape(body: WeeklyScrapeRequest) -> dict[str, Any]:
    from cron.weekly import WeeklyCronArgs, apply_dry_run_settings, execute, validate_args

    async def _run() -> dict[str, Any]:
        args = WeeklyCronArgs(
            dry_run=body.dry_run,
            headcount_only=body.headcount_only,
            force=body.force,
        )
        validate_args(args)
        apply_dry_run_settings(args)
        await execute(args)
        return {"ok": True}

    job = await submit_job("weekly_scrape", body.model_dump(), _run)
    logger.info("Queued weekly_scrape job %s", job.id)
    return {"job": job.to_dict()}


@app.post(
    "/admin/jobs/headcount-backfill",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(_require_admin_secret)],
)
async def start_headcount_backfill(body: HeadcountBackfillRequest) -> dict[str, Any]:
    from scrape.headcount_backfill import backfill_apollo_headcounts

    async def _run() -> dict[str, Any]:
        return await backfill_apollo_headcounts(limit=body.limit, run_all=body.run_all)

    job = await submit_job("headcount_backfill", body.model_dump(), _run)
    logger.info("Queued headcount_backfill job %s", job.id)
    return {"job": job.to_dict()}
