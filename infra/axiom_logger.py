"""Buffered Axiom client for the discovery service (worker + weekly cron)."""

from __future__ import annotations

import asyncio
import json
import logging
import traceback
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, Optional

import axiom_py as axiom

import settings as cfg

logger = logging.getLogger(__name__)

MAX_BODY_SIZE = 2048


class AxiomLogger:
    def __init__(self) -> None:
        self.client = None
        self.dataset_name = cfg.AXIOM_DATASET
        self.enabled = bool(cfg.AXIOM_TOKEN)
        self._buffer: deque = deque(maxlen=500)
        self._flush_task: Optional[asyncio.Task] = None
        self._flush_interval = 5
        self._batch_size = 50

        if self.enabled:
            try:
                kwargs: dict[str, Any] = {"token": cfg.AXIOM_TOKEN}
                if cfg.AXIOM_ORG_ID:
                    kwargs["org_id"] = cfg.AXIOM_ORG_ID
                self.client = axiom.Client(**kwargs)
                logger.info("[axiom] discovery initialized — dataset=%s", self.dataset_name)
            except Exception as e:
                logger.warning("[axiom] discovery init failed: %s", e)
                self.enabled = False

    def start_background_flush(self) -> None:
        if self.enabled and (self._flush_task is None or self._flush_task.done()):
            self._flush_task = asyncio.create_task(self._periodic_flush())

    async def stop(self) -> None:
        if self._flush_task:
            self._flush_task.cancel()
        if self._buffer:
            await self._flush()

    async def log_job_run(
        self,
        *,
        job_id: str,
        job_name: str,
        success: bool,
        duration_ms: float,
        service: str = "tellora-discovery-worker",
        job_try: int | None = None,
        exception_type: str | None = None,
        exception_message: str | None = None,
    ) -> None:
        await self._append({
            "trace_id": job_id,
            "service": service,
            "type": "job_run",
            "path": job_name,
            "job_id": job_id,
            "job_name": job_name,
            "status_code": 200 if success else 500,
            "success": success,
            "duration_ms": round(duration_ms, 2),
            "job_try": job_try,
            "exception_type": exception_type,
            "exception_message": exception_message,
        })

    async def log_job_failure(
        self,
        *,
        job_id: str,
        job_name: str,
        exc: BaseException,
        service: str = "tellora-discovery-worker",
    ) -> None:
        await self._append({
            "trace_id": job_id,
            "service": service,
            "type": "job_failure",
            "path": job_name,
            "job_id": job_id,
            "job_name": job_name,
            "status_code": 500,
            "success": False,
            "exception_type": type(exc).__name__,
            "exception_message": str(exc),
            "traceback": self._truncate(traceback.format_exc()),
        })

    async def log_task_run(
        self,
        *,
        task_name: str,
        success: bool,
        duration_ms: float,
        service: str = "tellora-discovery",
        stats: dict | None = None,
        exception_type: str | None = None,
        exception_message: str | None = None,
    ) -> None:
        await self._append({
            "trace_id": str(uuid.uuid4()),
            "service": service,
            "type": "task_run",
            "path": task_name,
            "task_name": task_name,
            "status_code": 200 if success else 500,
            "success": success,
            "duration_ms": round(duration_ms, 2),
            "exception_type": exception_type,
            "exception_message": exception_message,
            "event_data": json.dumps({"stats": stats}) if stats else None,
        })

    async def _append(self, fields: dict[str, Any]) -> None:
        if not self.enabled:
            return
        try:
            event = {
                "_time": datetime.now(timezone.utc).isoformat(),
                "environment": cfg.ENVIRONMENT,
                **fields,
            }
            self._buffer.append(event)
            if len(self._buffer) >= self._batch_size:
                await self._flush()
        except Exception as e:
            logger.warning("[axiom] failed to build event: %s", e)

    async def _periodic_flush(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval)
            if self._buffer:
                await self._flush()

    async def _flush(self) -> None:
        if not self._buffer or not self.client:
            return
        batch = []
        while self._buffer and len(batch) < 200:
            batch.append(self._buffer.popleft())
        try:
            await asyncio.to_thread(
                self.client.ingest_events,
                dataset=self.dataset_name,
                events=batch,
            )
        except Exception as e:
            logger.warning("[axiom] flush failed (%d events): %s", len(batch), e)

    @staticmethod
    def _truncate(text: Optional[str]) -> Optional[str]:
        if text and len(text) > MAX_BODY_SIZE:
            return text[:MAX_BODY_SIZE] + "...[truncated]"
        return text


axiom_logger = AxiomLogger()
