from __future__ import annotations

import json
import logging
from typing import AsyncIterator, List, Optional

from fastapi import HTTPException, status
from fastapi.responses import StreamingResponse

from app.core.logging import log_event
from app.db.enums import TaskStatus as DbTaskStatus
from app.dto.jobs import (
    JobDetail,
    JobSummary,
    PaginatedExtractedRecords,
    StartJobRequest,
    StartJobResponse,
    TaskDetail,
    TaskSummary,
)
from app.services.jobs import JobService, job_service

logger = logging.getLogger(__name__)


class JobHandler:
    """Translates between HTTP concerns and the JobService.

    Maps service results to HTTP outcomes (404 on missing, 422 on bad filter)
    and builds the SSE response. The service is injected (DIP) so tests can
    supply a stubbed one.
    """

    def __init__(self, service: JobService = job_service) -> None:
        self._service = service

    async def list_jobs(self) -> List[JobSummary]:
        return await self._service.list_jobs()

    async def start_job(self, payload: Optional[StartJobRequest]) -> StartJobResponse:
        idempotency_key = payload.idempotency_key if payload else None
        return await self._service.start_job(idempotency_key)

    async def get_job(self, job_id: str) -> JobDetail:
        job = await self._service.get_job(job_id)
        if not job:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
        return job

    async def get_tasks(
        self,
        job_id: str,
        status_filter: Optional[str],
        sort_by: str,
    ) -> List[TaskSummary]:
        parsed_status_filter = self._parse_task_status_filter(status_filter)
        tasks = await self._service.list_tasks(job_id, parsed_status_filter, sort_by)
        if tasks is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
        return tasks

    async def get_task(self, job_id: str, task_id: int) -> TaskDetail:
        task = await self._service.get_task(job_id, task_id)
        if not task:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
        return task

    async def get_task_records(
        self,
        job_id: str,
        task_id: int,
        *,
        limit: int,
        offset: int,
    ) -> PaginatedExtractedRecords:
        records = await self._service.list_task_records(job_id, task_id, limit=limit, offset=offset)
        if records is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
        log_event(
            logger,
            logging.INFO,
            "task.records.page_served",
            job_id=job_id,
            task_id=task_id,
            limit=limit,
            offset=offset,
            returned_count=len(records.items),
            total=records.total,
        )
        return records

    async def stream_job_events(self, job_id: str) -> StreamingResponse:
        if not await self._service.job_event_stream_available(job_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

        service = self._service

        async def event_stream() -> AsyncIterator[str]:
            async for payload in service.job_event_stream(job_id):
                if payload is None:
                    yield ": keepalive\n\n"
                else:
                    yield _encode_sse(payload)

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    @staticmethod
    def _parse_task_status_filter(value: Optional[str]) -> Optional[DbTaskStatus]:
        if value is None or value == "all":
            return None
        try:
            return DbTaskStatus(value)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Invalid task status filter",
            ) from exc


def _encode_sse(payload: dict) -> str:
    return f"data: {json.dumps(payload)}\n\n"


# Default application-wide handler, wired with the production JobService.
job_handler = JobHandler()
