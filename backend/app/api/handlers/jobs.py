from __future__ import annotations

import json
import logging
from typing import AsyncIterator, List, Optional

from fastapi import HTTPException, status
from fastapi.responses import StreamingResponse

from app.core.logging import log_event
from app.db.enums import TaskStatus as DbTaskStatus
from app.schemas.jobs import (
    JobDetail,
    JobSummary,
    PaginatedExtractedRecords,
    StartJobRequest,
    StartJobResponse,
    TaskDetail,
    TaskSummary,
)
from app.services import jobs as job_service

logger = logging.getLogger(__name__)


async def list_jobs() -> List[JobSummary]:
    return await job_service.list_jobs()


async def start_job(payload: Optional[StartJobRequest]) -> StartJobResponse:
    idempotency_key = payload.idempotency_key if payload else None
    try:
        return await job_service.start_job(idempotency_key)
    except job_service.SimulatorRuntimeDisabledError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Simulator runtime is disabled unless ENABLE_SIMULATOR_RUNTIME=true",
        ) from exc


async def get_job(job_id: str) -> JobDetail:
    job = await job_service.get_job(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return job


async def get_tasks(
    job_id: str,
    status_filter: Optional[str],
    sort_by: str,
) -> List[TaskSummary]:
    parsed_status_filter = _parse_task_status_filter(status_filter)
    tasks = await job_service.list_tasks(job_id, parsed_status_filter, sort_by)
    if tasks is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return tasks


async def get_task(job_id: str, task_id: int) -> TaskDetail:
    task = await job_service.get_task(job_id, task_id)
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
    return task


async def get_task_records(
    job_id: str,
    task_id: int,
    *,
    limit: int,
    offset: int,
) -> PaginatedExtractedRecords:
    records = await job_service.list_task_records(job_id, task_id, limit=limit, offset=offset)
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


async def stream_job_events(job_id: str) -> StreamingResponse:
    if not await job_service.job_event_stream_available(job_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    async def event_stream() -> AsyncIterator[str]:
        async for payload in job_service.job_event_stream(job_id):
            if payload is None:
                yield ": keepalive\n\n"
            else:
                yield _encode_sse(payload)

    return StreamingResponse(event_stream(), media_type="text/event-stream")


def _encode_sse(payload: dict) -> str:
    return "data: {0}\n\n".format(json.dumps(payload))


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
