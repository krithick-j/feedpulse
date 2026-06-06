from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Query, status
from fastapi.responses import StreamingResponse

from app.api.handlers import jobs as handlers
from app.dto.jobs import (
    JobDetail,
    JobSummary,
    PaginatedExtractedRecords,
    StartJobRequest,
    StartJobResponse,
    TaskDetail,
    TaskSummary,
)

router = APIRouter(prefix="/api/v1", tags=["jobs"])


@router.get("/jobs", response_model=List[JobSummary])
async def list_jobs() -> List[JobSummary]:
    return await handlers.list_jobs()


@router.post("/jobs", response_model=StartJobResponse, status_code=status.HTTP_202_ACCEPTED)
async def start_job(payload: Optional[StartJobRequest] = None) -> StartJobResponse:
    return await handlers.start_job(payload)


@router.get("/jobs/{job_id}", response_model=JobDetail)
async def get_job(job_id: str) -> JobDetail:
    return await handlers.get_job(job_id)


@router.get("/jobs/{job_id}/tasks", response_model=List[TaskSummary])
async def get_tasks(
    job_id: str,
    status_filter: Optional[str] = Query(default=None, alias="status"),
    sort_by: str = Query(default="url", alias="sort"),
) -> List[TaskSummary]:
    return await handlers.get_tasks(job_id, status_filter, sort_by)


@router.get("/jobs/{job_id}/tasks/{task_id}", response_model=TaskDetail)
async def get_task(job_id: str, task_id: int) -> TaskDetail:
    return await handlers.get_task(job_id, task_id)


@router.get("/jobs/{job_id}/tasks/{task_id}/records", response_model=PaginatedExtractedRecords)
async def get_task_records(
    job_id: str,
    task_id: int,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> PaginatedExtractedRecords:
    return await handlers.get_task_records(job_id, task_id, limit=limit, offset=offset)


@router.get("/jobs/{job_id}/events")
async def stream_job_events(job_id: str) -> StreamingResponse:
    return await handlers.stream_job_events(job_id)
