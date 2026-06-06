from __future__ import annotations

from typing import List, Optional

from app.db.enums import TaskStatus as DbTaskStatus
from app.dto.jobs import JobDetail, JobSummary, PaginatedExtractedRecords, TaskDetail, TaskSummary
from app.services.jobs.gateway import JobRepositoryGateway


class JobReader:
    """Read-side queries for jobs, tasks, and records."""

    def __init__(self, gateway: JobRepositoryGateway) -> None:
        self._gateway = gateway

    async def list_jobs(self) -> List[JobSummary]:
        return await self._gateway.list_jobs()

    async def get_job(self, job_id: str) -> Optional[JobDetail]:
        return await self._gateway.get_job(job_id)

    async def list_tasks(
        self, job_id: str, status_filter: Optional[DbTaskStatus], sort_by: str
    ) -> Optional[List[TaskSummary]]:
        return await self._gateway.list_tasks(job_id, status_filter, sort_by)

    async def get_task(self, job_id: str, task_id: int) -> Optional[TaskDetail]:
        return await self._gateway.get_task(job_id, task_id)

    async def list_records(
        self, job_id: str, task_id: int, *, limit: int, offset: int
    ) -> Optional[PaginatedExtractedRecords]:
        return await self._gateway.list_records(job_id, task_id, limit=limit, offset=offset)
