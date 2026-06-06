from __future__ import annotations

from typing import AsyncIterator, List, Optional

from app.core.settings import Settings, get_settings
from app.db.enums import TaskStatus as DbTaskStatus
from app.dto.jobs import (
    JobDetail,
    JobSummary,
    PaginatedExtractedRecords,
    StartJobResponse,
    TaskDetail,
    TaskSummary,
)
from app.services.jobs._common import with_repository
from app.services.jobs.executor import TemporalJobExecutor
from app.services.jobs.gateway import JobRepositoryGateway
from app.services.jobs.launcher import JobLauncher
from app.services.jobs.reader import JobReader
from app.services.jobs.streaming import JobEventStream


class JobService:
    """Facade composing the read, launch, and event-stream services.

    Lets JobHandler depend on a single collaborator while each underlying
    service keeps a single responsibility.
    """

    def __init__(self, *, reader: JobReader, launcher: JobLauncher, events: JobEventStream) -> None:
        self._reader = reader
        self._launcher = launcher
        self._events = events

    async def list_jobs(self) -> List[JobSummary]:
        return await self._reader.list_jobs()

    async def start_job(self, idempotency_key: Optional[str]) -> StartJobResponse:
        return await self._launcher.start_job(idempotency_key)

    async def get_job(self, job_id: str) -> Optional[JobDetail]:
        return await self._reader.get_job(job_id)

    async def list_tasks(
        self, job_id: str, status_filter: Optional[DbTaskStatus], sort_by: str
    ) -> Optional[List[TaskSummary]]:
        return await self._reader.list_tasks(job_id, status_filter, sort_by)

    async def get_task(self, job_id: str, task_id: int) -> Optional[TaskDetail]:
        return await self._reader.get_task(job_id, task_id)

    async def list_task_records(
        self, job_id: str, task_id: int, *, limit: int, offset: int
    ) -> Optional[PaginatedExtractedRecords]:
        return await self._reader.list_records(job_id, task_id, limit=limit, offset=offset)

    async def job_event_stream_available(self, job_id: str) -> bool:
        return await self._events.available(job_id)

    def job_event_stream(self, job_id: str) -> AsyncIterator[Optional[dict]]:
        return self._events.stream(job_id)


def build_job_service(
    settings: Optional[Settings] = None,
    *,
    run_repository=with_repository,
) -> JobService:
    """Composition root: wire the gateway, executor strategy, and services."""
    settings = settings or get_settings()
    gateway = JobRepositoryGateway(run_repository=run_repository)
    executor = TemporalJobExecutor(settings=settings, run_repository=run_repository)

    return JobService(
        reader=JobReader(gateway),
        launcher=JobLauncher(gateway, executor),
        events=JobEventStream(gateway),
    )


job_service = build_job_service()
