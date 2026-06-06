from __future__ import annotations

import logging
import uuid
from typing import AsyncIterator, List, Optional

from app.core.logging import log_event
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

logger = logging.getLogger(__name__)


class JobService:
    """Job orchestration over a persistence gateway and an execution backend.

    Reads/queries delegate to the gateway; start_job creates a job and hands it
    to the executor. Both collaborators are injected (DIP) for testability.
    """

    def __init__(self, *, gateway: JobRepositoryGateway, executor: TemporalJobExecutor) -> None:
        self._gateway = gateway
        self._executor = executor

    async def list_jobs(self) -> List[JobSummary]:
        return await self._gateway.list_jobs()

    async def start_job(self, idempotency_key: Optional[str]) -> StartJobResponse:
        response = await self._gateway.create_job(idempotency_key)
        if not response.reused:
            await self._executor.start(uuid.UUID(response.job_id))

        log_event(
            logger,
            logging.INFO,
            "job.start.accepted",
            job_id=response.job_id,
            reused=response.reused,
        )
        return response

    async def get_job(self, job_id: str) -> Optional[JobDetail]:
        return await self._gateway.get_job(job_id)

    async def list_tasks(
        self, job_id: str, status_filter: Optional[DbTaskStatus], sort_by: str
    ) -> Optional[List[TaskSummary]]:
        return await self._gateway.list_tasks(job_id, status_filter, sort_by)

    async def get_task(self, job_id: str, task_id: int) -> Optional[TaskDetail]:
        return await self._gateway.get_task(job_id, task_id)

    async def list_task_records(
        self, job_id: str, task_id: int, *, limit: int, offset: int
    ) -> Optional[PaginatedExtractedRecords]:
        return await self._gateway.list_records(job_id, task_id, limit=limit, offset=offset)

    async def job_event_stream_available(self, job_id: str) -> bool:
        return await self._gateway.event_stream_available(job_id)

    def job_event_stream(self, job_id: str) -> AsyncIterator[Optional[dict]]:
        return self._gateway.event_stream(job_id)


def build_job_service(
    settings: Optional[Settings] = None,
    *,
    run_repository=with_repository,
) -> JobService:
    """Composition root: wire the gateway and Temporal executor."""
    settings = settings or get_settings()
    return JobService(
        gateway=JobRepositoryGateway(run_repository=run_repository),
        executor=TemporalJobExecutor(settings=settings, run_repository=run_repository),
    )


job_service = build_job_service()
