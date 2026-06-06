from __future__ import annotations

import logging
import time
import uuid
from typing import AsyncIterator, List, Optional

from app.core.logging import log_event
from app.core.settings import Settings, get_settings
from app.data.xml_sources import load_source_urls
from app.db.enums import TaskStatus as DbTaskStatus
from app.db.notifications import JobEventListener
from app.dto.jobs import (
    JobDetail,
    JobSummary,
    PaginatedExtractedRecords,
    StartJobResponse,
    TaskDetail,
    TaskSummary,
)
from app.services.jobs import projections
from app.services.jobs._common import try_parse_job_id, with_repository
from app.services.jobs.executor import TemporalJobExecutor

logger = logging.getLogger(__name__)

SSE_KEEPALIVE_SECONDS = 15.0


class JobService:
    """Job orchestration over the repository and the Temporal executor.

    Manages the per-call DB session (via the injected repository runner), adapts
    inputs (job-id parsing), and runs the live event stream. Collaborators are
    injected for testability.
    """

    def __init__(
        self,
        *,
        executor: TemporalJobExecutor,
        run_repository=with_repository,
        event_listener_factory=JobEventListener,
        urls_provider=load_source_urls,
    ) -> None:
        self._executor = executor
        self._run = run_repository
        self._event_listener_factory = event_listener_factory
        self._urls_provider = urls_provider

    async def list_jobs(self) -> List[JobSummary]:
        return await self._run(lambda repository: repository.list_job_summaries())

    async def start_job(self, idempotency_key: Optional[str]) -> StartJobResponse:
        resolved_key = idempotency_key or f"api-{time.time_ns()}"
        response = await self._run(
            lambda repository: repository.create_job_with_tasks(
                idempotency_key=resolved_key,
                urls=self._urls_provider(),
            )
        )
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
        job_uuid = try_parse_job_id(job_id)
        if job_uuid is None:
            return None
        return await self._run(lambda repository: repository.get_job_detail(job_uuid))

    async def list_tasks(
        self, job_id: str, status_filter: Optional[DbTaskStatus], sort_by: str
    ) -> Optional[List[TaskSummary]]:
        job_uuid = try_parse_job_id(job_id)
        if job_uuid is None:
            return None
        return await self._run(
            lambda repository: repository.list_task_summaries(
                job_uuid, status_filter=status_filter, sort_by=sort_by
            )
        )

    async def get_task(self, job_id: str, task_id: int) -> Optional[TaskDetail]:
        job_uuid = try_parse_job_id(job_id)
        if job_uuid is None:
            return None
        return await self._run(lambda repository: repository.get_task_detail(job_uuid, task_id))

    async def list_task_records(
        self, job_id: str, task_id: int, *, limit: int, offset: int
    ) -> Optional[PaginatedExtractedRecords]:
        job_uuid = try_parse_job_id(job_id)
        if job_uuid is None:
            return None
        return await self._run(
            lambda repository: repository.list_task_records(
                job_uuid, task_id, limit=limit, offset=offset
            )
        )

    async def job_event_stream_available(self, job_id: str) -> bool:
        job_uuid = try_parse_job_id(job_id)
        if job_uuid is None:
            return False
        projection = await self._run(lambda repository: repository.get_job_projection(job_uuid))
        return projection is not None

    async def job_event_stream(self, job_id: str) -> AsyncIterator[Optional[dict]]:
        job_uuid = try_parse_job_id(job_id)
        if job_uuid is None:
            return

        async with self._event_listener_factory(job_id=job_id) as listener:
            initial = await self._run(lambda repository: repository.get_job_projection(job_uuid))
            if initial is None:
                return

            last_snapshot = projections.job_snapshot_payload(initial, "job.snapshot")
            last_tasks = projections.task_payload_map(initial)
            yield last_snapshot

            if projections.is_terminal(initial):
                yield projections.job_snapshot_payload(initial, "job.completed")
                return

            while True:
                notification = await listener.next_event(timeout=SSE_KEEPALIVE_SECONDS)
                projection = await self._run(lambda repository: repository.get_job_projection(job_uuid))
                if projection is None:
                    return

                delta_events, snapshot, current_tasks = projections.projection_delta_events(
                    projection, previous_snapshot=last_snapshot, previous_tasks=last_tasks
                )
                for event in delta_events:
                    yield event

                last_snapshot = snapshot
                last_tasks = current_tasks

                if projections.is_terminal(projection):
                    yield projections.job_snapshot_payload(projection, "job.completed")
                    return

                if notification is None and not delta_events:
                    yield None


def build_job_service(
    settings: Optional[Settings] = None,
    *,
    run_repository=with_repository,
) -> JobService:
    """Composition root: wire the Temporal executor into the service."""
    settings = settings or get_settings()
    executor = TemporalJobExecutor(settings=settings, run_repository=run_repository)
    return JobService(executor=executor, run_repository=run_repository)


job_service = build_job_service()
