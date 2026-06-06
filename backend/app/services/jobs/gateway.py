"""Persistence access for jobs — the single (database) data source.

Concentrates every repository call behind one collaborator so the reader,
launcher, and event services depend on this gateway rather than on raw
repository lambdas. Database-only, so no backend abstraction is warranted.
"""
from __future__ import annotations

import time
from typing import AsyncIterator, List, Optional

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

SSE_KEEPALIVE_SECONDS = 15.0


class JobRepositoryGateway:
    def __init__(
        self,
        *,
        run_repository=with_repository,
        event_listener_factory=JobEventListener,
        urls_provider=load_source_urls,
    ) -> None:
        self._run = run_repository
        self._event_listener_factory = event_listener_factory
        self._urls_provider = urls_provider

    async def list_jobs(self) -> List[JobSummary]:
        return await self._run(lambda repository: repository.list_job_summaries())

    async def get_job(self, job_id: str) -> Optional[JobDetail]:
        job_uuid = try_parse_job_id(job_id)
        if job_uuid is None:
            return None
        return await self._run(lambda repository: repository.get_job_detail(job_uuid))

    async def create_job(self, idempotency_key: Optional[str]) -> StartJobResponse:
        resolved_key = idempotency_key or f"api-{time.time_ns()}"
        return await self._run(
            lambda repository: repository.create_job_with_tasks(
                idempotency_key=resolved_key,
                urls=self._urls_provider(),
            )
        )

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

    async def list_records(
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

    async def event_stream_available(self, job_id: str) -> bool:
        job_uuid = try_parse_job_id(job_id)
        if job_uuid is None:
            return False
        projection = await self._run(lambda repository: repository.get_job_projection(job_uuid))
        return projection is not None

    async def event_stream(self, job_id: str) -> AsyncIterator[Optional[dict]]:
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
