from __future__ import annotations

import asyncio
import logging
import time
import uuid
from typing import AsyncIterator, List, Optional

from temporalio.exceptions import WorkflowAlreadyStartedError

from app.core.logging import log_event
from app.core.settings import Settings, get_settings
from app.data.mock_store import store as default_store
from app.data.xml_sources import load_source_urls
from app.db.notifications import JobEventListener
from app.dto.jobs import (
    JobDetail,
    JobSummary,
    PaginatedExtractedRecords,
    StartJobResponse,
    TaskDetail,
    TaskSummary,
)
from app.db.enums import TaskStatus as DbTaskStatus
from app.services.job_runtime import queue_for_url
from app.services.job_simulator import schedule_job_simulation
from app.services.jobs import projections
from app.services.jobs._common import try_parse_job_id, with_repository
from app.temporal.client import get_temporal_client
from app.temporal.types import ProcessXmlJobInput, WorkflowTaskInput
from app.temporal.workflows import ProcessXmlJobWorkflow

logger = logging.getLogger(__name__)

SSE_KEEPALIVE_SECONDS = 15.0


class SimulatorRuntimeDisabledError(Exception):
    """Raised when a job needs the simulator runtime but it is disabled."""


class JobService:
    """Orchestrates job lifecycle, task/record queries, and event streaming.

    Collaborators are injected (DIP) so callers and tests can substitute the
    backend, repository runner, simulator, and event listener without patching
    module globals.
    """

    def __init__(
        self,
        *,
        settings: Optional[Settings] = None,
        run_repository=with_repository,
        store=default_store,
        schedule_simulation=schedule_job_simulation,
        event_listener_factory=JobEventListener,
        temporal_starter=None,
    ) -> None:
        self._settings = settings or get_settings()
        self._run = run_repository
        self._store = store
        self._schedule_simulation = schedule_simulation
        self._event_listener_factory = event_listener_factory
        self._start_temporal = temporal_starter or self._default_start_temporal

    @property
    def _is_mock(self) -> bool:
        return self._settings.data_backend == "mock"

    # ------------------------------------------------------------------ jobs
    async def list_jobs(self) -> List[JobSummary]:
        if self._is_mock:
            return self._store.list_jobs()
        return await self._run(lambda repository: repository.list_job_summaries())

    async def start_job(self, idempotency_key: Optional[str]) -> StartJobResponse:
        if self._is_mock:
            return self._store.start_job(idempotency_key)

        resolved_key = idempotency_key or f"api-{time.time_ns()}"
        response = await self._run(
            lambda repository: repository.create_job_with_tasks(
                idempotency_key=resolved_key,
                urls=load_source_urls(),
            )
        )
        if not response.reused:
            if self._settings.job_execution_backend == "temporal":
                await self._start_temporal(uuid.UUID(response.job_id))
            else:
                if not self._settings.enable_simulator_runtime:
                    raise SimulatorRuntimeDisabledError
                self._schedule_simulation(uuid.UUID(response.job_id))

        log_event(
            logger,
            logging.INFO,
            "job.start.accepted",
            job_id=response.job_id,
            idempotency_key=resolved_key,
            reused=response.reused,
            execution_backend=self._settings.job_execution_backend,
        )
        return response

    async def get_job(self, job_id: str) -> Optional[JobDetail]:
        if self._is_mock:
            return self._store.get_job(job_id)

        job_uuid = try_parse_job_id(job_id)
        if job_uuid is None:
            return None
        return await self._run(lambda repository: repository.get_job_detail(job_uuid))

    # ----------------------------------------------------------- tasks/records
    async def list_tasks(
        self,
        job_id: str,
        status_filter: Optional[DbTaskStatus],
        sort_by: str,
    ) -> Optional[List[TaskSummary]]:
        if self._is_mock:
            tasks = self._store.get_tasks(job_id)
            if tasks is None:
                return None
            if status_filter is not None:
                tasks = [task for task in tasks if task.status == status_filter.value]
            return projections.sort_mock_tasks(tasks, sort_by)

        job_uuid = try_parse_job_id(job_id)
        if job_uuid is None:
            return None
        return await self._run(
            lambda repository: repository.list_task_summaries(
                job_uuid,
                status_filter=status_filter,
                sort_by=sort_by,
            )
        )

    async def get_task(self, job_id: str, task_id: int) -> Optional[TaskDetail]:
        if self._is_mock:
            return self._store.get_task(job_id, task_id)

        job_uuid = try_parse_job_id(job_id)
        if job_uuid is None:
            return None
        return await self._run(lambda repository: repository.get_task_detail(job_uuid, task_id))

    async def list_task_records(
        self,
        job_id: str,
        task_id: int,
        *,
        limit: int,
        offset: int,
    ) -> Optional[PaginatedExtractedRecords]:
        if self._is_mock:
            records = self._store.get_task_records(job_id, task_id, limit=limit, offset=offset)
            return records or None

        job_uuid = try_parse_job_id(job_id)
        if job_uuid is None:
            return None
        return await self._run(
            lambda repository: repository.list_task_records(
                job_uuid,
                task_id,
                limit=limit,
                offset=offset,
            )
        )

    # ---------------------------------------------------------------- events
    async def job_event_stream_available(self, job_id: str) -> bool:
        if self._is_mock:
            return self._store.snapshot_event(job_id) is not None

        job_uuid = try_parse_job_id(job_id)
        if job_uuid is None:
            return False
        projection = await self._run(lambda repository: repository.get_job_projection(job_uuid))
        return projection is not None

    async def job_event_stream(self, job_id: str) -> AsyncIterator[Optional[dict]]:
        stream = self._mock_event_stream if self._is_mock else self._database_event_stream
        async for payload in stream(job_id):
            yield payload

    async def _mock_event_stream(self, job_id: str) -> AsyncIterator[Optional[dict]]:
        snapshot = self._store.snapshot_event(job_id)
        if not snapshot:
            return

        yield snapshot.model_dump()

        while True:
            await asyncio.sleep(1.5)
            events = self._store.advance_job(job_id)
            if not events:
                break

            for event in events:
                yield event.model_dump()

            if any(event.type == "job.completed" for event in events if hasattr(event, "type")):
                break

    async def _database_event_stream(self, job_id: str) -> AsyncIterator[Optional[dict]]:
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
                    projection,
                    previous_snapshot=last_snapshot,
                    previous_tasks=last_tasks,
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

    # --------------------------------------------------------------- temporal
    async def _default_start_temporal(self, job_id: uuid.UUID) -> None:
        tasks = await self._run(lambda repository: repository.list_job_task_rows(job_id))
        client = await get_temporal_client()
        payload = ProcessXmlJobInput(
            job_id=str(job_id),
            tasks=[WorkflowTaskInput(task_id=task.id, url=task.url, queue=queue_for_url(task.url)) for task in tasks],
        )

        try:
            handle = await client.start_workflow(
                ProcessXmlJobWorkflow.run,
                payload,
                id=str(job_id),
                task_queue=self._settings.temporal_workflow_task_queue,
            )
            run_id = self._workflow_run_id(handle)
            if run_id:
                await self._run(lambda repository: repository.set_temporal_run_id(job_id, run_id))
            log_event(
                logger,
                logging.INFO,
                "job.temporal_workflow.started",
                job_id=job_id,
                task_count=len(tasks),
                temporal_run_id=run_id,
            )
        except WorkflowAlreadyStartedError as exc:
            if exc.run_id:
                await self._run(lambda repository: repository.set_temporal_run_id(job_id, exc.run_id))
            log_event(
                logger,
                logging.WARNING,
                "job.temporal_workflow.already_started",
                job_id=job_id,
                task_count=len(tasks),
                temporal_run_id=exc.run_id,
            )

    @staticmethod
    def _workflow_run_id(handle: object) -> Optional[str]:
        for attribute in ("run_id", "first_execution_run_id", "result_run_id"):
            value = getattr(handle, attribute, None)
            if isinstance(value, str) and value:
                return value
        return None
