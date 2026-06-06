from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import AsyncIterator, List, Optional

from temporalio.exceptions import WorkflowAlreadyStartedError

from app.core.logging import log_event
from app.core.settings import get_settings
from app.data.mock_store import store
from app.data.xml_sources import load_source_urls
from app.db.enums import TaskStatus as DbTaskStatus
from app.db.notifications import JobEventListener
from app.db.session import get_db_session
from app.repositories.jobs import JobRepository
from app.schemas.jobs import (
    JobDetail,
    JobProjection,
    JobSummary,
    PaginatedExtractedRecords,
    StartJobResponse,
    TaskDetail,
    TaskSummary,
)
from app.services.job_runtime import queue_for_url
from app.services.job_simulator import schedule_job_simulation
from app.temporal.client import get_temporal_client
from app.temporal.types import ProcessXmlJobInput, WorkflowTaskInput
from app.temporal.workflows import ProcessXmlJobWorkflow

settings = get_settings()
logger = logging.getLogger(__name__)

SSE_KEEPALIVE_SECONDS = 15.0


class SimulatorRuntimeDisabledError(Exception):
    """Raised when a job needs the simulator runtime but it is disabled."""


# ---------------------------------------------------------------------------
# Job collection / lifecycle
# ---------------------------------------------------------------------------
async def list_jobs() -> List[JobSummary]:
    if settings.data_backend == "mock":
        return store.list_jobs()

    return await _with_repository(lambda repository: repository.list_job_summaries())


async def start_job(idempotency_key: Optional[str]) -> StartJobResponse:
    if settings.data_backend == "mock":
        return store.start_job(idempotency_key)

    resolved_key = idempotency_key or f"api-{time.time_ns()}"
    response = await _with_repository(
        lambda repository: repository.create_job_with_tasks(
            idempotency_key=resolved_key,
            urls=load_source_urls(),
        )
    )
    if not response.reused:
        if settings.job_execution_backend == "temporal":
            await _start_temporal_job(uuid.UUID(response.job_id))
        else:
            if not settings.enable_simulator_runtime:
                raise SimulatorRuntimeDisabledError
            schedule_job_simulation(uuid.UUID(response.job_id))

    log_event(
        logger,
        logging.INFO,
        "job.start.accepted",
        job_id=response.job_id,
        idempotency_key=resolved_key,
        reused=response.reused,
        execution_backend=settings.job_execution_backend,
    )
    return response


async def get_job(job_id: str) -> Optional[JobDetail]:
    if settings.data_backend == "mock":
        return store.get_job(job_id)

    job_uuid = _try_parse_job_id(job_id)
    if job_uuid is None:
        return None
    return await _with_repository(lambda repository: repository.get_job_detail(job_uuid))


# ---------------------------------------------------------------------------
# Tasks / records
# ---------------------------------------------------------------------------
async def list_tasks(
    job_id: str,
    status_filter: Optional[DbTaskStatus],
    sort_by: str,
) -> Optional[List[TaskSummary]]:
    if settings.data_backend == "mock":
        tasks = store.get_tasks(job_id)
        if tasks is None:
            return None
        if status_filter is not None:
            tasks = [task for task in tasks if task.status == status_filter.value]
        return _sort_mock_tasks(tasks, sort_by)

    job_uuid = _try_parse_job_id(job_id)
    if job_uuid is None:
        return None
    return await _with_repository(
        lambda repository: repository.list_task_summaries(
            job_uuid,
            status_filter=status_filter,
            sort_by=sort_by,
        )
    )


async def get_task(job_id: str, task_id: int) -> Optional[TaskDetail]:
    if settings.data_backend == "mock":
        return store.get_task(job_id, task_id)

    job_uuid = _try_parse_job_id(job_id)
    if job_uuid is None:
        return None
    return await _with_repository(lambda repository: repository.get_task_detail(job_uuid, task_id))


async def list_task_records(
    job_id: str,
    task_id: int,
    *,
    limit: int,
    offset: int,
) -> Optional[PaginatedExtractedRecords]:
    if settings.data_backend == "mock":
        records = store.get_task_records(job_id, task_id, limit=limit, offset=offset)
        return records or None

    job_uuid = _try_parse_job_id(job_id)
    if job_uuid is None:
        return None
    return await _with_repository(
        lambda repository: repository.list_task_records(
            job_uuid,
            task_id,
            limit=limit,
            offset=offset,
        )
    )


# ---------------------------------------------------------------------------
# Event streaming (yields payload dicts; None signals a keepalive frame)
# ---------------------------------------------------------------------------
async def job_event_stream_available(job_id: str) -> bool:
    if settings.data_backend == "mock":
        return store.snapshot_event(job_id) is not None

    job_uuid = _try_parse_job_id(job_id)
    if job_uuid is None:
        return False
    projection = await _with_repository(lambda repository: repository.get_job_projection(job_uuid))
    return projection is not None


async def job_event_stream(job_id: str) -> AsyncIterator[Optional[dict]]:
    if settings.data_backend == "mock":
        async for payload in _mock_event_stream(job_id):
            yield payload
        return

    async for payload in _database_event_stream(job_id):
        yield payload


async def _mock_event_stream(job_id: str) -> AsyncIterator[Optional[dict]]:
    snapshot = store.snapshot_event(job_id)
    if not snapshot:
        return

    yield snapshot.model_dump()

    while True:
        await asyncio.sleep(1.5)
        events = store.advance_job(job_id)
        if not events:
            break

        for event in events:
            yield event.model_dump()

        if any(event.type == "job.completed" for event in events if hasattr(event, "type")):
            break


async def _database_event_stream(job_id: str) -> AsyncIterator[Optional[dict]]:
    job_uuid = _try_parse_job_id(job_id)
    if job_uuid is None:
        return

    async with JobEventListener(job_id=job_id) as listener:
        initial_projection = await _with_repository(lambda repository: repository.get_job_projection(job_uuid))
        if initial_projection is None:
            return

        last_snapshot = _job_snapshot_payload(initial_projection, "job.snapshot")
        last_tasks = _task_payload_map(initial_projection)
        yield last_snapshot

        if _job_is_terminal(initial_projection):
            yield _job_snapshot_payload(initial_projection, "job.completed")
            return

        while True:
            notification = await listener.next_event(timeout=SSE_KEEPALIVE_SECONDS)
            projection = await _with_repository(lambda repository: repository.get_job_projection(job_uuid))
            if projection is None:
                return

            delta_events, snapshot, current_tasks = _projection_delta_events(
                projection,
                previous_snapshot=last_snapshot,
                previous_tasks=last_tasks,
            )

            for event in delta_events:
                yield event

            last_snapshot = snapshot
            last_tasks = current_tasks

            if _job_is_terminal(projection):
                yield _job_snapshot_payload(projection, "job.completed")
                return

            if notification is None and not delta_events:
                yield None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
async def _start_temporal_job(job_id: uuid.UUID) -> None:
    tasks = await _with_repository(lambda repository: repository.list_job_task_rows(job_id))
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
            task_queue=settings.temporal_workflow_task_queue,
        )
        run_id = _workflow_run_id(handle)
        if run_id:
            await _with_repository(lambda repository: repository.set_temporal_run_id(job_id, run_id))
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
            await _with_repository(lambda repository: repository.set_temporal_run_id(job_id, exc.run_id))
        log_event(
            logger,
            logging.WARNING,
            "job.temporal_workflow.already_started",
            job_id=job_id,
            task_count=len(tasks),
            temporal_run_id=exc.run_id,
        )


def _workflow_run_id(handle: object) -> Optional[str]:
    for attribute in ("run_id", "first_execution_run_id", "result_run_id"):
        value = getattr(handle, attribute, None)
        if isinstance(value, str) and value:
            return value
    return None


def _try_parse_job_id(job_id: str) -> Optional[uuid.UUID]:
    try:
        return uuid.UUID(job_id)
    except ValueError:
        return None


def _sort_mock_tasks(tasks: List[TaskSummary], sort_by: str) -> List[TaskSummary]:
    if sort_by == "status":
        return sorted(tasks, key=lambda task: (task.status, task.url))
    if sort_by == "duration":
        return sorted(tasks, key=lambda task: (-(task.duration_ms or -1), task.url))
    if sort_by == "records":
        return sorted(tasks, key=lambda task: (-task.records_extracted, task.url))
    if sort_by == "attempts":
        return sorted(tasks, key=lambda task: (-task.attempt_count, task.url))
    return sorted(tasks, key=lambda task: (task.url, task.id))


def _job_snapshot_payload(projection: JobProjection, event_type: str) -> dict:
    return {
        "type": event_type,
        "payload": {
            "job_id": projection.job.id,
            "counts": {
                "pending": projection.job.counts.pending,
                "in_progress": projection.job.counts.in_progress,
                "completed": projection.job.counts.completed,
                "failed": projection.job.counts.failed,
            },
            "elapsed_ms": projection.job.elapsed_ms,
            "status": projection.job.status,
        },
    }


def _task_payload_map(projection: JobProjection) -> dict[int, dict]:
    return {
        task.id: {
            "id": task.id,
            "url": task.url,
            "status": task.status,
            "queue": task.queue,
            "attempt_count": task.attempt_count,
            "records_extracted": task.records_extracted,
            "duration_ms": task.duration_ms,
            "last_error": task.last_error,
            "last_error_type": task.last_error_type,
            "started_at": task.started_at,
            "finished_at": task.finished_at,
        }
        for task in projection.task_summaries
    }


def _projection_delta_events(
    projection: JobProjection,
    *,
    previous_snapshot: dict,
    previous_tasks: dict[int, dict],
) -> tuple[list[dict], dict, dict[int, dict]]:
    current_tasks = _task_payload_map(projection)
    delta_events: list[dict] = []

    changed_task_ids = [
        task_id
        for task_id, payload in current_tasks.items()
        if previous_tasks.get(task_id) != payload
    ]
    for task_id in changed_task_ids:
        delta_events.append(
            {
                "type": "task.updated",
                "payload": {
                    "job_id": projection.job.id,
                    "task": current_tasks[task_id],
                },
            }
        )

    snapshot = _job_snapshot_payload(projection, "job.progress")
    if snapshot != previous_snapshot:
        delta_events.append(snapshot)

    return delta_events, snapshot, current_tasks


def _job_is_terminal(projection: JobProjection) -> bool:
    return projection.job.status in {"completed", "completed_with_failures", "failed"}


async def _with_repository(operation: Callable[[JobRepository], Awaitable]):
    async for session in get_db_session():
        repository = JobRepository(session)
        return await operation(repository)

    raise RuntimeError("Database session could not be created")
