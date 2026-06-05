from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import AsyncIterator, List, Optional

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from temporalio.exceptions import WorkflowAlreadyStartedError

from app.core.settings import get_settings
from app.data.mock_store import store
from app.data.source_manifest import load_source_urls
from app.db.enums import TaskStatus as DbTaskStatus
from app.db.notifications import JobEventListener
from app.db.session import get_db_session
from app.repositories.jobs import JobRepository
from app.schemas.jobs import ExtractedRecord, JobDetail, JobProjection, JobSummary, StartJobRequest, StartJobResponse, TaskDetail, TaskSummary
from app.services.job_runtime import queue_for_url
from app.services.job_simulator import schedule_job_simulation
from app.temporal.client import get_temporal_client
from app.temporal.workflows import ProcessXmlJobWorkflow
from app.temporal.types import ProcessXmlJobInput, WorkflowTaskInput

router = APIRouter(prefix="/api/v1", tags=["jobs"])
settings = get_settings()
SSE_KEEPALIVE_SECONDS = 15.0


@router.get("/jobs", response_model=List[JobSummary])
async def list_jobs() -> List[JobSummary]:
    if settings.data_backend == "mock":
        return store.list_jobs()

    return await _with_repository(lambda repository: repository.list_job_summaries())


@router.post("/jobs", response_model=StartJobResponse, status_code=status.HTTP_202_ACCEPTED)
async def start_job(
    payload: Optional[StartJobRequest] = None,
) -> StartJobResponse:
    if settings.data_backend == "mock":
        return store.start_job(payload.idempotency_key if payload else None)

    idempotency_key = payload.idempotency_key if payload and payload.idempotency_key else f"api-{time.time_ns()}"
    response = await _with_repository(
        lambda repository: repository.create_job_with_tasks(
            idempotency_key=idempotency_key,
            urls=load_source_urls(),
        )
    )
    if not response.reused:
        if settings.job_execution_backend == "temporal":
            await _start_temporal_job(uuid.UUID(response.job_id))
        else:
            _ensure_simulator_runtime_enabled()
            schedule_job_simulation(uuid.UUID(response.job_id))
    return response


@router.get("/jobs/{job_id}", response_model=JobDetail)
async def get_job(job_id: str) -> JobDetail:
    if settings.data_backend == "mock":
        job = store.get_job(job_id)
        if not job:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
        return job

    job = await _with_repository(lambda repository: repository.get_job_detail(_parse_job_id(job_id)))
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return job


@router.get("/jobs/{job_id}/tasks", response_model=List[TaskSummary])
async def get_tasks(
    job_id: str,
    status_filter: Optional[str] = Query(default=None, alias="status"),
    sort_by: str = Query(default="url", alias="sort"),
) -> List[TaskSummary]:
    if settings.data_backend == "mock":
        tasks = store.get_tasks(job_id)
        if tasks is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
        if status_filter is not None:
            tasks = [task for task in tasks if task.status == status_filter]
        tasks = _sort_mock_tasks(tasks, sort_by)
        return tasks

    parsed_status_filter = _parse_task_status_filter(status_filter)
    tasks = await _with_repository(
        lambda repository: repository.list_task_summaries(
            _parse_job_id(job_id),
            status_filter=parsed_status_filter,
            sort_by=sort_by,
        )
    )
    if tasks is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return tasks


@router.get("/jobs/{job_id}/tasks/{task_id}", response_model=TaskDetail)
async def get_task(job_id: str, task_id: int) -> TaskDetail:
    if settings.data_backend == "mock":
        task = store.get_task(job_id, task_id)
        if not task:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
        return task

    task = await _with_repository(lambda repository: repository.get_task_detail(_parse_job_id(job_id), task_id))
    if not task:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
    return task


@router.get("/jobs/{job_id}/tasks/{task_id}/records", response_model=List[ExtractedRecord])
async def get_task_records(
    job_id: str,
    task_id: int,
) -> List[ExtractedRecord]:
    if settings.data_backend == "mock":
        task = store.get_task(job_id, task_id)
        if not task:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
        return task.sample_records

    records = await _with_repository(lambda repository: repository.list_task_records(_parse_job_id(job_id), task_id))
    if records is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
    return records


@router.get("/jobs/{job_id}/events")
async def stream_job_events(job_id: str) -> StreamingResponse:
    if settings.data_backend == "mock":
        snapshot = store.snapshot_event(job_id)
        if not snapshot:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

        async def event_stream():
            yield _encode_sse(snapshot.model_dump())

            while True:
                await asyncio.sleep(1.5)
                events = store.advance_job(job_id)

                if not events:
                    break

                for event in events:
                    yield _encode_sse(event.model_dump())

                if any(event.type == "job.completed" for event in events if hasattr(event, "type")):
                    break

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    parsed_job_id = _parse_job_id(job_id)
    existing_projection = await _with_repository(lambda repository: repository.get_job_projection(parsed_job_id))
    if existing_projection is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")

    async def event_stream() -> AsyncIterator[str]:
        async with JobEventListener(job_id=job_id) as listener:
            initial_projection = await _with_repository(lambda repository: repository.get_job_projection(parsed_job_id))
            if initial_projection is None:
                return

            last_snapshot = _job_snapshot_payload(initial_projection, "job.snapshot")
            last_tasks = _task_payload_map(initial_projection)
            yield _encode_sse(last_snapshot)

            if _job_is_terminal(initial_projection):
                yield _encode_sse(_job_snapshot_payload(initial_projection, "job.completed"))
                return

            while True:
                notification = await listener.next_event(timeout=SSE_KEEPALIVE_SECONDS)
                if notification is None:
                    yield ": keepalive\n\n"
                    continue

                projection = await _with_repository(lambda repository: repository.get_job_projection(parsed_job_id))
                if projection is None:
                    return

                delta_events, snapshot, current_tasks = _projection_delta_events(
                    projection,
                    previous_snapshot=last_snapshot,
                    previous_tasks=last_tasks,
                )

                for event in delta_events:
                    yield _encode_sse(event)

                last_snapshot = snapshot
                last_tasks = current_tasks

                if _job_is_terminal(projection):
                    yield _encode_sse(_job_snapshot_payload(projection, "job.completed"))
                    return

    return StreamingResponse(event_stream(), media_type="text/event-stream")


def _encode_sse(payload: dict) -> str:
    return "data: {0}\n\n".format(json.dumps(payload))


def _parse_job_id(job_id: str) -> uuid.UUID:
    try:
        return uuid.UUID(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found") from exc


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
    except WorkflowAlreadyStartedError as exc:
        if exc.run_id:
            await _with_repository(lambda repository: repository.set_temporal_run_id(job_id, exc.run_id))


def _workflow_run_id(handle: object) -> Optional[str]:
    for attribute in ("run_id", "first_execution_run_id", "result_run_id"):
        value = getattr(handle, attribute, None)
        if isinstance(value, str) and value:
            return value
    return None


def _ensure_simulator_runtime_enabled() -> None:
    if settings.enable_simulator_runtime:
        return

    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Simulator runtime is disabled unless ENABLE_SIMULATOR_RUNTIME=true",
    )


def _parse_task_status_filter(value: Optional[str]) -> Optional[DbTaskStatus]:
    if value is None or value == "all":
        return None
    try:
        return DbTaskStatus(value)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid task status filter") from exc


def _sort_mock_tasks(tasks: list[TaskSummary], sort_by: str) -> list[TaskSummary]:
    if sort_by == "status":
        return sorted(tasks, key=lambda task: (task.status, task.url))
    if sort_by == "duration":
        return sorted(tasks, key=lambda task: (-(task.duration_ms or -1), task.url))
    if sort_by == "records":
        return sorted(tasks, key=lambda task: (-task.records_extracted, task.url))
    return sorted(tasks, key=lambda task: (task.url, task.id))
