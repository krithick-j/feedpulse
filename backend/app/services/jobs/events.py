from __future__ import annotations

import asyncio
from typing import AsyncIterator, Optional

from app.core.settings import get_settings
from app.data.mock_store import store
from app.db.notifications import JobEventListener
from app.dto.jobs import JobProjection
from app.services.jobs._common import try_parse_job_id, with_repository

settings = get_settings()

SSE_KEEPALIVE_SECONDS = 15.0


async def job_event_stream_available(job_id: str) -> bool:
    if settings.data_backend == "mock":
        return store.snapshot_event(job_id) is not None

    job_uuid = try_parse_job_id(job_id)
    if job_uuid is None:
        return False
    projection = await with_repository(lambda repository: repository.get_job_projection(job_uuid))
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
    job_uuid = try_parse_job_id(job_id)
    if job_uuid is None:
        return

    async with JobEventListener(job_id=job_id) as listener:
        initial_projection = await with_repository(lambda repository: repository.get_job_projection(job_uuid))
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
            projection = await with_repository(lambda repository: repository.get_job_projection(job_uuid))
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
