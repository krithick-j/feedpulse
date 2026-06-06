"""Pure mappers from a JobProjection to SSE event payloads.

Kept separate from JobService so the presentation/shape logic has a single
responsibility and can be tested without any orchestration or I/O.
"""
from __future__ import annotations

from app.dto.jobs import JobProjection

TERMINAL_STATUSES = {"completed", "completed_with_failures", "failed"}


def job_snapshot_payload(projection: JobProjection, event_type: str) -> dict:
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


def task_payload_map(projection: JobProjection) -> dict[int, dict]:
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


def projection_delta_events(
    projection: JobProjection,
    *,
    previous_snapshot: dict,
    previous_tasks: dict[int, dict],
) -> tuple[list[dict], dict, dict[int, dict]]:
    current_tasks = task_payload_map(projection)
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

    snapshot = job_snapshot_payload(projection, "job.progress")
    if snapshot != previous_snapshot:
        delta_events.append(snapshot)

    return delta_events, snapshot, current_tasks


def is_terminal(projection: JobProjection) -> bool:
    return projection.job.status in TERMINAL_STATUSES


def sort_mock_tasks(tasks, sort_by: str):
    if sort_by == "status":
        return sorted(tasks, key=lambda task: (task.status, task.url))
    if sort_by == "duration":
        return sorted(tasks, key=lambda task: (-(task.duration_ms or -1), task.url))
    if sort_by == "records":
        return sorted(tasks, key=lambda task: (-task.records_extracted, task.url))
    if sort_by == "attempts":
        return sorted(tasks, key=lambda task: (-task.attempt_count, task.url))
    return sorted(tasks, key=lambda task: (task.url, task.id))
