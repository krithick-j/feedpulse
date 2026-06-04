from __future__ import annotations

import asyncio
import uuid

from temporalio import activity

from app.db.session import SessionLocal
from app.repositories.jobs import JobRepository
from app.services.job_runtime import build_records, is_forbidden_url, task_delay_seconds, task_duration_ms
from app.temporal.types import ProcessedTaskResult, WorkflowTaskInput


@activity.defn
async def set_job_running_activity(job_id: str) -> None:
    async with SessionLocal() as session:
        repository = JobRepository(session)
        await repository.mark_job_running(uuid.UUID(job_id))


@activity.defn
async def process_single_url_activity(job_id: str, task: WorkflowTaskInput) -> ProcessedTaskResult:
    queue = task.queue
    attempt_number = activity.info().attempt

    async with SessionLocal() as session:
        repository = JobRepository(session)
        await repository.mark_task_started(
            task.task_id,
            queue=queue,
            attempt_number=attempt_number,
        )

    await asyncio.sleep(task_delay_seconds(task.task_id, task.url))

    async with SessionLocal() as session:
        repository = JobRepository(session)
        duration_ms = task_duration_ms(task.task_id)

        if is_forbidden_url(task.url):
            await repository.complete_task_failure(
                task.task_id,
                queue=queue,
                error_type="HttpClientError",
                error_message="403 response while fetching feed",
                http_status=403,
                duration_ms=duration_ms,
                attempt_number=attempt_number,
            )
            return ProcessedTaskResult(
                task_id=task.task_id,
                status="failed",
                queue=queue,
                records_extracted=0,
            )

        records = build_records(task.url, task.task_id)
        await repository.complete_task_success(
            task.task_id,
            queue=queue,
            records=records,
            duration_ms=duration_ms,
            attempt_number=attempt_number,
        )
        return ProcessedTaskResult(
            task_id=task.task_id,
            status="completed",
            queue=queue,
            records_extracted=len(records),
        )


@activity.defn
async def fail_incomplete_tasks_activity(job_id: str, error_type: str, error_message: str) -> None:
    async with SessionLocal() as session:
        repository = JobRepository(session)
        await repository.fail_incomplete_tasks(
            uuid.UUID(job_id),
            error_type=error_type,
            error_message=error_message,
        )


@activity.defn
async def finalize_job_activity(job_id: str) -> None:
    async with SessionLocal() as session:
        repository = JobRepository(session)
        await repository.finalize_job(uuid.UUID(job_id))
