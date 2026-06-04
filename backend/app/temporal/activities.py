from __future__ import annotations

import time
import uuid

from temporalio import activity
from temporalio.exceptions import ApplicationError

from app.db.session import SessionLocal
from app.repositories.jobs import JobRepository
from app.services.xml_ingest import FeedFetchError, HttpClientError, MalformedXmlError, OversizedResponseError, extract_feed_records
from app.temporal.types import ProcessedTaskResult, URL_ACTIVITY_MAX_ATTEMPTS, WorkflowTaskInput


@activity.defn
async def set_job_running_activity(job_id: str) -> None:
    async with SessionLocal() as session:
        repository = JobRepository(session)
        await repository.mark_job_running(uuid.UUID(job_id))


@activity.defn
async def process_single_url_activity(job_id: str, task: WorkflowTaskInput) -> ProcessedTaskResult:
    queue = task.queue
    attempt_number = activity.info().attempt
    started_at = time.perf_counter()

    async with SessionLocal() as session:
        repository = JobRepository(session)
        await repository.mark_task_started(
            task.task_id,
            queue=queue,
            attempt_number=attempt_number,
        )

    try:
        records = await extract_feed_records(task.url, queue=queue)
    except (HttpClientError, MalformedXmlError, OversizedResponseError) as exc:
        duration_ms = _duration_ms(started_at)
        http_status = exc.status_code if isinstance(exc, HttpClientError) else None
        error_type = type(exc).__name__
        async with SessionLocal() as session:
            repository = JobRepository(session)
            await repository.complete_task_failure(
                task.task_id,
                queue=queue,
                error_type=error_type,
                error_message=str(exc),
                http_status=http_status,
                duration_ms=duration_ms,
                attempt_number=attempt_number,
            )
        return ProcessedTaskResult(
            task_id=task.task_id,
            status="failed",
            queue=queue,
            records_extracted=0,
        )
    except FeedFetchError as exc:
        duration_ms = _duration_ms(started_at)
        error_type = type(exc).__name__
        if attempt_number >= URL_ACTIVITY_MAX_ATTEMPTS:
            async with SessionLocal() as session:
                repository = JobRepository(session)
                await repository.complete_task_failure(
                    task.task_id,
                    queue=queue,
                    error_type=error_type,
                    error_message=str(exc),
                    http_status=None,
                    duration_ms=duration_ms,
                    attempt_number=attempt_number,
                )
            return ProcessedTaskResult(
                task_id=task.task_id,
                status="failed",
                queue=queue,
                records_extracted=0,
            )

        async with SessionLocal() as session:
            repository = JobRepository(session)
            await repository.mark_task_attempt_failed(
                task.task_id,
                queue=queue,
                error_type=error_type,
                error_message=str(exc),
                http_status=None,
                duration_ms=duration_ms,
                attempt_number=attempt_number,
            )
        raise ApplicationError(str(exc), type=error_type)

    duration_ms = _duration_ms(started_at)
    async with SessionLocal() as session:
        repository = JobRepository(session)
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


def _duration_ms(started_at: float) -> int:
    return int((time.perf_counter() - started_at) * 1000)


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
