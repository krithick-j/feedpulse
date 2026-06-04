from __future__ import annotations

import asyncio
import uuid

from app.db.session import SessionLocal
from app.repositories.jobs import JobRepository
from app.services.job_runtime import build_records, is_forbidden_url, queue_for_url, task_delay_seconds, task_duration_ms

_active_job_tasks: set[uuid.UUID] = set()


def schedule_job_simulation(job_id: uuid.UUID) -> None:
    if job_id in _active_job_tasks:
        return

    _active_job_tasks.add(job_id)
    asyncio.create_task(_run_job(job_id))


async def _run_job(job_id: uuid.UUID) -> None:
    try:
        async with SessionLocal() as session:
            repository = JobRepository(session)
            await repository.mark_job_running(job_id, temporal_run_id=f"local-run-{job_id.hex[:12]}")
            tasks = await repository.list_job_task_rows(job_id)

        semaphore = asyncio.Semaphore(8)

        async def process_task(task_id: int, url: str) -> None:
            async with semaphore:
                await _process_single_task(job_id, task_id, url)

        await asyncio.gather(*(process_task(task.id, task.url) for task in tasks))

        async with SessionLocal() as session:
            repository = JobRepository(session)
            await repository.finalize_job(job_id)
    finally:
        _active_job_tasks.discard(job_id)


async def _process_single_task(job_id: uuid.UUID, task_id: int, url: str) -> None:
    queue = queue_for_url(url)
    attempt_number = 1

    async with SessionLocal() as session:
        repository = JobRepository(session)
        await repository.mark_task_started(task_id, queue=queue, attempt_number=attempt_number)

    await asyncio.sleep(task_delay_seconds(task_id, url))

    async with SessionLocal() as session:
        repository = JobRepository(session)
        duration_ms = task_duration_ms(task_id)

        if is_forbidden_url(url):
            await repository.complete_task_failure(
                task_id,
                queue=queue,
                error_type="HttpClientError",
                error_message="403 response while fetching feed",
                http_status=403,
                duration_ms=duration_ms,
                attempt_number=attempt_number,
            )
            await repository.finalize_job(job_id)
            return

        records = build_records(url, task_id)
        await repository.complete_task_success(
            task_id,
            queue=queue,
            records=records,
            duration_ms=duration_ms,
            attempt_number=attempt_number,
        )
        await repository.finalize_job(job_id)
