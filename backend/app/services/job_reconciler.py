from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

from temporalio.api.enums.v1 import WorkflowExecutionStatus

from app.core.settings import get_settings
from app.db.session import SessionLocal
from app.repositories.jobs import JobRepository
from app.temporal.client import get_temporal_client


def reconciliation_enabled(settings=None) -> bool:
    current_settings = settings or get_settings()
    return (
        current_settings.data_backend == "database"
        and current_settings.job_execution_backend == "temporal"
    )


async def reconcile_running_jobs() -> int:
    settings = get_settings()
    if not reconciliation_enabled(settings):
        return 0

    async with SessionLocal() as session:
        repository = JobRepository(session)
        running_jobs = await repository.list_running_jobs()

    if not running_jobs:
        return 0

    client = await get_temporal_client()
    reconciled = 0
    stale_before = datetime.now(timezone.utc) - timedelta(
        seconds=settings.job_reconciliation_grace_seconds
    )

    for job in running_jobs:
        handle = client.get_workflow_handle(job.id, run_id=job.temporal_run_id or None)
        try:
            description = await handle.describe()
        except Exception:
            await _repair_job(
                job.id,
                error_type="WorkflowMissingError",
                error_message="Temporal workflow was not found during reconciliation",
            )
            reconciled += 1
            continue

        status_name = WorkflowExecutionStatus.Name(description.status)
        if description.status != WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING:
            await _repair_job(
                job.id,
                error_type="WorkflowClosedWithoutFinalization",
                error_message=f"Temporal workflow closed with status {status_name}",
            )
            reconciled += 1
            continue

        if (
            job.started_at is not None
            and datetime.fromisoformat(job.started_at) <= stale_before
            and job.counts.pending == job.total_urls
            and job.counts.in_progress == 0
            and job.counts.completed == 0
            and job.counts.failed == 0
            and description.history_length <= settings.job_reconciliation_pending_history_limit
        ):
            await handle.terminate(
                reason="Feedpulse reconciliation terminated a stuck running workflow"
            )
            await _repair_job(
                job.id,
                error_type="WorkflowStuckRunning",
                error_message="Temporal workflow was still running but had made no task progress within the reconciliation grace window",
            )
            reconciled += 1

    return reconciled


async def run_reconciliation_loop(
    *,
    stop_event: asyncio.Event,
    interval_seconds: int | None = None,
) -> None:
    settings = get_settings()
    if not reconciliation_enabled(settings):
        return

    interval = interval_seconds or settings.job_reconciliation_interval_seconds
    if interval <= 0:
        return

    while True:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
            return
        except asyncio.TimeoutError:
            await reconcile_running_jobs()


async def _repair_job(job_id: str, *, error_type: str, error_message: str) -> None:
    async with SessionLocal() as session:
        repository = JobRepository(session)
        parsed_job_id = uuid.UUID(job_id)
        await repository.fail_incomplete_tasks(
            parsed_job_id,
            error_type=error_type,
            error_message=error_message,
        )
        await repository.finalize_job(parsed_job_id)
