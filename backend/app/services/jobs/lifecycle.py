from __future__ import annotations

import logging
import time
import uuid
from typing import List, Optional

from temporalio.exceptions import WorkflowAlreadyStartedError

from app.core.logging import log_event
from app.core.settings import get_settings
from app.data.mock_store import store
from app.data.xml_sources import load_source_urls
from app.dto.jobs import JobDetail, JobSummary, StartJobResponse
from app.services.job_runtime import queue_for_url
from app.services.job_simulator import schedule_job_simulation
from app.services.jobs._common import try_parse_job_id, with_repository
from app.temporal.client import get_temporal_client
from app.temporal.types import ProcessXmlJobInput, WorkflowTaskInput
from app.temporal.workflows import ProcessXmlJobWorkflow

settings = get_settings()
logger = logging.getLogger(__name__)


class SimulatorRuntimeDisabledError(Exception):
    """Raised when a job needs the simulator runtime but it is disabled."""


async def list_jobs() -> List[JobSummary]:
    if settings.data_backend == "mock":
        return store.list_jobs()

    return await with_repository(lambda repository: repository.list_job_summaries())


async def start_job(idempotency_key: Optional[str]) -> StartJobResponse:
    if settings.data_backend == "mock":
        return store.start_job(idempotency_key)

    resolved_key = idempotency_key or f"api-{time.time_ns()}"
    response = await with_repository(
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

    job_uuid = try_parse_job_id(job_id)
    if job_uuid is None:
        return None
    return await with_repository(lambda repository: repository.get_job_detail(job_uuid))


async def _start_temporal_job(job_id: uuid.UUID) -> None:
    tasks = await with_repository(lambda repository: repository.list_job_task_rows(job_id))
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
            await with_repository(lambda repository: repository.set_temporal_run_id(job_id, run_id))
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
            await with_repository(lambda repository: repository.set_temporal_run_id(job_id, exc.run_id))
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
