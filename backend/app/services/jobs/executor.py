"""Temporal execution: start the workflow for a created job."""
from __future__ import annotations

import logging
import uuid
from typing import Optional

from temporalio.exceptions import WorkflowAlreadyStartedError

from app.core.logging import log_event
from app.services.job_runtime import queue_for_url
from app.services.jobs._common import with_repository
from app.temporal.client import get_temporal_client
from app.temporal.types import ProcessXmlJobInput, WorkflowTaskInput
from app.temporal.workflows import ProcessXmlJobWorkflow

logger = logging.getLogger(__name__)


class TemporalJobExecutor:
    def __init__(
        self,
        *,
        settings,
        run_repository=with_repository,
        client_getter=get_temporal_client,
        workflow=ProcessXmlJobWorkflow,
    ) -> None:
        self._settings = settings
        self._run = run_repository
        self._client_getter = client_getter
        self._workflow = workflow

    async def start(self, job_id: uuid.UUID) -> None:
        tasks = await self._run(lambda repository: repository.list_job_task_rows(job_id))
        client = await self._client_getter()
        payload = ProcessXmlJobInput(
            job_id=str(job_id),
            tasks=[
                WorkflowTaskInput(task_id=task.id, url=task.url, queue=queue_for_url(task.url))
                for task in tasks
            ],
        )

        try:
            handle = await client.start_workflow(
                self._workflow.run,
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
