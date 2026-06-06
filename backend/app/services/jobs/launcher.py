from __future__ import annotations

import logging
import uuid
from typing import Optional

from app.core.logging import log_event
from app.dto.jobs import StartJobResponse
from app.services.jobs.executor import TemporalJobExecutor
from app.services.jobs.gateway import JobRepositoryGateway

logger = logging.getLogger(__name__)


class JobLauncher:
    """Creates a job and hands it to the execution strategy."""

    def __init__(self, gateway: JobRepositoryGateway, executor: TemporalJobExecutor) -> None:
        self._gateway = gateway
        self._executor = executor

    async def start_job(self, idempotency_key: Optional[str]) -> StartJobResponse:
        response = await self._gateway.create_job(idempotency_key)
        if not response.reused:
            await self._executor.start(uuid.UUID(response.job_id))

        log_event(
            logger,
            logging.INFO,
            "job.start.accepted",
            job_id=response.job_id,
            reused=response.reused,
        )
        return response
