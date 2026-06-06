from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from fastapi import HTTPException

from app.api.handlers.jobs import JobHandler
from app.dto.jobs import StartJobRequest, StartJobResponse
from app.services.jobs import JobService


JOB_ID = "11111111-1111-4111-8111-111111111111"


def make_handler(*, job_execution_backend, enable_simulator_runtime, response, simulator, temporal):
    service = JobService(
        settings=SimpleNamespace(
            data_backend="database",
            job_execution_backend=job_execution_backend,
            enable_simulator_runtime=enable_simulator_runtime,
        ),
        run_repository=AsyncMock(return_value=response),
        schedule_simulation=simulator,
        temporal_starter=temporal,
    )
    return JobHandler(service=service)


class JobStartRuntimeTests(unittest.IsolatedAsyncioTestCase):
    def test_start_job_request_accepts_camel_case_idempotency_key(self) -> None:
        payload = StartJobRequest.model_validate({"idempotencyKey": "ui-123"})

        self.assertEqual(payload.idempotency_key, "ui-123")

    async def test_start_job_rejects_simulator_when_not_explicitly_enabled(self) -> None:
        simulator, temporal = Mock(), AsyncMock()
        handler = make_handler(
            job_execution_backend="simulator",
            enable_simulator_runtime=False,
            response=StartJobResponse(job_id=JOB_ID, reused=False),
            simulator=simulator,
            temporal=temporal,
        )

        with self.assertRaises(HTTPException) as raised:
            await handler.start_job(None)

        self.assertEqual(raised.exception.status_code, 503)
        self.assertIn("Simulator runtime is disabled", raised.exception.detail)
        simulator.assert_not_called()
        temporal.assert_not_awaited()

    async def test_start_job_allows_simulator_when_explicitly_enabled(self) -> None:
        simulator, temporal = Mock(), AsyncMock()
        handler = make_handler(
            job_execution_backend="simulator",
            enable_simulator_runtime=True,
            response=StartJobResponse(job_id=JOB_ID, reused=False),
            simulator=simulator,
            temporal=temporal,
        )

        result = await handler.start_job(None)

        self.assertEqual(result.job_id, JOB_ID)
        simulator.assert_called_once()
        temporal.assert_not_awaited()

    async def test_start_job_prefers_temporal_backend_by_default(self) -> None:
        simulator, temporal = Mock(), AsyncMock()
        handler = make_handler(
            job_execution_backend="temporal",
            enable_simulator_runtime=False,
            response=StartJobResponse(job_id=JOB_ID, reused=False),
            simulator=simulator,
            temporal=temporal,
        )

        result = await handler.start_job(None)

        self.assertEqual(result.job_id, JOB_ID)
        temporal.assert_awaited_once()
        simulator.assert_not_called()

    async def test_start_job_does_not_schedule_when_repository_reuses_job(self) -> None:
        simulator, temporal = Mock(), AsyncMock()
        handler = make_handler(
            job_execution_backend="temporal",
            enable_simulator_runtime=False,
            response=StartJobResponse(job_id=JOB_ID, reused=True),
            simulator=simulator,
            temporal=temporal,
        )

        result = await handler.start_job(StartJobRequest(idempotency_key="ui-123"))

        self.assertEqual(result.job_id, JOB_ID)
        self.assertTrue(result.reused)
        temporal.assert_not_awaited()
        simulator.assert_not_called()
