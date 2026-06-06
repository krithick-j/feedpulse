from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from app.api.handlers import jobs as job_handlers
from app.services.jobs import lifecycle as job_service
from app.dto.jobs import StartJobRequest, StartJobResponse


JOB_ID = "11111111-1111-4111-8111-111111111111"


class JobStartRuntimeTests(unittest.IsolatedAsyncioTestCase):
    def test_start_job_request_accepts_camel_case_idempotency_key(self) -> None:
        payload = StartJobRequest.model_validate({"idempotencyKey": "ui-123"})

        self.assertEqual(payload.idempotency_key, "ui-123")

    async def test_start_job_rejects_simulator_when_not_explicitly_enabled(self) -> None:
        response = StartJobResponse(job_id=JOB_ID, reused=False)

        with (
            patch.object(
                job_service,
                "settings",
                SimpleNamespace(
                    data_backend="database",
                    job_execution_backend="simulator",
                    enable_simulator_runtime=False,
                ),
            ),
            patch.object(job_service, "with_repository", new=AsyncMock(return_value=response)),
            patch.object(job_service, "schedule_job_simulation") as simulator_mock,
            patch.object(job_service, "_start_temporal_job", new=AsyncMock()) as temporal_mock,
        ):
            with self.assertRaises(HTTPException) as raised:
                await job_handlers.start_job(None)

        self.assertEqual(raised.exception.status_code, 503)
        self.assertIn("Simulator runtime is disabled", raised.exception.detail)
        simulator_mock.assert_not_called()
        temporal_mock.assert_not_awaited()

    async def test_start_job_allows_simulator_when_explicitly_enabled(self) -> None:
        response = StartJobResponse(job_id=JOB_ID, reused=False)

        with (
            patch.object(
                job_service,
                "settings",
                SimpleNamespace(
                    data_backend="database",
                    job_execution_backend="simulator",
                    enable_simulator_runtime=True,
                ),
            ),
            patch.object(job_service, "with_repository", new=AsyncMock(return_value=response)),
            patch.object(job_service, "schedule_job_simulation") as simulator_mock,
            patch.object(job_service, "_start_temporal_job", new=AsyncMock()) as temporal_mock,
        ):
            result = await job_handlers.start_job(None)

        self.assertEqual(result.job_id, JOB_ID)
        simulator_mock.assert_called_once()
        temporal_mock.assert_not_awaited()

    async def test_start_job_prefers_temporal_backend_by_default(self) -> None:
        response = StartJobResponse(job_id=JOB_ID, reused=False)

        with (
            patch.object(
                job_service,
                "settings",
                SimpleNamespace(
                    data_backend="database",
                    job_execution_backend="temporal",
                    enable_simulator_runtime=False,
                ),
            ),
            patch.object(job_service, "with_repository", new=AsyncMock(return_value=response)),
            patch.object(job_service, "_start_temporal_job", new=AsyncMock()) as temporal_mock,
            patch.object(job_service, "schedule_job_simulation") as simulator_mock,
        ):
            result = await job_handlers.start_job(None)

        self.assertEqual(result.job_id, JOB_ID)
        temporal_mock.assert_awaited_once()
        simulator_mock.assert_not_called()

    async def test_start_job_does_not_schedule_when_repository_reuses_job(self) -> None:
        response = StartJobResponse(job_id=JOB_ID, reused=True)

        with (
            patch.object(
                job_service,
                "settings",
                SimpleNamespace(
                    data_backend="database",
                    job_execution_backend="temporal",
                    enable_simulator_runtime=False,
                ),
            ),
            patch.object(job_service, "with_repository", new=AsyncMock(return_value=response)),
            patch.object(job_service, "_start_temporal_job", new=AsyncMock()) as temporal_mock,
            patch.object(job_service, "schedule_job_simulation") as simulator_mock,
        ):
            result = await job_handlers.start_job(StartJobRequest(idempotency_key="ui-123"))

        self.assertEqual(result.job_id, JOB_ID)
        self.assertTrue(result.reused)
        temporal_mock.assert_not_awaited()
        simulator_mock.assert_not_called()
