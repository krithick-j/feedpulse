from __future__ import annotations

import unittest
import uuid
from unittest.mock import AsyncMock, Mock

from fastapi import HTTPException

from app.api.handlers.jobs import JobHandler
from app.dto.jobs import StartJobRequest, StartJobResponse
from app.services.jobs import (
    JobLauncher,
    JobService,
    SimulatorJobExecutor,
    SimulatorRuntimeDisabledError,
)


JOB_ID = "11111111-1111-4111-8111-111111111111"


def gateway_returning(response: StartJobResponse):
    gateway = Mock()
    gateway.create_job = AsyncMock(return_value=response)
    return gateway


class JobLauncherTests(unittest.IsolatedAsyncioTestCase):
    def test_start_job_request_accepts_camel_case_idempotency_key(self) -> None:
        payload = StartJobRequest.model_validate({"idempotencyKey": "ui-123"})
        self.assertEqual(payload.idempotency_key, "ui-123")

    async def test_launches_executor_for_a_new_job(self) -> None:
        executor = AsyncMock()
        launcher = JobLauncher(gateway_returning(StartJobResponse(job_id=JOB_ID, reused=False)), executor)

        result = await launcher.start_job(None)

        self.assertEqual(result.job_id, JOB_ID)
        executor.start.assert_awaited_once_with(uuid.UUID(JOB_ID))

    async def test_does_not_launch_executor_for_a_reused_job(self) -> None:
        executor = AsyncMock()
        launcher = JobLauncher(gateway_returning(StartJobResponse(job_id=JOB_ID, reused=True)), executor)

        result = await launcher.start_job(StartJobRequest(idempotency_key="ui-123").idempotency_key)

        self.assertTrue(result.reused)
        executor.start.assert_not_awaited()


class SimulatorExecutorTests(unittest.IsolatedAsyncioTestCase):
    async def test_raises_when_disabled(self) -> None:
        schedule = Mock()
        executor = SimulatorJobExecutor(enabled=False, schedule=schedule)

        with self.assertRaises(SimulatorRuntimeDisabledError):
            await executor.start(uuid.UUID(JOB_ID))
        schedule.assert_not_called()

    async def test_schedules_when_enabled(self) -> None:
        schedule = Mock()
        executor = SimulatorJobExecutor(enabled=True, schedule=schedule)

        await executor.start(uuid.UUID(JOB_ID))
        schedule.assert_called_once_with(uuid.UUID(JOB_ID))


class HandlerSimulatorDisabledTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_simulator_surfaces_as_503(self) -> None:
        launcher = JobLauncher(
            gateway_returning(StartJobResponse(job_id=JOB_ID, reused=False)),
            SimulatorJobExecutor(enabled=False, schedule=Mock()),
        )
        service = JobService(reader=Mock(), launcher=launcher, events=Mock())
        handler = JobHandler(service=service)

        with self.assertRaises(HTTPException) as raised:
            await handler.start_job(None)

        self.assertEqual(raised.exception.status_code, 503)
        self.assertIn("Simulator runtime is disabled", raised.exception.detail)
