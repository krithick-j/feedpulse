from __future__ import annotations

import unittest
import uuid
from unittest.mock import AsyncMock, Mock

from app.dto.jobs import StartJobRequest, StartJobResponse
from app.services.jobs import JobService


JOB_ID = "11111111-1111-4111-8111-111111111111"


def make_service(*, response: StartJobResponse, executor):
    gateway = Mock()
    gateway.create_job = AsyncMock(return_value=response)
    return JobService(gateway=gateway, executor=executor)


class JobStartTests(unittest.IsolatedAsyncioTestCase):
    def test_start_job_request_accepts_camel_case_idempotency_key(self) -> None:
        payload = StartJobRequest.model_validate({"idempotencyKey": "ui-123"})
        self.assertEqual(payload.idempotency_key, "ui-123")

    async def test_executes_a_new_job(self) -> None:
        executor = AsyncMock()
        service = make_service(response=StartJobResponse(job_id=JOB_ID, reused=False), executor=executor)

        result = await service.start_job(None)

        self.assertEqual(result.job_id, JOB_ID)
        executor.start.assert_awaited_once_with(uuid.UUID(JOB_ID))

    async def test_does_not_execute_a_reused_job(self) -> None:
        executor = AsyncMock()
        service = make_service(response=StartJobResponse(job_id=JOB_ID, reused=True), executor=executor)

        result = await service.start_job("ui-123")

        self.assertTrue(result.reused)
        executor.start.assert_not_awaited()
