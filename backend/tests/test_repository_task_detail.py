from __future__ import annotations

import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

from app.db.enums import AttemptStatus, TaskStatus
from app.repositories.jobs import JobRepository


class FakeScalarResult:
    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value


class FakeScalarsResult:
    def __init__(self, values):
        self._values = values

    def scalars(self):
        return self

    def all(self):
        return self._values


class RepositoryTaskDetailTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_task_detail_maps_attempts_and_records_without_duplicate_kwargs(self) -> None:
        now = datetime(2026, 6, 5, 12, 0, tzinfo=timezone.utc)
        task = SimpleNamespace(
            id=12,
            url="https://example.com/failure.xml",
            status=TaskStatus.FAILED,
            queue="xml-small-queue",
            attempt_count=1,
            records_extracted=0,
            duration_ms=321,
            last_error="403 response while fetching feed",
            last_error_type="HttpClientError",
            started_at=now,
            finished_at=now,
        )
        attempt = SimpleNamespace(
            attempt_number=1,
            status=AttemptStatus.FAILED,
            started_at=now,
            finished_at=now,
            duration_ms=321,
            http_status=403,
            error_type="HttpClientError",
            error_message="403 response while fetching feed",
        )
        record = SimpleNamespace(
            id="00000000-0000-0000-0000-000000000001",
            title="Ignored",
            link="https://example.com/item",
            published_at=now,
            author="author",
            summary="summary",
        )

        session = AsyncMock()
        session.execute.side_effect = [
            FakeScalarResult(task),
            FakeScalarsResult([attempt]),
            FakeScalarsResult([record]),
        ]
        repository = JobRepository(session)

        detail = await repository.get_task_detail(
            job_id=SimpleNamespace(),
            task_id=12,
        )

        self.assertIsNotNone(detail)
        assert detail is not None
        self.assertEqual(detail.id, 12)
        self.assertEqual(detail.status, "failed")
        self.assertEqual(detail.attempts[0].status, "failed")
        self.assertEqual(detail.attempts[0].http_status, 403)
        self.assertEqual(detail.sample_records[0].link, "https://example.com/item")
