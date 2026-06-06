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

    def scalar_one(self):
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

    async def test_list_task_records_returns_pagination_metadata(self) -> None:
        now = datetime(2026, 6, 6, 12, 0, tzinfo=timezone.utc)
        record_one = SimpleNamespace(
            id="00000000-0000-0000-0000-000000000001",
            title="First",
            link="https://example.com/first",
            published_at=now,
            author="author-1",
            summary="summary-1",
        )
        record_two = SimpleNamespace(
            id="00000000-0000-0000-0000-000000000002",
            title="Second",
            link="https://example.com/second",
            published_at=now,
            author="author-2",
            summary="summary-2",
        )

        session = AsyncMock()
        session.execute.side_effect = [
            FakeScalarResult(12),
            FakeScalarResult(7),
            FakeScalarsResult([record_one, record_two]),
        ]
        repository = JobRepository(session)

        page = await repository.list_task_records(
            job_id=SimpleNamespace(),
            task_id=12,
            limit=2,
            offset=2,
        )

        self.assertIsNotNone(page)
        assert page is not None
        self.assertEqual(page.total, 7)
        self.assertEqual(page.limit, 2)
        self.assertEqual(page.offset, 2)
        self.assertTrue(page.has_more)
        self.assertEqual([record.link for record in page.items], [
            "https://example.com/first",
            "https://example.com/second",
        ])
