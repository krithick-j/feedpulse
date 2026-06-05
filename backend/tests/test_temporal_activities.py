from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from temporalio.exceptions import ApplicationError

from app.services.xml_ingest import FeedFetchError, HttpClientError
from app.temporal.activities import process_single_url_activity
from app.temporal.types import URL_ACTIVITY_MAX_ATTEMPTS, WorkflowTaskInput


class FakeSessionContext:
    def __init__(self, session: object) -> None:
        self._session = session

    async def __aenter__(self) -> object:
        return self._session

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class ActivityRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def mark_task_started(
        self,
        task_id: int,
        *,
        queue: str,
        attempt_number: int,
    ) -> None:
        self.calls.append(
            (
                "mark_task_started",
                {
                    "task_id": task_id,
                    "queue": queue,
                    "attempt_number": attempt_number,
                },
            )
        )

    async def complete_task_success(
        self,
        task_id: int,
        *,
        queue: str,
        records: list[dict[str, object]],
        duration_ms: int,
        attempt_number: int,
    ) -> None:
        self.calls.append(
            (
                "complete_task_success",
                {
                    "task_id": task_id,
                    "queue": queue,
                    "records": records,
                    "duration_ms": duration_ms,
                    "attempt_number": attempt_number,
                },
            )
        )

    async def complete_task_failure(
        self,
        task_id: int,
        *,
        queue: str,
        error_type: str,
        error_message: str,
        http_status: int | None,
        duration_ms: int,
        attempt_number: int,
    ) -> None:
        self.calls.append(
            (
                "complete_task_failure",
                {
                    "task_id": task_id,
                    "queue": queue,
                    "error_type": error_type,
                    "error_message": error_message,
                    "http_status": http_status,
                    "duration_ms": duration_ms,
                    "attempt_number": attempt_number,
                },
            )
        )

    async def mark_task_attempt_failed(
        self,
        task_id: int,
        *,
        queue: str,
        error_type: str,
        error_message: str,
        http_status: int | None,
        duration_ms: int,
        attempt_number: int,
    ) -> None:
        self.calls.append(
            (
                "mark_task_attempt_failed",
                {
                    "task_id": task_id,
                    "queue": queue,
                    "error_type": error_type,
                    "error_message": error_message,
                    "http_status": http_status,
                    "duration_ms": duration_ms,
                    "attempt_number": attempt_number,
                },
            )
        )


class TemporalActivityTests(unittest.IsolatedAsyncioTestCase):
    async def test_process_single_url_activity_records_success(self) -> None:
        repository = ActivityRepository()
        task = WorkflowTaskInput(
            task_id=42,
            url="https://example.com/feed.xml",
            queue="xml-small-queue",
        )
        records = [
            {"title": "One", "dedupe_key": "one"},
            {"title": "Two", "dedupe_key": "two"},
        ]

        with (
            patch("app.temporal.activities.SessionLocal", return_value=FakeSessionContext(object())),
            patch("app.temporal.activities.JobRepository", return_value=repository),
            patch("app.temporal.activities.activity.info", return_value=SimpleNamespace(attempt=1)),
            patch("app.temporal.activities._duration_ms", return_value=17),
            patch(
                "app.temporal.activities.extract_feed_records",
                new=AsyncMock(return_value=records),
            ),
        ):
            result = await process_single_url_activity(
                "11111111-1111-4111-8111-111111111111",
                task,
            )

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.records_extracted, 2)
        self.assertEqual(result.queue, "xml-small-queue")
        self.assertEqual(
            repository.calls,
            [
                (
                    "mark_task_started",
                    {
                        "task_id": 42,
                        "queue": "xml-small-queue",
                        "attempt_number": 1,
                    },
                ),
                (
                    "complete_task_success",
                    {
                        "task_id": 42,
                        "queue": "xml-small-queue",
                        "records": records,
                        "duration_ms": 17,
                        "attempt_number": 1,
                    },
                ),
            ],
        )

    async def test_process_single_url_activity_marks_permanent_http_failure(self) -> None:
        repository = ActivityRepository()
        task = WorkflowTaskInput(
            task_id=7,
            url="https://example.com/forbidden.xml",
            queue="xml-large-queue",
        )

        with (
            patch("app.temporal.activities.SessionLocal", return_value=FakeSessionContext(object())),
            patch("app.temporal.activities.JobRepository", return_value=repository),
            patch("app.temporal.activities.activity.info", return_value=SimpleNamespace(attempt=1)),
            patch("app.temporal.activities._duration_ms", return_value=23),
            patch(
                "app.temporal.activities.extract_feed_records",
                new=AsyncMock(side_effect=HttpClientError(403, "403 response while fetching feed")),
            ),
        ):
            result = await process_single_url_activity(
                "11111111-1111-4111-8111-111111111111",
                task,
            )

        self.assertEqual(result.status, "failed")
        self.assertEqual(result.records_extracted, 0)
        self.assertEqual(
            repository.calls,
            [
                (
                    "mark_task_started",
                    {
                        "task_id": 7,
                        "queue": "xml-large-queue",
                        "attempt_number": 1,
                    },
                ),
                (
                    "complete_task_failure",
                    {
                        "task_id": 7,
                        "queue": "xml-large-queue",
                        "error_type": "HttpClientError",
                        "error_message": "403 response while fetching feed",
                        "http_status": 403,
                        "duration_ms": 23,
                        "attempt_number": 1,
                    },
                ),
            ],
        )

    async def test_process_single_url_activity_raises_retryable_error_before_last_attempt(self) -> None:
        repository = ActivityRepository()
        task = WorkflowTaskInput(
            task_id=9,
            url="https://example.com/flaky.xml",
            queue="xml-small-queue",
        )

        with (
            patch("app.temporal.activities.SessionLocal", return_value=FakeSessionContext(object())),
            patch("app.temporal.activities.JobRepository", return_value=repository),
            patch("app.temporal.activities.activity.info", return_value=SimpleNamespace(attempt=2)),
            patch("app.temporal.activities._duration_ms", return_value=31),
            patch(
                "app.temporal.activities.extract_feed_records",
                new=AsyncMock(side_effect=FeedFetchError("Timeout while fetching feed")),
            ),
        ):
            with self.assertRaises(ApplicationError) as raised:
                await process_single_url_activity(
                    "11111111-1111-4111-8111-111111111111",
                    task,
                )

        self.assertEqual(raised.exception.type, "FeedFetchError")
        self.assertIn("Timeout while fetching feed", str(raised.exception))
        self.assertEqual(
            repository.calls,
            [
                (
                    "mark_task_started",
                    {
                        "task_id": 9,
                        "queue": "xml-small-queue",
                        "attempt_number": 2,
                    },
                ),
                (
                    "mark_task_attempt_failed",
                    {
                        "task_id": 9,
                        "queue": "xml-small-queue",
                        "error_type": "FeedFetchError",
                        "error_message": "Timeout while fetching feed",
                        "http_status": None,
                        "duration_ms": 31,
                        "attempt_number": 2,
                    },
                ),
            ],
        )

    async def test_process_single_url_activity_marks_terminal_failure_on_last_attempt(self) -> None:
        repository = ActivityRepository()
        task = WorkflowTaskInput(
            task_id=11,
            url="https://example.com/still-flaky.xml",
            queue="xml-small-queue",
        )

        with (
            patch("app.temporal.activities.SessionLocal", return_value=FakeSessionContext(object())),
            patch("app.temporal.activities.JobRepository", return_value=repository),
            patch(
                "app.temporal.activities.activity.info",
                return_value=SimpleNamespace(attempt=URL_ACTIVITY_MAX_ATTEMPTS),
            ),
            patch("app.temporal.activities._duration_ms", return_value=45),
            patch(
                "app.temporal.activities.extract_feed_records",
                new=AsyncMock(side_effect=FeedFetchError("Timeout while fetching feed")),
            ),
        ):
            result = await process_single_url_activity(
                "11111111-1111-4111-8111-111111111111",
                task,
            )

        self.assertEqual(result.status, "failed")
        self.assertEqual(result.records_extracted, 0)
        self.assertEqual(
            repository.calls,
            [
                (
                    "mark_task_started",
                    {
                        "task_id": 11,
                        "queue": "xml-small-queue",
                        "attempt_number": URL_ACTIVITY_MAX_ATTEMPTS,
                    },
                ),
                (
                    "complete_task_failure",
                    {
                        "task_id": 11,
                        "queue": "xml-small-queue",
                        "error_type": "FeedFetchError",
                        "error_message": "Timeout while fetching feed",
                        "http_status": None,
                        "duration_ms": 45,
                        "attempt_number": URL_ACTIVITY_MAX_ATTEMPTS,
                    },
                ),
            ],
        )
