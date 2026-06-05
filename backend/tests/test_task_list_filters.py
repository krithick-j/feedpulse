from __future__ import annotations

import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.api.routes import jobs as job_routes
from app.schemas.jobs import TaskSummary


JOB_ID = "11111111-1111-4111-8111-111111111111"


def make_task(*, task_id: int, attempt_count: int, status: str = "completed") -> TaskSummary:
    return TaskSummary(
        id=task_id,
        url=f"https://example.com/{task_id}.xml",
        status=status,
        queue="xml-small-queue",
        attempt_count=attempt_count,
        records_extracted=0,
        duration_ms=100,
        last_error=None,
        last_error_type=None,
        started_at="2026-06-05T00:00:00+00:00",
        finished_at="2026-06-05T00:00:01+00:00",
    )


class TaskListFilterTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_tasks_filters_retried_only_in_mock_mode(self) -> None:
        tasks = [
            make_task(task_id=1, attempt_count=1),
            make_task(task_id=2, attempt_count=3),
            make_task(task_id=3, attempt_count=2, status="failed"),
        ]

        with (
            patch.object(job_routes, "settings", SimpleNamespace(data_backend="mock")),
            patch.object(job_routes.store, "get_tasks", return_value=tasks),
        ):
            result = await job_routes.get_tasks(
                JOB_ID,
                status_filter=None,
                retried_only=True,
                sort_by="url",
            )

        self.assertEqual([task.id for task in result], [2, 3])

    async def test_get_tasks_passes_retried_filter_to_repository(self) -> None:
        repository = SimpleNamespace(list_task_summaries=AsyncMock(return_value=[]))

        async def run_with_repository(operation):
            return await operation(repository)

        with (
            patch.object(job_routes, "settings", SimpleNamespace(data_backend="database")),
            patch.object(job_routes, "_with_repository", new=AsyncMock(side_effect=run_with_repository)),
        ):
            await job_routes.get_tasks(
                JOB_ID,
                status_filter=None,
                retried_only=True,
                sort_by="attempts",
            )

        repository.list_task_summaries.assert_awaited_once_with(
            uuid.UUID(JOB_ID),
            status_filter=None,
            retried_only=True,
            sort_by="attempts",
        )
