from __future__ import annotations

import unittest

from app.api.routes.jobs import _sort_mock_tasks
from app.repositories.jobs import JobRepository
from app.schemas.jobs import TaskSummary


def make_task(*, task_id: int, url: str, attempt_count: int) -> TaskSummary:
    return TaskSummary(
        id=task_id,
        url=url,
        status="completed",
        queue="xml-small-queue",
        attempt_count=attempt_count,
        records_extracted=0,
        duration_ms=100,
        last_error=None,
        last_error_type=None,
        started_at="2026-06-05T00:00:00+00:00",
        finished_at="2026-06-05T00:00:01+00:00",
    )


class TaskSortingTests(unittest.TestCase):
    def test_mock_sort_by_attempts_orders_descending(self) -> None:
        tasks = [
            make_task(task_id=1, url="https://b.example/feed.xml", attempt_count=1),
            make_task(task_id=2, url="https://a.example/feed.xml", attempt_count=3),
            make_task(task_id=3, url="https://c.example/feed.xml", attempt_count=2),
        ]

        ordered = _sort_mock_tasks(tasks, "attempts")

        self.assertEqual([task.id for task in ordered], [2, 3, 1])

    def test_repository_sort_clause_supports_attempts(self) -> None:
        clause = JobRepository._task_sort_clause("attempts")

        self.assertEqual(len(clause), 2)
        self.assertIn("attempt_count", str(clause[0]))
        self.assertIn("DESC", str(clause[0]))
        self.assertIn("url", str(clause[1]))

