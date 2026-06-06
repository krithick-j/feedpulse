from __future__ import annotations

import unittest

from app.repositories.jobs import JobRepository


class TaskSortingTests(unittest.TestCase):
    def test_repository_sort_clause_supports_attempts(self) -> None:
        clause = JobRepository._task_sort_clause("attempts")

        self.assertEqual(len(clause), 2)
        self.assertIn("attempt_count", str(clause[0]))
        self.assertIn("DESC", str(clause[0]))
        self.assertIn("url", str(clause[1]))
