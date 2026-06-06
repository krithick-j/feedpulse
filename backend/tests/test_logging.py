from __future__ import annotations

import json
import logging
import unittest
import uuid
from datetime import datetime, timezone

from app.core.logging import JsonLogFormatter


class JsonLoggingTests(unittest.TestCase):
    def test_formatter_serializes_structured_fields(self) -> None:
        logger = logging.getLogger("feedpulse.tests.logging")
        formatter = JsonLogFormatter()

        record = logger.makeRecord(
            logger.name,
            logging.INFO,
            fn=__file__,
            lno=24,
            msg="task.completed",
            args=(),
            exc_info=None,
            extra={
                "event": "task.completed",
                "job_id": uuid.UUID("11111111-1111-4111-8111-111111111111"),
                "task_id": 42,
                "counts": {"completed": 3},
                "started_at": datetime(2026, 6, 6, 10, 30, tzinfo=timezone.utc),
            },
        )

        payload = json.loads(formatter.format(record))

        self.assertEqual(payload["event"], "task.completed")
        self.assertEqual(payload["message"], "task.completed")
        self.assertEqual(payload["job_id"], "11111111-1111-4111-8111-111111111111")
        self.assertEqual(payload["task_id"], 42)
        self.assertEqual(payload["counts"]["completed"], 3)
        self.assertEqual(payload["started_at"], "2026-06-06T10:30:00+00:00")
