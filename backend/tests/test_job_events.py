from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from app.api.handlers import jobs as job_handlers
from app.services.jobs import JobService
from app.db.notifications import JOB_EVENTS_CHANNEL, JobEventListener, JobNotification, _parse_job_notification, job_events_channel_for_job


def make_event_service(*, projections, listener_events):
    return JobService(
        settings=SimpleNamespace(data_backend="database"),
        run_repository=AsyncMock(side_effect=projections),
        event_listener_factory=lambda job_id: FakeListener(listener_events),
    )
from app.dto.jobs import JobCounts, JobProjection, JobSummary, TaskSummary


JOB_ID = "11111111-1111-4111-8111-111111111111"


def build_projection(
    *,
    status: str,
    counts: JobCounts,
    task_status: str,
    attempt_count: int,
    records_extracted: int,
    elapsed_ms: int,
) -> JobProjection:
    return JobProjection(
        job=JobSummary(
            id=JOB_ID,
            status=status,
            total_urls=1,
            counts=counts,
            created_at="2026-06-05T10:00:00+00:00",
            started_at="2026-06-05T10:00:05+00:00",
            finished_at="2026-06-05T10:00:15+00:00" if status != "running" else None,
            elapsed_ms=elapsed_ms,
            temporal_run_id="run-1",
        ),
        live=status == "running",
        task_summaries=[
            TaskSummary(
                id=1,
                url="https://example.com/feed.xml",
                status=task_status,
                queue="xml-small-queue",
                attempt_count=attempt_count,
                records_extracted=records_extracted,
                duration_ms=120 if task_status != "pending" else None,
                last_error="Timeout while fetching feed" if task_status == "failed" else None,
                last_error_type="FeedFetchError" if task_status == "failed" else None,
                started_at="2026-06-05T10:00:05+00:00" if task_status != "pending" else None,
                finished_at="2026-06-05T10:00:15+00:00" if task_status in {"completed", "failed"} else None,
            )
        ],
    )


class FakeListener:
    def __init__(self, events: list[object | None]) -> None:
        self._events = list(events)

    async def __aenter__(self) -> "FakeListener":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def next_event(self, *, timeout=None):
        if self._events:
            return self._events.pop(0)
        return None


async def collect_sse(response) -> list[dict]:
    events: list[dict] = []
    async for chunk in response.body_iterator:
        text = chunk.decode() if isinstance(chunk, bytes) else chunk
        if not text.startswith("data: "):
            continue
        events.append(json.loads(text.removeprefix("data: ").strip()))
    return events


class JobNotificationTests(unittest.TestCase):
    def test_job_events_channel_for_job_uses_uuid_hex_suffix(self) -> None:
        self.assertEqual(
            job_events_channel_for_job(JOB_ID),
            f"{JOB_EVENTS_CHANNEL}_11111111111141118111111111111111",
        )

    def test_listener_uses_job_scoped_channel_when_job_id_is_provided(self) -> None:
        listener = JobEventListener(job_id=JOB_ID)

        self.assertEqual(
            listener.channel,
            f"{JOB_EVENTS_CHANNEL}_11111111111141118111111111111111",
        )

    def test_parse_job_notification_accepts_valid_payload(self) -> None:
        payload = json.dumps(
            {
                "job_id": JOB_ID,
                "scope": "task.updated",
                "task_id": 4,
            }
        )

        parsed = _parse_job_notification(payload)

        self.assertEqual(
            parsed,
            JobNotification(job_id=JOB_ID, scope="task.updated", task_id=4),
        )

    def test_parse_job_notification_rejects_invalid_payloads(self) -> None:
        self.assertIsNone(_parse_job_notification("not-json"))
        self.assertIsNone(_parse_job_notification(json.dumps({"job_id": JOB_ID})))
        self.assertIsNone(
            _parse_job_notification(
                json.dumps(
                    {
                        "job_id": JOB_ID,
                        "scope": "task.updated",
                        "task_id": "1",
                    }
                )
            )
        )


class JobEventStreamTests(unittest.IsolatedAsyncioTestCase):
    async def test_stream_job_events_emits_snapshot_and_completed_for_terminal_job(self) -> None:
        terminal_projection = build_projection(
            status="completed",
            counts=JobCounts(pending=0, in_progress=0, completed=1, failed=0),
            task_status="completed",
            attempt_count=1,
            records_extracted=3,
            elapsed_ms=1500,
        )

        service = make_event_service(
            projections=[terminal_projection, terminal_projection],
            listener_events=[],
        )
        with patch.object(job_handlers, "job_service", service):
            response = await job_handlers.stream_job_events(JOB_ID)
            events = await collect_sse(response)

        self.assertEqual(
            [event["type"] for event in events],
            ["job.snapshot", "job.completed"],
        )
        self.assertEqual(events[0]["payload"]["status"], "completed")
        self.assertEqual(events[1]["payload"]["counts"]["completed"], 1)

    async def test_stream_job_events_emits_task_delta_then_progress_then_completed(self) -> None:
        initial_projection = build_projection(
            status="running",
            counts=JobCounts(pending=1, in_progress=0, completed=0, failed=0),
            task_status="pending",
            attempt_count=0,
            records_extracted=0,
            elapsed_ms=100,
        )
        terminal_projection = build_projection(
            status="completed_with_failures",
            counts=JobCounts(pending=0, in_progress=0, completed=0, failed=1),
            task_status="failed",
            attempt_count=3,
            records_extracted=0,
            elapsed_ms=1800,
        )

        service = make_event_service(
            projections=[initial_projection, initial_projection, terminal_projection],
            listener_events=[JobNotification(job_id=JOB_ID, scope="task.updated", task_id=1)],
        )
        with patch.object(job_handlers, "job_service", service):
            response = await job_handlers.stream_job_events(JOB_ID)
            events = await collect_sse(response)

        self.assertEqual(
            [event["type"] for event in events],
            ["job.snapshot", "task.updated", "job.progress", "job.completed"],
        )
        self.assertEqual(events[1]["payload"]["task"]["status"], "failed")
        self.assertEqual(events[1]["payload"]["task"]["attempt_count"], 3)
        self.assertEqual(events[2]["payload"]["status"], "completed_with_failures")
        self.assertEqual(events[3]["payload"]["counts"]["failed"], 1)

    async def test_stream_job_events_reconciles_terminal_state_after_timeout_without_notification(self) -> None:
        initial_projection = build_projection(
            status="running",
            counts=JobCounts(pending=1, in_progress=0, completed=0, failed=0),
            task_status="pending",
            attempt_count=0,
            records_extracted=0,
            elapsed_ms=100,
        )
        terminal_projection = build_projection(
            status="completed_with_failures",
            counts=JobCounts(pending=0, in_progress=0, completed=0, failed=1),
            task_status="failed",
            attempt_count=1,
            records_extracted=0,
            elapsed_ms=1800,
        )

        service = make_event_service(
            projections=[initial_projection, initial_projection, terminal_projection],
            listener_events=[None],
        )
        with patch.object(job_handlers, "job_service", service):
            response = await job_handlers.stream_job_events(JOB_ID)
            events = await collect_sse(response)

        self.assertEqual(
            [event["type"] for event in events],
            ["job.snapshot", "task.updated", "job.progress", "job.completed"],
        )
        self.assertEqual(events[-1]["payload"]["status"], "completed_with_failures")
