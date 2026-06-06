from __future__ import annotations

import asyncio
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from temporalio.api.enums.v1 import WorkflowExecutionStatus

from app.dto.jobs import JobCounts, JobSummary
from app.services.job_reconciler import reconcile_running_jobs, reconciliation_enabled, run_reconciliation_loop


@dataclass
class FakeSettings:
    data_backend: str = "database"
    job_execution_backend: str = "temporal"
    job_reconciliation_grace_seconds: int = 60
    job_reconciliation_pending_history_limit: int = 25
    job_reconciliation_interval_seconds: int = 60


class FakeSessionContext:
    def __init__(self, session: object) -> None:
        self._session = session

    async def __aenter__(self) -> object:
        return self._session

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class RunningJobRepository:
    def __init__(
        self,
        *_args,
        counts: JobCounts | None = None,
        started_at: str | None = None,
        **_kwargs,
    ) -> None:
        self.failed_jobs: list[tuple[str, str, str]] = []
        self.finalized_jobs: list[str] = []
        self.job_id = "11111111-1111-4111-8111-111111111111"
        self.counts = counts or JobCounts(pending=101, in_progress=0, completed=0, failed=0)
        self.started_at = started_at or (
            datetime.now(timezone.utc) - timedelta(minutes=5)
        ).isoformat()

    async def list_running_jobs(self) -> list[JobSummary]:
        return [
            JobSummary(
                id=self.job_id,
                status="running",
                total_urls=101,
                counts=self.counts,
                created_at=self.started_at,
                started_at=self.started_at,
                finished_at=None,
                elapsed_ms=300000,
                temporal_run_id="run-1",
            )
        ]

    async def fail_incomplete_tasks(self, job_id, *, error_type: str, error_message: str) -> bool:
        self.failed_jobs.append((str(job_id), error_type, error_message))
        return True

    async def finalize_job(self, job_id) -> bool:
        self.finalized_jobs.append(str(job_id))
        return True


class FakeHandle:
    def __init__(
        self,
        *,
        status: WorkflowExecutionStatus = WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING,
        history_length: int = 19,
        describe_error: Exception | None = None,
    ) -> None:
        self.terminated_reasons: list[str | None] = []
        self.status = status
        self.history_length = history_length
        self.describe_error = describe_error

    async def describe(self):
        if self.describe_error is not None:
            raise self.describe_error
        return SimpleNamespace(
            status=self.status,
            history_length=self.history_length,
        )

    async def terminate(self, *args, reason: str | None = None, **_kwargs) -> None:
        self.terminated_reasons.append(reason)


class FakeClient:
    def __init__(self, handle: FakeHandle) -> None:
        self.handle = handle
        self.job_id = "11111111-1111-4111-8111-111111111111"

    def get_workflow_handle(self, workflow_id: str, *, run_id: str | None = None) -> FakeHandle:
        assert workflow_id == self.job_id
        assert run_id == "run-1"
        return self.handle


class JobReconcilerTests(unittest.IsolatedAsyncioTestCase):
    async def test_reconcile_running_jobs_terminates_stale_zero_progress_workflow(self) -> None:
        job_id = "11111111-1111-4111-8111-111111111111"
        session = object()
        repository = RunningJobRepository()
        handle = FakeHandle()
        client = FakeClient(handle)

        with (
            patch("app.services.job_reconciler.get_settings", return_value=FakeSettings()),
            patch("app.services.job_reconciler.SessionLocal", return_value=FakeSessionContext(session)),
            patch("app.services.job_reconciler.JobRepository", return_value=repository),
            patch("app.services.job_reconciler.get_temporal_client", return_value=client),
        ):
            reconciled = await reconcile_running_jobs()

        self.assertEqual(reconciled, 1)
        self.assertEqual(
            handle.terminated_reasons,
            ["Feedpulse reconciliation terminated a stuck running workflow"],
        )
        self.assertEqual(
            repository.failed_jobs,
            [
                (
                    job_id,
                    "WorkflowStuckRunning",
                    "Temporal workflow was still running but had made no task progress within the reconciliation grace window",
                )
            ],
        )
        self.assertEqual(repository.finalized_jobs, [job_id])

    async def test_reconcile_running_jobs_repairs_missing_workflow(self) -> None:
        job_id = "11111111-1111-4111-8111-111111111111"
        repository = RunningJobRepository()
        handle = FakeHandle(describe_error=RuntimeError("missing"))
        client = FakeClient(handle)

        with (
            patch("app.services.job_reconciler.get_settings", return_value=FakeSettings()),
            patch("app.services.job_reconciler.SessionLocal", return_value=FakeSessionContext(object())),
            patch("app.services.job_reconciler.JobRepository", return_value=repository),
            patch("app.services.job_reconciler.get_temporal_client", return_value=client),
        ):
            reconciled = await reconcile_running_jobs()

        self.assertEqual(reconciled, 1)
        self.assertEqual(
            repository.failed_jobs,
            [
                (
                    job_id,
                    "WorkflowMissingError",
                    "Temporal workflow was not found during reconciliation",
                )
            ],
        )
        self.assertEqual(repository.finalized_jobs, [job_id])

    async def test_reconcile_running_jobs_repairs_closed_workflow_with_status_specific_error(self) -> None:
        job_id = "11111111-1111-4111-8111-111111111111"
        repository = RunningJobRepository()
        handle = FakeHandle(
            status=WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TERMINATED
        )
        client = FakeClient(handle)

        with (
            patch("app.services.job_reconciler.get_settings", return_value=FakeSettings()),
            patch("app.services.job_reconciler.SessionLocal", return_value=FakeSessionContext(object())),
            patch("app.services.job_reconciler.JobRepository", return_value=repository),
            patch("app.services.job_reconciler.get_temporal_client", return_value=client),
        ):
            reconciled = await reconcile_running_jobs()

        self.assertEqual(reconciled, 1)
        self.assertEqual(
            repository.failed_jobs,
            [
                (
                    job_id,
                    "WorkflowTerminatedWithoutFinalization",
                    "Temporal workflow closed with status WORKFLOW_EXECUTION_STATUS_TERMINATED before DB finalization completed",
                )
            ],
        )
        self.assertEqual(repository.finalized_jobs, [job_id])

    async def test_reconcile_running_jobs_leaves_progressing_running_workflow_untouched(self) -> None:
        repository = RunningJobRepository(
            counts=JobCounts(pending=80, in_progress=10, completed=11, failed=0)
        )
        handle = FakeHandle(
            status=WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING,
            history_length=80,
        )
        client = FakeClient(handle)

        with (
            patch("app.services.job_reconciler.get_settings", return_value=FakeSettings()),
            patch("app.services.job_reconciler.SessionLocal", return_value=FakeSessionContext(object())),
            patch("app.services.job_reconciler.JobRepository", return_value=repository),
            patch("app.services.job_reconciler.get_temporal_client", return_value=client),
        ):
            reconciled = await reconcile_running_jobs()

        self.assertEqual(reconciled, 0)
        self.assertEqual(repository.failed_jobs, [])
        self.assertEqual(repository.finalized_jobs, [])

    async def test_run_reconciliation_loop_runs_until_stop(self) -> None:
        stop_event = asyncio.Event()
        calls: list[str] = []

        async def fake_reconcile() -> int:
            calls.append("reconcile")
            stop_event.set()
            return 1

        with (
            patch("app.services.job_reconciler.get_settings", return_value=FakeSettings()),
            patch("app.services.job_reconciler.reconcile_running_jobs", side_effect=fake_reconcile),
        ):
            await run_reconciliation_loop(stop_event=stop_event, interval_seconds=0.001)

        self.assertEqual(calls, ["reconcile"])

    async def test_run_reconciliation_loop_returns_when_reconciliation_disabled(self) -> None:
        stop_event = unittest.mock.Mock()
        stop_event.wait = AsyncMock()

        with (
            patch(
                "app.services.job_reconciler.get_settings",
                return_value=FakeSettings(data_backend="mock"),
            ),
            patch("app.services.job_reconciler.reconcile_running_jobs", new=AsyncMock()) as reconcile_mock,
        ):
            await run_reconciliation_loop(stop_event=stop_event, interval_seconds=0.001)

        reconcile_mock.assert_not_awaited()
        stop_event.wait.assert_not_awaited()

    def test_reconciliation_enabled_requires_database_temporal_runtime(self) -> None:
        self.assertTrue(reconciliation_enabled(FakeSettings()))
        self.assertFalse(reconciliation_enabled(FakeSettings(data_backend="mock")))
        self.assertFalse(
            reconciliation_enabled(FakeSettings(job_execution_backend="simulator"))
        )
