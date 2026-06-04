from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from temporalio.api.enums.v1 import WorkflowExecutionStatus

from app.schemas.jobs import JobCounts, JobSummary
from app.services.job_reconciler import reconcile_running_jobs


@dataclass
class FakeSettings:
    data_backend: str = "database"
    job_execution_backend: str = "temporal"
    job_reconciliation_grace_seconds: int = 60
    job_reconciliation_pending_history_limit: int = 25


class FakeSessionContext:
    def __init__(self, session: object) -> None:
        self._session = session

    async def __aenter__(self) -> object:
        return self._session

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class RunningJobRepository:
    def __init__(self, *_args, **_kwargs) -> None:
        self.failed_jobs: list[tuple[str, str, str]] = []
        self.finalized_jobs: list[str] = []
        self.job_id = "11111111-1111-4111-8111-111111111111"

    async def list_running_jobs(self) -> list[JobSummary]:
        old_started_at = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        return [
            JobSummary(
                id=self.job_id,
                status="running",
                total_urls=101,
                counts=JobCounts(pending=101, in_progress=0, completed=0, failed=0),
                created_at=old_started_at,
                started_at=old_started_at,
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
    def __init__(self) -> None:
        self.terminated_reasons: list[str | None] = []

    async def describe(self):
        return SimpleNamespace(
            status=WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING,
            history_length=19,
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
