from __future__ import annotations

import unittest

from app.temporal.types import ProcessXmlJobInput, ProcessedTaskResult, WorkflowTaskInput
from app.temporal.workflows import (
    ProcessXmlJobWorkflow,
    fail_incomplete_tasks_activity,
    finalize_job_activity,
    process_single_url_activity,
    set_job_running_activity,
)


JOB_ID = "11111111-1111-4111-8111-111111111111"


class TemporalWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def test_workflow_runs_all_tasks_and_finalizes_job(self) -> None:
        payload = ProcessXmlJobInput(
            job_id=JOB_ID,
            tasks=[
                WorkflowTaskInput(
                    task_id=1,
                    url="https://example.com/feed-one.xml",
                    queue="xml-small-queue",
                ),
                WorkflowTaskInput(
                    task_id=2,
                    url="https://example.com/feed-two.xml",
                    queue="xml-large-queue",
                ),
            ],
        )
        recorded_calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

        async def fake_execute_activity(func, *args, **kwargs):
            activity_args = tuple(kwargs.get("args", args))
            recorded_calls.append((func.__name__, activity_args, kwargs))

            if func is set_job_running_activity:
                return None
            if func is process_single_url_activity:
                _job_id, task = activity_args
                return ProcessedTaskResult(
                    task_id=task.task_id,
                    status="completed",
                    queue=task.queue,
                    records_extracted=task.task_id,
                )
            if func is finalize_job_activity:
                return None

            raise AssertionError(f"Unexpected activity {func.__name__}")

        workflow = ProcessXmlJobWorkflow()

        with unittest.mock.patch(
            "app.temporal.workflows.workflow.execute_activity",
            side_effect=fake_execute_activity,
        ):
            results = await workflow.run(payload)

        self.assertEqual(
            results,
            [
                ProcessedTaskResult(
                    task_id=1,
                    status="completed",
                    queue="xml-small-queue",
                    records_extracted=1,
                ),
                ProcessedTaskResult(
                    task_id=2,
                    status="completed",
                    queue="xml-large-queue",
                    records_extracted=2,
                ),
            ],
        )
        self.assertEqual(
            [name for name, _args, _kwargs in recorded_calls],
            [
                "set_job_running_activity",
                "process_single_url_activity",
                "process_single_url_activity",
                "finalize_job_activity",
            ],
        )
        self.assertEqual(recorded_calls[1][1][1].queue, "xml-small-queue")
        self.assertEqual(recorded_calls[1][2]["task_queue"], "xml-small-queue")
        self.assertEqual(recorded_calls[2][1][1].queue, "xml-large-queue")
        self.assertEqual(recorded_calls[2][2]["task_queue"], "xml-large-queue")
        self.assertEqual(recorded_calls[3][1], (JOB_ID,))

    async def test_workflow_repairs_incomplete_tasks_when_activity_fails(self) -> None:
        payload = ProcessXmlJobInput(
            job_id=JOB_ID,
            tasks=[
                WorkflowTaskInput(
                    task_id=9,
                    url="https://example.com/broken.xml",
                    queue="xml-small-queue",
                )
            ],
        )
        recorded_calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

        async def fake_execute_activity(func, *args, **kwargs):
            activity_args = tuple(kwargs.get("args", args))
            recorded_calls.append((func.__name__, activity_args, kwargs))

            if func is set_job_running_activity:
                return None
            if func is process_single_url_activity:
                raise RuntimeError("boom")
            if func is fail_incomplete_tasks_activity:
                return None
            if func is finalize_job_activity:
                return None

            raise AssertionError(f"Unexpected activity {func.__name__}")

        workflow = ProcessXmlJobWorkflow()

        with unittest.mock.patch(
            "app.temporal.workflows.workflow.execute_activity",
            side_effect=fake_execute_activity,
        ):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                await workflow.run(payload)

        self.assertEqual(
            [name for name, _args, _kwargs in recorded_calls],
            [
                "set_job_running_activity",
                "process_single_url_activity",
                "fail_incomplete_tasks_activity",
                "finalize_job_activity",
            ],
        )
        self.assertEqual(
            recorded_calls[2][1],
            (JOB_ID, "RuntimeError", "boom"),
        )
        self.assertEqual(recorded_calls[3][1], (JOB_ID,))
