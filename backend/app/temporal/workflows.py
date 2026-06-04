from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from app.temporal.types import ProcessXmlJobInput, ProcessedTaskResult

with workflow.unsafe.imports_passed_through():
    from app.temporal.activities import (
        fail_incomplete_tasks_activity,
        finalize_job_activity,
        process_single_url_activity,
        set_job_running_activity,
    )


@workflow.defn
class ProcessXmlJobWorkflow:
    @workflow.run
    async def run(self, payload: ProcessXmlJobInput) -> list[ProcessedTaskResult]:
        try:
            await workflow.execute_activity(
                set_job_running_activity,
                args=(payload.job_id,),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )

            tasks = [
                workflow.execute_activity(
                    process_single_url_activity,
                    args=(payload.job_id, task),
                    task_queue=task.queue,
                    start_to_close_timeout=timedelta(seconds=30),
                    schedule_to_close_timeout=timedelta(minutes=2),
                    heartbeat_timeout=timedelta(seconds=10),
                    retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=1),
                        backoff_coefficient=2.0,
                        maximum_interval=timedelta(seconds=10),
                        maximum_attempts=3,
                        non_retryable_error_types=["HttpClientError"],
                    ),
                )
                for task in payload.tasks
            ]

            return await asyncio.gather(*tasks)
        except Exception as exc:
            await workflow.execute_activity(
                fail_incomplete_tasks_activity,
                args=(
                    payload.job_id,
                    type(exc).__name__,
                    str(exc) or "Temporal workflow execution failed",
                ),
                start_to_close_timeout=timedelta(seconds=15),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            raise
        finally:
            await workflow.execute_activity(
                finalize_job_activity,
                args=(payload.job_id,),
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
