from __future__ import annotations

import argparse
import asyncio

from temporalio.worker import Worker

from app.core.settings import get_settings
from app.temporal.activities import (
    fail_incomplete_tasks_activity,
    finalize_job_activity,
    process_single_url_activity,
    set_job_running_activity,
)
from app.temporal.client import get_temporal_client
from app.temporal.workflows import ProcessXmlJobWorkflow


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a Feedpulse Temporal worker.")
    parser.add_argument(
        "--mode",
        choices=["workflow", "small", "large"],
        default="workflow",
        help="Worker mode. Workflow runs workflow-queue activities, small runs small-lane activities, large runs large-lane activities.",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    settings = get_settings()
    client = await get_temporal_client()

    if args.mode == "workflow":
        worker = Worker(
            client,
            task_queue=settings.temporal_workflow_task_queue,
            workflows=[ProcessXmlJobWorkflow],
            activities=[set_job_running_activity, fail_incomplete_tasks_activity, finalize_job_activity],
            max_concurrent_activities=4,
        )
        await worker.run()
    elif args.mode == "small":
        worker = Worker(
            client,
            task_queue=settings.temporal_small_activity_task_queue,
            workflows=[],
            activities=[process_single_url_activity],
            max_concurrent_activities=32,
        )
        await worker.run()
    else:
        worker = Worker(
            client,
            task_queue=settings.temporal_large_activity_task_queue,
            workflows=[],
            activities=[process_single_url_activity],
            max_concurrent_activities=12,
        )
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
