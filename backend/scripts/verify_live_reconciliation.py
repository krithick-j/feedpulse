#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import urllib.error
import urllib.request
import uuid

from app.services.job_reconciler import reconcile_running_jobs
from app.temporal.client import get_temporal_client


TERMINAL_STATUSES = {"completed", "completed_with_failures", "failed"}
EXPECTED_ERROR_TYPE = "WorkflowTerminatedWithoutFinalization"


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify live Temporal reconciliation by terminating a running workflow and repairing the job state."
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8000/api/v1",
        help="API base URL",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=60,
        help="Maximum time to wait for each phase of verification",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=0.25,
        help="Polling interval while waiting for a running job or repaired terminal state",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=3,
        help="Maximum number of jobs to start while looking for a still-running workflow to terminate",
    )
    args = parser.parse_args()

    client = await get_temporal_client()
    last_attempts: list[dict[str, object]] = []

    for _ in range(args.max_jobs):
        start_payload = {"idempotency_key": f"verify-live-reconciliation-{uuid.uuid4()}"}
        start_response = request_json(
            "POST",
            f"{args.base_url}/jobs",
            payload=start_payload,
        )
        job_id = start_response["job_id"]
        running_job = wait_for_running_job(
            base_url=args.base_url,
            job_id=job_id,
            timeout_seconds=args.timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
        )
        if running_job is None:
            last_attempts.append(
                {
                    "job_id": job_id,
                    "reason": "job_reached_terminal_state_before_reconciliation_probe",
                }
            )
            continue

        temporal_run_id = running_job.get("temporal_run_id")
        if not temporal_run_id:
            last_attempts.append(
                {
                    "job_id": job_id,
                    "reason": "job_never_persisted_temporal_run_id_before_probe",
                }
            )
            continue

        handle = client.get_workflow_handle(job_id, run_id=temporal_run_id)
        await handle.terminate(
            reason="Feedpulse live reconciliation verifier terminated workflow"
        )
        reconciled = await reconcile_running_jobs()
        repaired_job = wait_for_terminal_job(
            base_url=args.base_url,
            job_id=job_id,
            timeout_seconds=args.timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
        )

        if repaired_job["status"] == "running":
            raise RuntimeError(f"Job {job_id} remained running after reconciliation")

        counts = repaired_job["counts"]
        if counts["pending"] != 0 or counts["in_progress"] != 0:
            raise RuntimeError(
                f"Job {job_id} still had active work after reconciliation: {counts}"
            )
        if counts["completed"] + counts["failed"] != repaired_job["total_urls"]:
            raise RuntimeError(
                f"Job {job_id} count mismatch after reconciliation: {counts}"
            )
        if counts["failed"] <= 0:
            raise RuntimeError(f"Job {job_id} had no failed tasks after reconciliation")

        failed_tasks = request_json(
            "GET",
            f"{args.base_url}/jobs/{job_id}/tasks?status=failed&sort=url",
        )
        repaired_task = next(
            (task for task in failed_tasks if task.get("last_error_type") == EXPECTED_ERROR_TYPE),
            None,
        )
        if repaired_task is None:
            raise RuntimeError(
                f"Job {job_id} failed tasks did not include reconciliation error type {EXPECTED_ERROR_TYPE}"
            )

        task_detail = request_json(
            "GET",
            f"{args.base_url}/jobs/{job_id}/tasks/{repaired_task['id']}",
        )
        if task_detail.get("last_error_type") != EXPECTED_ERROR_TYPE:
            raise RuntimeError(
                f"Task {repaired_task['id']} detail did not retain reconciliation error type"
            )

        summary = {
            "job_id": job_id,
            "job_status": repaired_job["status"],
            "temporal_run_id": temporal_run_id,
            "reconciled_count": reconciled,
            "failed_count": counts["failed"],
            "completed_count": counts["completed"],
            "repaired_task_id": repaired_task["id"],
            "repaired_task_error_type": task_detail["last_error_type"],
        }
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0

    raise RuntimeError(
        "Live reconciliation verification did not catch a still-running workflow within the configured job budget: "
        f"{json.dumps(last_attempts, sort_keys=True)}"
    )


def wait_for_running_job(
    *,
    base_url: str,
    job_id: str,
    timeout_seconds: int,
    poll_interval_seconds: float,
) -> dict | None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        job_detail = request_json("GET", f"{base_url}/jobs/{job_id}")
        if job_detail.get("temporal_run_id") and job_detail["status"] == "running":
            return job_detail
        if job_detail["status"] in TERMINAL_STATUSES:
            return None
        time.sleep(poll_interval_seconds)
    return None


def wait_for_terminal_job(
    *,
    base_url: str,
    job_id: str,
    timeout_seconds: int,
    poll_interval_seconds: float,
) -> dict:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        job_detail = request_json("GET", f"{base_url}/jobs/{job_id}")
        if job_detail["status"] in TERMINAL_STATUSES:
            return job_detail
        time.sleep(poll_interval_seconds)

    raise RuntimeError(
        f"Job {job_id} did not reach terminal state within {timeout_seconds} seconds after reconciliation"
    )


def request_json(method: str, url: str, payload: dict | None = None):
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} failed with HTTP {exc.code}: {body}") from exc


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
