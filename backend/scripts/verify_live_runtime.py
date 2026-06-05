#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
import uuid


TERMINAL_STATUSES = {"completed", "completed_with_failures", "failed"}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Smoke verify the live Feedpulse runtime through the API."
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8000/api/v1",
        help="API base URL",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=240,
        help="Maximum time to wait for terminal job state",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=2.0,
        help="Polling interval while waiting for job completion",
    )
    args = parser.parse_args()

    idempotency_key = f"verify-live-runtime-{uuid.uuid4()}"
    start_payload = {"idempotency_key": idempotency_key}
    start_response = request_json(
        "POST",
        f"{args.base_url}/jobs",
        payload=start_payload,
    )
    job_id = start_response["job_id"]

    deadline = time.time() + args.timeout_seconds
    job_detail = None
    while time.time() < deadline:
        job_detail = request_json("GET", f"{args.base_url}/jobs/{job_id}")
        if job_detail["status"] in TERMINAL_STATUSES:
            break
        time.sleep(args.poll_interval_seconds)

    if job_detail is None:
        raise RuntimeError("Job detail could not be loaded")

    if job_detail["status"] not in {"completed", "completed_with_failures"}:
        raise RuntimeError(
            f"Job {job_id} did not finish successfully enough for smoke verification: "
            f"status={job_detail['status']}"
        )

    if not job_detail.get("temporal_run_id"):
        raise RuntimeError(f"Job {job_id} did not persist a Temporal run id")

    total_urls = job_detail["total_urls"]
    counts = job_detail["counts"]
    if counts["pending"] != 0 or counts["in_progress"] != 0:
        raise RuntimeError(f"Job {job_id} still has active work in terminal state: {counts}")
    if counts["completed"] + counts["failed"] != total_urls:
        raise RuntimeError(
            f"Job {job_id} count mismatch: completed+failed={counts['completed'] + counts['failed']} total_urls={total_urls}"
        )
    if counts["completed"] <= 0:
        raise RuntimeError(f"Job {job_id} completed zero tasks")

    tasks = request_json("GET", f"{args.base_url}/jobs/{job_id}/tasks?status=completed&sort=records")
    completed_task = next(
        (task for task in tasks if task["records_extracted"] > 0),
        None,
    )
    if completed_task is None:
        raise RuntimeError(f"Job {job_id} had no completed task with extracted records")

    records = request_json(
        "GET",
        f"{args.base_url}/jobs/{job_id}/tasks/{completed_task['id']}/records",
    )
    if not records:
        raise RuntimeError(
            f"Job {job_id} task {completed_task['id']} reported extracted records but returned none"
        )

    summary = {
        "job_id": job_id,
        "job_status": job_detail["status"],
        "temporal_run_id": job_detail["temporal_run_id"],
        "counts": counts,
        "completed_task_id": completed_task["id"],
        "completed_task_records": completed_task["records_extracted"],
        "sample_record_count": len(records),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


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
        raise SystemExit(main())
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
