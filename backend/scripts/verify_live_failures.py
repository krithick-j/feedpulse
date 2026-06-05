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
        description="Smoke verify failed-task inspection through the live Feedpulse API."
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

    start_payload = {"idempotency_key": f"verify-live-failures-{uuid.uuid4()}"}
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
    if job_detail["status"] not in {"completed_with_failures", "failed"}:
        raise RuntimeError(
            f"Job {job_id} did not expose a failure path for verification: status={job_detail['status']}"
        )

    counts = job_detail["counts"]
    if counts["failed"] <= 0:
        raise RuntimeError(f"Job {job_id} reported no failed tasks")

    failed_tasks = request_json(
        "GET",
        f"{args.base_url}/jobs/{job_id}/tasks?status=failed&sort=duration",
    )
    if not failed_tasks:
        raise RuntimeError(f"Job {job_id} returned no failed tasks from failed-task listing")

    failed_task = failed_tasks[0]
    task_detail = request_json(
        "GET",
        f"{args.base_url}/jobs/{job_id}/tasks/{failed_task['id']}",
    )
    attempts = task_detail.get("attempts") or []
    if not attempts:
        raise RuntimeError(f"Failed task {failed_task['id']} returned no attempts")

    final_attempt = attempts[-1]
    if final_attempt["status"] != "failed":
        raise RuntimeError(
            f"Failed task {failed_task['id']} final attempt was not failed: {final_attempt['status']}"
        )
    if not final_attempt.get("error_type"):
        raise RuntimeError(f"Failed task {failed_task['id']} final attempt missing error_type")
    if not final_attempt.get("error_message"):
        raise RuntimeError(f"Failed task {failed_task['id']} final attempt missing error_message")
    if not task_detail.get("last_error_type"):
        raise RuntimeError(f"Failed task {failed_task['id']} missing last_error_type")
    if not task_detail.get("last_error"):
        raise RuntimeError(f"Failed task {failed_task['id']} missing last_error")

    summary = {
        "job_id": job_id,
        "job_status": job_detail["status"],
        "failed_task_id": failed_task["id"],
        "failed_task_url": failed_task["url"],
        "attempt_count": len(attempts),
        "final_attempt_error_type": final_attempt["error_type"],
        "final_attempt_http_status": final_attempt.get("http_status"),
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
