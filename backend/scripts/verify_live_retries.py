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
        description="Verify retry visibility and multi-attempt task detail through the live Feedpulse API."
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
        help="Maximum time to wait for each job to reach terminal state",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=2.0,
        help="Polling interval while waiting for job completion",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=3,
        help="Maximum number of live jobs to launch while looking for a retried task",
    )
    parser.add_argument(
        "--recent-jobs-to-scan",
        type=int,
        default=20,
        help="Number of recent persisted jobs to scan before launching new jobs",
    )
    args = parser.parse_args()

    attempted_jobs: list[dict[str, object]] = []
    retried_task_summary = None
    retried_task_detail = None
    job_detail = None
    verification_source = "recent_history"

    recent_jobs = request_json("GET", f"{args.base_url}/jobs")
    for recent_job in recent_jobs[: args.recent_jobs_to_scan]:
        job_detail = request_json("GET", f"{args.base_url}/jobs/{recent_job['id']}")
        retried_task_summary, retried_task_detail = load_retried_task(
            base_url=args.base_url,
            job_id=recent_job["id"],
        )
        if retried_task_summary is None or retried_task_detail is None:
            continue
        validate_retried_task(retried_task_summary, retried_task_detail)
        break

    if retried_task_summary is None or retried_task_detail is None or job_detail is None:
        verification_source = "fresh_jobs"
        for _ in range(args.max_jobs):
            start_payload = {"idempotency_key": f"verify-live-retries-{uuid.uuid4()}"}
            start_response = request_json(
                "POST",
                f"{args.base_url}/jobs",
                payload=start_payload,
            )
            job_id = start_response["job_id"]
            job_detail = wait_for_terminal_job(
                base_url=args.base_url,
                job_id=job_id,
                timeout_seconds=args.timeout_seconds,
                poll_interval_seconds=args.poll_interval_seconds,
            )
            retried_task_summary, retried_task_detail = load_retried_task(
                base_url=args.base_url,
                job_id=job_id,
            )
            attempted_jobs.append(
                {
                    "job_id": job_id,
                    "job_status": job_detail["status"],
                    "max_attempt_count": retried_task_summary["attempt_count"]
                    if retried_task_summary is not None
                    else 1,
                }
            )
            if retried_task_summary is None or retried_task_detail is None:
                continue
            validate_retried_task(retried_task_summary, retried_task_detail)
            break

    if retried_task_summary is None or retried_task_detail is None or job_detail is None:
        raise RuntimeError(
            "Live retry verification did not observe a multi-attempt task within the configured job budget: "
            f"{json.dumps(attempted_jobs, sort_keys=True)}"
        )

    attempts = retried_task_detail["attempts"]
    summary = {
        "job_id": job_detail["id"],
        "job_status": job_detail["status"],
        "temporal_run_id": job_detail.get("temporal_run_id"),
        "retried_task_id": retried_task_summary["id"],
        "retried_task_url": retried_task_summary["url"],
        "retried_task_status": retried_task_summary["status"],
        "attempt_count": retried_task_summary["attempt_count"],
        "first_attempt_status": attempts[0]["status"],
        "final_attempt_status": attempts[-1]["status"],
        "failed_attempts": sum(1 for attempt in attempts if attempt["status"] == "failed"),
        "jobs_started": len(attempted_jobs),
        "verification_source": verification_source,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


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
        f"Job {job_id} did not reach terminal state within {timeout_seconds} seconds"
    )


def validate_retried_task(task_summary: dict, task_detail: dict) -> None:
    attempts = task_detail.get("attempts") or []
    if len(attempts) != task_summary["attempt_count"]:
        raise RuntimeError(
            f"Task {task_summary['id']} attempt_count mismatch: summary={task_summary['attempt_count']} detail={len(attempts)}"
        )

    expected_attempt_numbers = list(range(1, len(attempts) + 1))
    actual_attempt_numbers = [attempt["attempt_number"] for attempt in attempts]
    if actual_attempt_numbers != expected_attempt_numbers:
        raise RuntimeError(
            f"Task {task_summary['id']} attempt numbers were not contiguous: {actual_attempt_numbers}"
        )

    if not any(attempt["status"] == "failed" for attempt in attempts[:-1]):
        raise RuntimeError(
            f"Task {task_summary['id']} had multiple attempts but no failed retry precursor"
        )

    final_attempt = attempts[-1]
    expected_final_status = "succeeded" if task_summary["status"] == "completed" else "failed"
    if final_attempt["status"] != expected_final_status:
        raise RuntimeError(
            f"Task {task_summary['id']} final attempt status mismatch: "
            f"task_status={task_summary['status']} final_attempt_status={final_attempt['status']}"
        )

    if task_detail["id"] != task_summary["id"]:
        raise RuntimeError(
            f"Task detail id {task_detail['id']} did not match summary id {task_summary['id']}"
        )


def load_retried_task(*, base_url: str, job_id: str) -> tuple[dict | None, dict | None]:
    tasks = request_json("GET", f"{base_url}/jobs/{job_id}/tasks?sort=attempts")
    attempt_counts = [task["attempt_count"] for task in tasks]
    if not is_sorted_descending(attempt_counts):
        raise RuntimeError(
            f"Job {job_id} task list was not sorted by descending attempts: {attempt_counts[:10]}"
        )

    retried_task_summary = next(
        (task for task in tasks if task["attempt_count"] > 1),
        None,
    )
    if retried_task_summary is None:
        return None, None

    retried_task_detail = request_json(
        "GET",
        f"{base_url}/jobs/{job_id}/tasks/{retried_task_summary['id']}",
    )
    return retried_task_summary, retried_task_detail


def is_sorted_descending(values: list[int]) -> bool:
    return all(left >= right for left, right in zip(values, values[1:]))


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
