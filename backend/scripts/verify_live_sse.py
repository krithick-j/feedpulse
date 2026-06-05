#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
import uuid


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Smoke verify the live Feedpulse SSE stream through the API."
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
        help="Maximum time to wait for terminal SSE event",
    )
    args = parser.parse_args()

    start_payload = {"idempotency_key": f"verify-live-sse-{uuid.uuid4()}"}
    start_response = request_json(
        "POST",
        f"{args.base_url}/jobs",
        payload=start_payload,
    )
    job_id = start_response["job_id"]

    events_url = f"{args.base_url}/jobs/{job_id}/events"
    deadline = time.time() + args.timeout_seconds
    event_types: list[str] = []
    task_updates = 0
    progress_events = 0
    last_payload: dict | None = None

    request = urllib.request.Request(
        events_url,
        headers={"Accept": "text/event-stream"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=args.timeout_seconds) as response:
            for raw_line in response:
                if time.time() > deadline:
                    raise RuntimeError(f"SSE stream for job {job_id} timed out")

                line = raw_line.decode("utf-8").strip()
                if not line or line.startswith(":"):
                    continue
                if not line.startswith("data: "):
                    continue

                payload = json.loads(line.removeprefix("data: ").strip())
                event_type = payload["type"]
                if payload["payload"]["job_id"] != job_id:
                    raise RuntimeError(
                        f"SSE stream returned mismatched job id {payload['payload']['job_id']} for {job_id}"
                    )

                event_types.append(event_type)
                last_payload = payload["payload"]

                if event_type == "task.updated":
                    task_updates += 1
                elif event_type == "job.progress":
                    progress_events += 1
                elif event_type == "job.completed":
                    break
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GET {events_url} failed with HTTP {exc.code}: {body}") from exc

    if not event_types:
        raise RuntimeError(f"SSE stream for job {job_id} returned no events")
    if event_types[0] != "job.snapshot":
        raise RuntimeError(
            f"SSE stream for job {job_id} did not start with job.snapshot: {event_types[0]}"
        )
    if event_types[-1] != "job.completed":
        raise RuntimeError(
            f"SSE stream for job {job_id} did not reach job.completed: {event_types[-1]}"
        )
    if task_updates <= 0:
        raise RuntimeError(f"SSE stream for job {job_id} emitted no task.updated events")
    if progress_events <= 0:
        raise RuntimeError(f"SSE stream for job {job_id} emitted no job.progress events")
    if last_payload is None:
        raise RuntimeError(f"SSE stream for job {job_id} ended without a terminal payload")
    counts = last_payload["counts"]
    if counts["pending"] != 0 or counts["in_progress"] != 0:
        raise RuntimeError(
            f"SSE terminal payload for job {job_id} still shows active work: {counts}"
        )
    if counts["completed"] <= 0:
        raise RuntimeError(
            f"SSE terminal payload for job {job_id} completed zero tasks: {counts}"
        )

    summary = {
        "job_id": job_id,
        "completed_count": counts["completed"],
        "failed_count": counts["failed"],
        "progress_events": progress_events,
        "task_updated_events": task_updates,
        "terminal_status": last_payload["status"],
        "total_events": len(event_types),
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
