#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
import uuid


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify idempotent job creation through the live Feedpulse API."
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8000/api/v1",
        help="API base URL",
    )
    args = parser.parse_args()

    idempotency_key = f"verify-live-idempotency-{uuid.uuid4()}"
    payload = {"idempotencyKey": idempotency_key}

    first = request_json("POST", f"{args.base_url}/jobs", payload=payload)
    second = request_json("POST", f"{args.base_url}/jobs", payload=payload)

    if first["reused"]:
        raise RuntimeError("First idempotent job creation unexpectedly returned reused=true")
    if not second["reused"]:
        raise RuntimeError("Second idempotent job creation did not return reused=true")
    if first["job_id"] != second["job_id"]:
        raise RuntimeError(
            f"Idempotent job creation returned different job ids: {first['job_id']} vs {second['job_id']}"
        )

    job_detail = request_json("GET", f"{args.base_url}/jobs/{first['job_id']}")
    if job_detail["id"] != first["job_id"]:
        raise RuntimeError(
            f"Loaded job detail id {job_detail['id']} does not match created job {first['job_id']}"
        )

    summary = {
        "idempotency_key": idempotency_key,
        "job_id": first["job_id"],
        "first_reused": first["reused"],
        "second_reused": second["reused"],
        "job_status": job_detail["status"],
        "temporal_run_id": job_detail.get("temporal_run_id"),
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
