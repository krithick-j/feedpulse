from __future__ import annotations

import copy
import random
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from app.schemas.jobs import (
    ExtractedRecord,
    JobCounts,
    JobDetail,
    JobEvent,
    JobProgressPayload,
    JobSnapshotEvent,
    JobSummary,
    PaginatedExtractedRecords,
    StartJobResponse,
    TaskAttempt,
    TaskDetail,
    TaskSummary,
    TaskUpdatedEvent,
    TaskUpdatedPayload,
)

_URLS = [
    "https://news.ycombinator.com/rss",
    "https://www.youtube.com/feeds/videos.xml?channel_id=UC_x5XG1OV2P6uZZ5FSM9Ttw",
    "https://feeds.arstechnica.com/arstechnica/index",
    "https://www.1password.com/blog/feed.xml",
    "https://www.nasa.gov/rss/dyn/breaking_news.rss",
    "https://www.cisa.gov/news.xml",
    "https://www.space.com/feeds/all",
    "https://www.bleepingcomputer.com/feed/",
    "https://www.theverge.com/rss/index.xml",
    "https://www.darkreading.com/rss.xml",
    "https://krebsonsecurity.com/feed/",
    "https://www.crowdstrike.com/blog/feed/",
]

_SAMPLE_RECORDS = [
    ExtractedRecord(
        id="rec-1",
        title="Launch window opens for new orbital test",
        link="https://example.com/launch-window",
        published_at="2026-06-04T05:30:00Z",
        author="Mission Desk",
        summary="Mission control confirms final checks are complete.",
    ),
    ExtractedRecord(
        id="rec-2",
        title="Threat intel brief highlights emerging campaign",
        link="https://example.com/threat-intel",
        published_at="2026-06-04T06:10:00Z",
        author="Ops Team",
        summary="Analysts note an increase in credential-harvesting activity.",
    ),
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _make_task(task_id: int, url: str, status: str, retried: bool = False) -> TaskDetail:
    completed = status == "completed"
    failed = status == "failed"

    if failed:
        attempts = [
            TaskAttempt(
                attempt_number=1,
                status="failed",
                started_at="2026-06-04T06:11:00Z",
                finished_at="2026-06-04T06:11:02Z",
                duration_ms=2100,
                http_status=403,
                error_type="HttpClientError",
                error_message="403 response while fetching feed",
            )
        ]
    elif retried:
        attempts = [
            TaskAttempt(
                attempt_number=1,
                status="failed",
                started_at="2026-06-04T06:09:00Z",
                finished_at="2026-06-04T06:09:01Z",
                duration_ms=1200,
                http_status=429,
                error_type="FeedFetchError",
                error_message="Timeout while fetching feed",
            ),
            TaskAttempt(
                attempt_number=2,
                status="succeeded" if completed else "running",
                started_at="2026-06-04T06:11:00Z",
                finished_at="2026-06-04T06:11:02Z" if completed else None,
                duration_ms=2100 if completed else None,
                http_status=None,
                error_type=None,
                error_message=None,
            ),
        ]
    else:
        attempts = [
            TaskAttempt(
                attempt_number=1,
                status="succeeded" if completed else "running",
                started_at="2026-06-04T06:11:00Z",
                finished_at="2026-06-04T06:11:02Z" if completed else None,
                duration_ms=2100 if completed else None,
                http_status=None,
                error_type=None,
                error_message=None,
            )
        ]

    return TaskDetail(
        id=task_id,
        url=url,
        status=status,
        queue="xml-large-queue" if "youtube" in url else "xml-small-queue",
        attempt_count=len(attempts),
        records_extracted=6 + (task_id % 4) if completed else 0,
        duration_ms=1800 + task_id * 60 if completed else 2100 if failed else None,
        last_error="403 response while fetching feed" if failed else None,
        last_error_type="HttpClientError" if failed else None,
        started_at="2026-06-04T06:11:00Z",
        finished_at="2026-06-04T06:11:02Z" if completed or failed else None,
        attempts=attempts,
        sample_records=copy.deepcopy(_SAMPLE_RECORDS) if completed else [],
    )


def _calculate_counts(tasks: List[TaskDetail]) -> JobCounts:
    counts = {"pending": 0, "in_progress": 0, "completed": 0, "failed": 0}

    for task in tasks:
        counts[task.status] += 1

    return JobCounts(**counts)


def _to_summary(job: JobDetail) -> JobSummary:
    return JobSummary(
        id=job.id,
        status=job.status,
        total_urls=job.total_urls,
        counts=job.counts,
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        elapsed_ms=job.elapsed_ms,
        temporal_run_id=job.temporal_run_id,
    )


class MockJobStore:
    def __init__(self) -> None:
        self._jobs: Dict[str, JobDetail] = {}
        self._idempotency: Dict[str, str] = {}
        self._bootstrap()

    def _bootstrap(self) -> None:
        initial = [
            JobDetail(
                id="job-2026-001",
                status="running",
                total_urls=len(_URLS),
                counts=JobCounts(pending=4, in_progress=3, completed=4, failed=1),
                created_at="2026-06-04T06:10:00Z",
                started_at="2026-06-04T06:10:02Z",
                finished_at=None,
                elapsed_ms=224000,
                temporal_run_id="run-9fa132c2",
                live=True,
                throughput_per_minute=2.1,
                rerouted_tasks=3,
                tasks=[
                    _make_task(1, _URLS[0], "completed"),
                    _make_task(2, _URLS[1], "completed", retried=True),
                    _make_task(3, _URLS[2], "completed"),
                    _make_task(4, _URLS[3], "completed"),
                    _make_task(5, _URLS[4], "failed"),
                    _make_task(6, _URLS[5], "in_progress"),
                    _make_task(7, _URLS[6], "in_progress"),
                    _make_task(8, _URLS[7], "in_progress"),
                    _make_task(9, _URLS[8], "pending"),
                    _make_task(10, _URLS[9], "pending"),
                    _make_task(11, _URLS[10], "pending"),
                    _make_task(12, _URLS[11], "pending"),
                ],
            ),
            JobDetail(
                id="job-2026-000",
                status="completed_with_failures",
                total_urls=len(_URLS),
                counts=JobCounts(pending=0, in_progress=0, completed=10, failed=2),
                created_at="2026-06-04T04:30:00Z",
                started_at="2026-06-04T04:30:03Z",
                finished_at="2026-06-04T04:33:41Z",
                elapsed_ms=218000,
                temporal_run_id="run-34231ab0",
                live=False,
                throughput_per_minute=3.3,
                rerouted_tasks=2,
                tasks=[
                    _make_task(index + 20, url, "failed" if index in (3, 8) else "completed", retried=index == 5)
                    for index, url in enumerate(_URLS)
                ],
            ),
        ]

        for job in initial:
            self._jobs[job.id] = job

    def list_jobs(self) -> List[JobSummary]:
        jobs = sorted(self._jobs.values(), key=lambda item: item.created_at, reverse=True)
        return [_to_summary(copy.deepcopy(job)) for job in jobs]

    def get_job(self, job_id: str) -> Optional[JobDetail]:
        job = self._jobs.get(job_id)
        return copy.deepcopy(job) if job else None

    def get_tasks(self, job_id: str) -> Optional[List[TaskSummary]]:
        job = self._jobs.get(job_id)
        if not job:
            return None

        return [TaskSummary(**task.model_dump(exclude={"attempts", "sample_records"})) for task in job.tasks]

    def get_task(self, job_id: str, task_id: int) -> Optional[TaskDetail]:
        job = self._jobs.get(job_id)
        if not job:
            return None

        for task in job.tasks:
            if task.id == task_id:
                return copy.deepcopy(task)

        return None

    def get_task_records(
        self,
        job_id: str,
        task_id: int,
        *,
        limit: int,
        offset: int,
    ) -> Optional[PaginatedExtractedRecords]:
        task = self.get_task(job_id, task_id)
        if not task:
            return None

        items = task.sample_records[offset: offset + limit]
        return PaginatedExtractedRecords(
            items=items,
            total=len(task.sample_records),
            limit=limit,
            offset=offset,
            has_more=offset + len(items) < len(task.sample_records),
        )

    def start_job(self, idempotency_key: Optional[str]) -> StartJobResponse:
        key = idempotency_key or "server-{0}".format(int(time.time() * 1000))

        existing = self._idempotency.get(key)
        if existing:
            return StartJobResponse(job_id=existing, reused=True)

        created_at = _now_iso()
        job_id = "job-{0}".format(int(time.time() * 1000))
        tasks = [
            _make_task(index + 100, url, "in_progress" if index < 2 else "pending")
            for index, url in enumerate(_URLS)
        ]
        job = JobDetail(
            id=job_id,
            status="running",
            total_urls=len(_URLS),
            counts=JobCounts(pending=len(_URLS) - 2, in_progress=2, completed=0, failed=0),
            created_at=created_at,
            started_at=created_at,
            finished_at=None,
            elapsed_ms=0,
            temporal_run_id="run-{0}".format(random.randint(100000, 999999)),
            live=True,
            throughput_per_minute=0.0,
            rerouted_tasks=0,
            tasks=tasks,
        )

        self._jobs[job_id] = job
        self._idempotency[key] = job_id
        return StartJobResponse(job_id=job_id, reused=False)

    def advance_job(self, job_id: str) -> List[JobEvent]:
        job = self._jobs.get(job_id)
        if not job or not job.live:
            return []

        job.elapsed_ms += 6500
        candidate = None
        for task in job.tasks:
            if task.status == "in_progress":
                candidate = task
                break

        if candidate is None:
            for task in job.tasks:
                if task.status == "pending":
                    candidate = task
                    break

        if candidate is None:
            job.live = False
            return [self._job_event("job.completed", job)]

        if candidate.status == "pending":
            candidate.status = "in_progress"
            candidate.started_at = _now_iso()
            candidate.attempts = [
                TaskAttempt(
                    attempt_number=1,
                    status="running",
                    started_at=candidate.started_at,
                    finished_at=None,
                    duration_ms=None,
                    http_status=None,
                    error_type=None,
                    error_message=None,
                )
            ]
        else:
            candidate.status = "failed" if "darkreading" in candidate.url else "completed"
            candidate.finished_at = _now_iso()
            candidate.duration_ms = 1500 + (candidate.id % 7) * 140
            candidate.records_extracted = 5 + (candidate.id % 4) if candidate.status == "completed" else 0
            candidate.last_error = "429 retry budget exhausted" if candidate.status == "failed" else None
            candidate.last_error_type = "Http429Error" if candidate.status == "failed" else None
            candidate.attempts = [
                TaskAttempt(
                    attempt_number=1,
                    status="failed" if candidate.status == "failed" else "succeeded",
                    started_at=candidate.started_at or _now_iso(),
                    finished_at=candidate.finished_at,
                    duration_ms=candidate.duration_ms,
                    http_status=429 if candidate.status == "failed" else None,
                    error_type="Http429Error" if candidate.status == "failed" else None,
                    error_message="429 retry budget exhausted" if candidate.status == "failed" else None,
                )
            ]
            candidate.sample_records = copy.deepcopy(_SAMPLE_RECORDS) if candidate.status == "completed" else []

        self._refresh_job(job)
        events: List[JobEvent] = [
            TaskUpdatedEvent(
                type="task.updated",
                payload=TaskUpdatedPayload(
                    job_id=job.id,
                    task=TaskSummary(**candidate.model_dump(exclude={"attempts", "sample_records"})),
                ),
            ),
            self._job_event("job.progress" if job.live else "job.completed", job),
        ]
        return events

    def snapshot_event(self, job_id: str) -> Optional[JobSnapshotEvent]:
        job = self._jobs.get(job_id)
        if not job:
            return None

        return self._job_event("job.snapshot", job)

    def _job_event(self, event_type: str, job: JobDetail) -> JobSnapshotEvent:
        return JobSnapshotEvent(
            type=event_type,
            payload=JobProgressPayload(
                job_id=job.id,
                counts=job.counts,
                elapsed_ms=job.elapsed_ms,
                status=job.status,
            ),
        )

    def _refresh_job(self, job: JobDetail) -> None:
        job.counts = _calculate_counts(job.tasks)
        job.rerouted_tasks = len([task for task in job.tasks if task.queue == "xml-large-queue"])
        processed = job.counts.completed + job.counts.failed
        elapsed_minutes = max(job.elapsed_ms / 60000, 1)
        job.throughput_per_minute = round(processed / elapsed_minutes, 1)

        if job.counts.pending == 0 and job.counts.in_progress == 0:
            job.live = False
            job.finished_at = _now_iso()
            job.status = "completed_with_failures" if job.counts.failed else "completed"
        else:
            job.live = True
            job.status = "running"


store = MockJobStore()
