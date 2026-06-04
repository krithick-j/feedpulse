from __future__ import annotations

from typing import List, Literal, Optional, Union

from pydantic import BaseModel

JobStatus = Literal["pending", "running", "completed", "completed_with_failures", "failed"]
TaskStatus = Literal["pending", "in_progress", "completed", "failed"]
QueueName = Literal["xml-small-queue", "xml-large-queue"]
AttemptStatus = Literal["running", "succeeded", "failed"]


class JobCounts(BaseModel):
    pending: int
    in_progress: int
    completed: int
    failed: int


class JobSummary(BaseModel):
    id: str
    status: JobStatus
    total_urls: int
    counts: JobCounts
    created_at: str
    started_at: Optional[str]
    finished_at: Optional[str]
    elapsed_ms: int
    temporal_run_id: Optional[str]


class TaskSummary(BaseModel):
    id: int
    url: str
    status: TaskStatus
    queue: Optional[QueueName]
    attempt_count: int
    records_extracted: int
    duration_ms: Optional[int]
    last_error: Optional[str]
    last_error_type: Optional[str]
    started_at: Optional[str]
    finished_at: Optional[str]


class TaskAttempt(BaseModel):
    attempt_number: int
    status: AttemptStatus
    started_at: str
    finished_at: Optional[str]
    duration_ms: Optional[int]
    http_status: Optional[int]
    error_type: Optional[str]
    error_message: Optional[str]


class ExtractedRecord(BaseModel):
    id: str
    title: str
    link: str
    published_at: Optional[str]
    author: Optional[str]
    summary: Optional[str]


class TaskDetail(TaskSummary):
    attempts: List[TaskAttempt]
    sample_records: List[ExtractedRecord]


class JobDetail(JobSummary):
    live: bool
    throughput_per_minute: float
    rerouted_tasks: int
    tasks: List[TaskDetail]


class StartJobRequest(BaseModel):
    idempotency_key: Optional[str] = None


class StartJobResponse(BaseModel):
    job_id: str
    reused: bool


class JobEventBase(BaseModel):
    job_id: str


class JobProgressPayload(JobEventBase):
    counts: JobCounts
    elapsed_ms: int
    status: JobStatus


class TaskUpdatedPayload(JobEventBase):
    task: TaskSummary


class JobSnapshotEvent(BaseModel):
    type: Literal["job.snapshot", "job.progress", "job.completed"]
    payload: JobProgressPayload


class TaskUpdatedEvent(BaseModel):
    type: Literal["task.updated"]
    payload: TaskUpdatedPayload


class JobProjection(BaseModel):
    job: JobSummary
    live: bool
    task_summaries: List[TaskSummary]


JobEvent = Union[JobSnapshotEvent, TaskUpdatedEvent]
