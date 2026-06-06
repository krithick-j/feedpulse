from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from datetime import datetime, timezone
import json
from typing import Any, Optional

from sqlalchemy import Select, case, func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.logging import log_event
from app.db.notifications import JOB_EVENTS_CHANNEL, job_events_channel_for_job
from app.db.enums import AttemptStatus, FeedType, JobStatus, TaskStatus
from app.db.models import Job, JobTask, Record, TaskAttempt
from app.schemas.jobs import (
    ExtractedRecord,
    JobCounts,
    JobDetail,
    JobProjection,
    JobSummary,
    PaginatedExtractedRecords,
    StartJobResponse,
    TaskAttempt as TaskAttemptSchema,
    TaskDetail,
    TaskSummary,
)

logger = logging.getLogger(__name__)


class JobRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_job_with_tasks(
        self,
        *,
        idempotency_key: str,
        urls: Sequence[str],
    ) -> StartJobResponse:
        job_id = uuid.uuid4()
        insert_stmt = (
            insert(Job)
            .values(
                id=job_id,
                idempotency_key=idempotency_key,
                total_urls=len(urls),
                status=JobStatus.PENDING,
            )
            .on_conflict_do_nothing(index_elements=[Job.idempotency_key])
            .returning(Job.id)
        )

        result = await self.session.execute(insert_stmt)
        inserted_job_id = result.scalar_one_or_none()

        if inserted_job_id is None:
            existing = await self._get_job_id_by_idempotency_key(idempotency_key)
            if existing is None:
                raise RuntimeError("Job conflict detected but no existing job row was found")

            log_event(
                logger,
                logging.INFO,
                "job.reused",
                job_id=existing,
                idempotency_key=idempotency_key,
            )
            return StartJobResponse(job_id=str(existing), reused=True)

        self.session.add_all(
            [
                JobTask(
                    job_id=inserted_job_id,
                    url=url,
                )
                for url in urls
            ]
        )
        await self._notify_job_event(inserted_job_id, "job.created")
        await self.session.commit()
        log_event(
            logger,
            logging.INFO,
            "job.created",
            job_id=inserted_job_id,
            idempotency_key=idempotency_key,
            total_urls=len(urls),
        )
        return StartJobResponse(job_id=str(inserted_job_id), reused=False)

    async def list_job_summaries(self, *, limit: int = 20) -> list[JobSummary]:
        rows = await self.session.execute(self._job_summary_query().limit(limit))
        return [self._map_summary_row(row) for row in rows.mappings()]

    async def list_running_jobs(self) -> list[JobSummary]:
        rows = await self.session.execute(
            self._job_summary_query().where(Job.status == JobStatus.RUNNING)
        )
        return [self._map_summary_row(row) for row in rows.mappings()]

    async def get_job_detail(self, job_id: uuid.UUID) -> Optional[JobDetail]:
        projection = await self.get_job_projection(job_id)
        if projection is None:
            return None
        tasks_result = await self.session.execute(
            select(JobTask).where(JobTask.job_id == job_id).order_by(JobTask.id.asc())
        )
        tasks = tasks_result.scalars().all()
        counts = projection.job.counts

        return JobDetail(
            **projection.job.model_dump(),
            live=projection.live,
            throughput_per_minute=self._throughput_per_minute(
                counts.completed + counts.failed,
                projection.job.elapsed_ms,
            ),
            rerouted_tasks=sum(1 for task in tasks if task.queue == "xml-large-queue"),
            tasks=[self._map_task_detail_stub(task) for task in tasks],
        )

    async def get_job_projection(self, job_id: uuid.UUID) -> Optional[JobProjection]:
        summary_result = await self.session.execute(
            self._job_summary_query().where(Job.id == job_id)
        )
        summary_row = summary_result.mappings().first()
        if not summary_row:
            return None

        tasks_result = await self.session.execute(
            select(JobTask).where(JobTask.job_id == job_id).order_by(JobTask.id.asc())
        )
        task_rows = tasks_result.scalars().all()
        summary = self._map_summary_row(summary_row)
        live = summary.status == JobStatus.RUNNING.value
        return JobProjection(
            job=summary,
            live=live,
            task_summaries=[self._map_task_summary(task) for task in task_rows],
        )

    async def get_task_detail(self, job_id: uuid.UUID, task_id: int) -> Optional[TaskDetail]:
        task_result = await self.session.execute(
            select(JobTask)
            .where(JobTask.job_id == job_id, JobTask.id == task_id)
        )
        task = task_result.scalar_one_or_none()
        if task is None:
            return None

        attempts_result = await self.session.execute(
            select(TaskAttempt)
            .where(TaskAttempt.task_id == task_id)
            .order_by(TaskAttempt.attempt_number.asc())
        )
        records_result = await self.session.execute(
            select(Record)
            .where(Record.task_id == task_id)
            .order_by(Record.created_at.desc())
            .limit(20)
        )

        return TaskDetail(
            **self._map_task_summary(task).model_dump(),
            attempts=[
                TaskAttemptSchema(
                    attempt_number=attempt.attempt_number,
                    status=attempt.status.value,
                    started_at=attempt.started_at.isoformat(),
                    finished_at=attempt.finished_at.isoformat() if attempt.finished_at else None,
                    duration_ms=attempt.duration_ms,
                    http_status=attempt.http_status,
                    error_type=attempt.error_type,
                    error_message=attempt.error_message,
                )
                for attempt in attempts_result.scalars().all()
            ],
            sample_records=[
                ExtractedRecord(
                    id=str(record.id),
                    title=record.title or "",
                    link=record.link or "",
                    published_at=record.published_at.isoformat() if record.published_at else None,
                    author=record.author,
                    summary=record.summary,
                )
                for record in records_result.scalars().all()
            ],
        )

    async def list_task_summaries(
        self,
        job_id: uuid.UUID,
        *,
        status_filter: Optional[TaskStatus] = None,
        sort_by: str = "url",
    ) -> Optional[list[TaskSummary]]:
        job_exists = await self.session.execute(
            select(Job.id).where(Job.id == job_id)
        )
        if job_exists.scalar_one_or_none() is None:
            return None

        statement = select(JobTask).where(JobTask.job_id == job_id)
        if status_filter is not None:
            statement = statement.where(JobTask.status == status_filter)

        statement = statement.order_by(*self._task_sort_clause(sort_by))
        result = await self.session.execute(statement)
        return [self._map_task_summary(task) for task in result.scalars().all()]

    async def list_task_records(
        self,
        job_id: uuid.UUID,
        task_id: int,
        *,
        limit: int = 50,
        offset: int = 0,
    ) -> Optional[PaginatedExtractedRecords]:
        task_result = await self.session.execute(
            select(JobTask.id)
            .where(JobTask.job_id == job_id, JobTask.id == task_id)
        )
        if task_result.scalar_one_or_none() is None:
            return None

        total_result = await self.session.execute(
            select(func.count(Record.id))
            .where(Record.job_id == job_id, Record.task_id == task_id)
        )
        total = int(total_result.scalar_one())

        result = await self.session.execute(
            select(Record)
            .where(Record.job_id == job_id, Record.task_id == task_id)
            .order_by(Record.published_at.desc().nullslast(), Record.id.desc())
            .offset(offset)
            .limit(limit)
        )
        items = [
            ExtractedRecord(
                id=str(record.id),
                title=record.title or "",
                link=record.link or "",
                published_at=record.published_at.isoformat() if record.published_at else None,
                author=record.author,
                summary=record.summary,
            )
            for record in result.scalars().all()
        ]
        return PaginatedExtractedRecords(
            items=items,
            total=total,
            limit=limit,
            offset=offset,
            has_more=offset + len(items) < total,
        )

    async def list_job_task_rows(self, job_id: uuid.UUID) -> list[JobTask]:
        result = await self.session.execute(
            select(JobTask).where(JobTask.job_id == job_id).order_by(JobTask.id.asc())
        )
        return list(result.scalars().all())

    async def mark_job_running(self, job_id: uuid.UUID, *, temporal_run_id: Optional[str] = None) -> bool:
        job = await self.session.get(Job, job_id)
        if job is None:
            return False

        if job.status == JobStatus.PENDING:
            job.status = JobStatus.RUNNING
            job.started_at = datetime.now(timezone.utc)

        if temporal_run_id and not job.temporal_run_id:
            job.temporal_run_id = temporal_run_id

        await self._notify_job_event(job_id, "job.updated")
        await self.session.commit()
        log_event(
            logger,
            logging.INFO,
            "job.running",
            job_id=job_id,
            temporal_run_id=job.temporal_run_id,
        )
        return True

    async def set_temporal_run_id(self, job_id: uuid.UUID, run_id: str) -> bool:
        job = await self.session.get(Job, job_id)
        if job is None:
            return False

        job.temporal_run_id = run_id
        await self._notify_job_event(job_id, "job.updated")
        await self.session.commit()
        log_event(
            logger,
            logging.INFO,
            "job.temporal_run_id.persisted",
            job_id=job_id,
            temporal_run_id=run_id,
        )
        return True

    async def mark_task_started(self, task_id: int, *, queue: str, attempt_number: int) -> bool:
        task = await self.session.get(JobTask, task_id)
        if task is None:
            return False

        now = datetime.now(timezone.utc)
        task.status = TaskStatus.IN_PROGRESS
        task.queue = queue
        task.started_at = task.started_at or now
        task.attempt_count = max(task.attempt_count, attempt_number)

        await self.session.execute(
            insert(TaskAttempt)
            .values(
                task_id=task_id,
                attempt_number=attempt_number,
                status=AttemptStatus.RUNNING,
                started_at=now,
            )
            .on_conflict_do_nothing(
                index_elements=[TaskAttempt.task_id, TaskAttempt.attempt_number]
            )
        )
        await self._notify_job_event(task.job_id, "task.updated", task_id=task_id)
        await self.session.commit()
        log_event(
            logger,
            logging.INFO,
            "task.started",
            job_id=task.job_id,
            task_id=task_id,
            queue=queue,
            attempt_number=attempt_number,
        )
        return True

    async def complete_task_success(
        self,
        task_id: int,
        *,
        queue: str,
        records: list[dict[str, Any]],
        duration_ms: int,
        attempt_number: int,
    ) -> bool:
        task = await self.session.get(JobTask, task_id)
        if task is None:
            return False

        now = datetime.now(timezone.utc)

        await self.session.execute(
            update(TaskAttempt)
            .where(
                TaskAttempt.task_id == task_id,
                TaskAttempt.attempt_number == attempt_number,
            )
            .values(
                status=AttemptStatus.SUCCEEDED,
                http_status=200,
                error_type=None,
                error_message=None,
                finished_at=now,
                duration_ms=duration_ms,
            )
        )

        if records:
            await self.session.execute(
                insert(Record)
                .values(
                    [
                        {
                            "job_id": task.job_id,
                            "task_id": task.id,
                            "title": record_payload["title"],
                            "link": record_payload["link"],
                            "author": record_payload.get("author"),
                            "summary": record_payload.get("summary"),
                            "published_at": record_payload.get("published_at"),
                            "feed_type": record_payload.get("feed_type", FeedType.RSS),
                            "dedupe_key": record_payload["dedupe_key"],
                            "extra": record_payload.get("extra", {}),
                        }
                        for record_payload in records
                    ]
                )
                .on_conflict_do_nothing(
                    index_elements=[Record.task_id, Record.dedupe_key]
                )
            )

        records_count_result = await self.session.execute(
            select(func.count(Record.id)).where(Record.task_id == task_id)
        )
        records_count = int(records_count_result.scalar_one())

        task.status = TaskStatus.COMPLETED
        task.queue = queue
        task.finished_at = now
        task.duration_ms = duration_ms
        task.attempt_count = max(task.attempt_count, attempt_number)
        task.records_extracted = records_count
        task.last_error = None
        task.last_error_type = None

        await self._notify_job_event(task.job_id, "task.updated", task_id=task_id)
        await self.session.commit()
        log_event(
            logger,
            logging.INFO,
            "task.completed",
            job_id=task.job_id,
            task_id=task_id,
            queue=queue,
            attempt_number=attempt_number,
            records_extracted=records_count,
            duration_ms=duration_ms,
        )
        return True

    async def complete_task_failure(
        self,
        task_id: int,
        *,
        queue: str,
        error_type: str,
        error_message: str,
        http_status: Optional[int],
        duration_ms: int,
        attempt_number: int,
    ) -> bool:
        task = await self.session.get(JobTask, task_id)
        if task is None:
            return False

        now = datetime.now(timezone.utc)

        await self.session.execute(
            update(TaskAttempt)
            .where(
                TaskAttempt.task_id == task_id,
                TaskAttempt.attempt_number == attempt_number,
            )
            .values(
                status=AttemptStatus.FAILED,
                http_status=http_status,
                error_type=error_type,
                error_message=error_message,
                finished_at=now,
                duration_ms=duration_ms,
            )
        )

        task.status = TaskStatus.FAILED
        task.queue = queue
        task.finished_at = now
        task.duration_ms = duration_ms
        task.attempt_count = max(task.attempt_count, attempt_number)
        task.records_extracted = 0
        task.last_error = error_message
        task.last_error_type = error_type

        await self._notify_job_event(task.job_id, "task.updated", task_id=task_id)
        await self.session.commit()
        log_event(
            logger,
            logging.WARNING,
            "task.failed",
            job_id=task.job_id,
            task_id=task_id,
            queue=queue,
            attempt_number=attempt_number,
            duration_ms=duration_ms,
            http_status=http_status,
            error_type=error_type,
            error_message=error_message,
        )
        return True

    async def mark_task_attempt_failed(
        self,
        task_id: int,
        *,
        queue: str,
        error_type: str,
        error_message: str,
        http_status: Optional[int],
        duration_ms: int,
        attempt_number: int,
    ) -> bool:
        task = await self.session.get(JobTask, task_id)
        if task is None:
            return False

        now = datetime.now(timezone.utc)

        await self.session.execute(
            update(TaskAttempt)
            .where(
                TaskAttempt.task_id == task_id,
                TaskAttempt.attempt_number == attempt_number,
            )
            .values(
                status=AttemptStatus.FAILED,
                http_status=http_status,
                error_type=error_type,
                error_message=error_message,
                finished_at=now,
                duration_ms=duration_ms,
            )
        )

        task.status = TaskStatus.IN_PROGRESS
        task.queue = queue
        task.attempt_count = max(task.attempt_count, attempt_number)
        task.last_error = error_message
        task.last_error_type = error_type

        await self._notify_job_event(task.job_id, "task.updated", task_id=task_id)
        await self.session.commit()
        log_event(
            logger,
            logging.WARNING,
            "task.retry_scheduled",
            job_id=task.job_id,
            task_id=task_id,
            queue=queue,
            attempt_number=attempt_number,
            duration_ms=duration_ms,
            http_status=http_status,
            error_type=error_type,
            error_message=error_message,
        )
        return True

    async def finalize_job(self, job_id: uuid.UUID) -> bool:
        job = await self.session.get(Job, job_id)
        if job is None:
            return False

        counts = await self.session.execute(
            select(
                func.count(case((JobTask.status == TaskStatus.FAILED, 1))).label("failed_count"),
                func.count(case((JobTask.status == TaskStatus.COMPLETED, 1))).label("completed_count"),
                func.count(case((JobTask.status == TaskStatus.IN_PROGRESS, 1))).label("in_progress_count"),
                func.count(case((JobTask.status == TaskStatus.PENDING, 1))).label("pending_count"),
            ).where(JobTask.job_id == job_id)
        )
        row = counts.mappings().one()

        if row["pending_count"] or row["in_progress_count"]:
            job.status = JobStatus.RUNNING
        elif row["failed_count"] and row["completed_count"]:
            job.status = JobStatus.COMPLETED_WITH_FAILURES
        elif row["failed_count"]:
            job.status = JobStatus.FAILED
        else:
            job.status = JobStatus.COMPLETED

        if job.status in {
            JobStatus.COMPLETED,
            JobStatus.COMPLETED_WITH_FAILURES,
            JobStatus.FAILED,
        }:
            job.finished_at = datetime.now(timezone.utc)

        await self._notify_job_event(job_id, "job.updated")
        await self.session.commit()
        log_event(
            logger,
            logging.INFO,
            "job.finalized",
            job_id=job_id,
            status=job.status,
            counts={
                "pending": row["pending_count"],
                "in_progress": row["in_progress_count"],
                "completed": row["completed_count"],
                "failed": row["failed_count"],
            },
        )
        return True

    async def fail_incomplete_tasks(
        self,
        job_id: uuid.UUID,
        *,
        error_type: str,
        error_message: str,
    ) -> bool:
        job = await self.session.get(Job, job_id)
        if job is None:
            return False

        unfinished_tasks_result = await self.session.execute(
            select(JobTask).where(
                JobTask.job_id == job_id,
                JobTask.status.in_([TaskStatus.PENDING, TaskStatus.IN_PROGRESS]),
            )
        )
        unfinished_tasks = unfinished_tasks_result.scalars().all()
        if not unfinished_tasks:
            return True

        now = datetime.now(timezone.utc)
        for task in unfinished_tasks:
            if task.status == TaskStatus.IN_PROGRESS and task.attempt_count > 0:
                await self.session.execute(
                    update(TaskAttempt)
                    .where(
                        TaskAttempt.task_id == task.id,
                        TaskAttempt.status == AttemptStatus.RUNNING,
                    )
                    .values(
                        status=AttemptStatus.FAILED,
                        error_type=error_type,
                        error_message=error_message,
                        finished_at=now,
                    )
                )

            task.status = TaskStatus.FAILED
            task.finished_at = task.finished_at or now
            task.last_error = error_message
            task.last_error_type = error_type

        await self._notify_job_event(job_id, "job.updated")
        await self.session.commit()
        log_event(
            logger,
            logging.WARNING,
            "job.incomplete_tasks_failed",
            job_id=job_id,
            error_type=error_type,
            error_message=error_message,
            failed_task_ids=[task.id for task in unfinished_tasks],
        )
        return True

    async def _get_job_id_by_idempotency_key(self, idempotency_key: str) -> Optional[uuid.UUID]:
        result = await self.session.execute(
            select(Job.id).where(Job.idempotency_key == idempotency_key)
        )
        return result.scalar_one_or_none()

    def _job_summary_query(self) -> Select[tuple]:
        return (
            select(
                Job.id,
                Job.status,
                Job.total_urls,
                Job.created_at,
                Job.started_at,
                Job.finished_at,
                Job.temporal_run_id,
                func.count(case((JobTask.status == TaskStatus.PENDING, 1))).label("pending_count"),
                func.count(case((JobTask.status == TaskStatus.IN_PROGRESS, 1))).label("in_progress_count"),
                func.count(case((JobTask.status == TaskStatus.COMPLETED, 1))).label("completed_count"),
                func.count(case((JobTask.status == TaskStatus.FAILED, 1))).label("failed_count"),
                func.extract(
                    "epoch",
                    func.coalesce(Job.finished_at, func.now()) - func.coalesce(Job.started_at, Job.created_at),
                ).label("elapsed_seconds"),
            )
            .outerjoin(JobTask, JobTask.job_id == Job.id)
            .group_by(Job.id)
            .order_by(Job.created_at.desc())
        )

    def _map_summary_row(self, row) -> JobSummary:
        elapsed_ms = int((row["elapsed_seconds"] or 0) * 1000)
        return JobSummary(
            id=str(row["id"]),
            status=row["status"].value if isinstance(row["status"], JobStatus) else row["status"],
            total_urls=row["total_urls"],
            counts=JobCounts(
                pending=row["pending_count"],
                in_progress=row["in_progress_count"],
                completed=row["completed_count"],
                failed=row["failed_count"],
            ),
            created_at=self._iso(row["created_at"]),
            started_at=self._iso(row["started_at"]),
            finished_at=self._iso(row["finished_at"]),
            elapsed_ms=elapsed_ms,
            temporal_run_id=row["temporal_run_id"],
        )

    def _map_task_detail_stub(self, task: JobTask) -> TaskDetail:
        return TaskDetail(
            **self._map_task_summary(task).model_dump(),
            attempts=[],
            sample_records=[],
        )

    def _map_task_summary(self, task: JobTask) -> TaskSummary:
        return TaskSummary(
            id=task.id,
            url=task.url,
            status=task.status.value,
            queue=task.queue,
            attempt_count=task.attempt_count,
            records_extracted=task.records_extracted,
            duration_ms=task.duration_ms,
            last_error=task.last_error,
            last_error_type=task.last_error_type,
            started_at=self._iso(task.started_at),
            finished_at=self._iso(task.finished_at),
        )

    @staticmethod
    def _iso(value: Optional[datetime]) -> Optional[str]:
        if value is None:
            return None

        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)

        return value.isoformat()

    @staticmethod
    def _throughput_per_minute(processed_tasks: int, elapsed_ms: int) -> float:
        if elapsed_ms <= 0:
            return 0.0

        return round(processed_tasks / max(elapsed_ms / 60000, 1), 1)

    @staticmethod
    def _task_sort_clause(sort_by: str):
        if sort_by == "status":
            return (JobTask.status.asc(), JobTask.url.asc())
        if sort_by == "duration":
            return (JobTask.duration_ms.desc().nullslast(), JobTask.url.asc())
        if sort_by == "records":
            return (JobTask.records_extracted.desc(), JobTask.url.asc())
        if sort_by == "attempts":
            return (JobTask.attempt_count.desc(), JobTask.url.asc())
        return (JobTask.url.asc(), JobTask.id.asc())

    async def _notify_job_event(
        self,
        job_id: uuid.UUID,
        scope: str,
        *,
        task_id: Optional[int] = None,
    ) -> None:
        payload = {"job_id": str(job_id), "scope": scope}
        if task_id is not None:
            payload["task_id"] = task_id

        serialized_payload = json.dumps(payload)
        channels = (
            JOB_EVENTS_CHANNEL,
            job_events_channel_for_job(job_id),
        )
        for channel in channels:
            await self.session.execute(
                text("SELECT pg_notify(:channel, :payload)"),
                {
                    "channel": channel,
                    "payload": serialized_payload,
                },
            )
