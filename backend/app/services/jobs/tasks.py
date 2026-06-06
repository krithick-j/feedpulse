from __future__ import annotations

from typing import List, Optional

from app.core.settings import get_settings
from app.data.mock_store import store
from app.db.enums import TaskStatus as DbTaskStatus
from app.dto.jobs import PaginatedExtractedRecords, TaskDetail, TaskSummary
from app.services.jobs._common import try_parse_job_id, with_repository

settings = get_settings()


async def list_tasks(
    job_id: str,
    status_filter: Optional[DbTaskStatus],
    sort_by: str,
) -> Optional[List[TaskSummary]]:
    if settings.data_backend == "mock":
        tasks = store.get_tasks(job_id)
        if tasks is None:
            return None
        if status_filter is not None:
            tasks = [task for task in tasks if task.status == status_filter.value]
        return _sort_mock_tasks(tasks, sort_by)

    job_uuid = try_parse_job_id(job_id)
    if job_uuid is None:
        return None
    return await with_repository(
        lambda repository: repository.list_task_summaries(
            job_uuid,
            status_filter=status_filter,
            sort_by=sort_by,
        )
    )


async def get_task(job_id: str, task_id: int) -> Optional[TaskDetail]:
    if settings.data_backend == "mock":
        return store.get_task(job_id, task_id)

    job_uuid = try_parse_job_id(job_id)
    if job_uuid is None:
        return None
    return await with_repository(lambda repository: repository.get_task_detail(job_uuid, task_id))


async def list_task_records(
    job_id: str,
    task_id: int,
    *,
    limit: int,
    offset: int,
) -> Optional[PaginatedExtractedRecords]:
    if settings.data_backend == "mock":
        records = store.get_task_records(job_id, task_id, limit=limit, offset=offset)
        return records or None

    job_uuid = try_parse_job_id(job_id)
    if job_uuid is None:
        return None
    return await with_repository(
        lambda repository: repository.list_task_records(
            job_uuid,
            task_id,
            limit=limit,
            offset=offset,
        )
    )


def _sort_mock_tasks(tasks: List[TaskSummary], sort_by: str) -> List[TaskSummary]:
    if sort_by == "status":
        return sorted(tasks, key=lambda task: (task.status, task.url))
    if sort_by == "duration":
        return sorted(tasks, key=lambda task: (-(task.duration_ms or -1), task.url))
    if sort_by == "records":
        return sorted(tasks, key=lambda task: (-task.records_extracted, task.url))
    if sort_by == "attempts":
        return sorted(tasks, key=lambda task: (-task.attempt_count, task.url))
    return sorted(tasks, key=lambda task: (task.url, task.id))
