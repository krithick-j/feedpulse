from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from typing import Optional

from app.db.session import get_db_session
from app.repositories.jobs import JobRepository


async def with_repository(operation: Callable[[JobRepository], Awaitable]):
    async for session in get_db_session():
        repository = JobRepository(session)
        return await operation(repository)

    raise RuntimeError("Database session could not be created")


def try_parse_job_id(job_id: str) -> Optional[uuid.UUID]:
    try:
        return uuid.UUID(job_id)
    except ValueError:
        return None
