from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from dataclasses import dataclass
from typing import Optional

import asyncpg
from sqlalchemy.engine import make_url

from app.core.settings import get_settings

JOB_EVENTS_CHANNEL = "job_events"


@dataclass(frozen=True)
class JobNotification:
    job_id: str
    scope: str
    task_id: Optional[int] = None


class JobEventListener:
    def __init__(
        self,
        *,
        job_id: Optional[str] = None,
        channel: str = JOB_EVENTS_CHANNEL,
    ) -> None:
        self.job_id = job_id
        self.channel = channel
        self._connection: Optional[asyncpg.Connection] = None
        self._queue: asyncio.Queue[JobNotification] = asyncio.Queue()

    async def __aenter__(self) -> "JobEventListener":
        self._connection = await asyncpg.connect(_asyncpg_dsn(get_settings().database_url))
        await self._connection.add_listener(self.channel, self._on_notification)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._connection is None:
            return

        with suppress(Exception):
            await self._connection.remove_listener(self.channel, self._on_notification)
        with suppress(Exception):
            await self._connection.close()
        self._connection = None

    async def next_event(self, *, timeout: Optional[float] = None) -> Optional[JobNotification]:
        if timeout is None:
            return await self._queue.get()

        try:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    def _on_notification(self, connection, pid, channel, payload) -> None:
        notification = _parse_job_notification(payload)
        if notification is None:
            return

        if self.job_id is not None and notification.job_id != self.job_id:
            return

        self._queue.put_nowait(notification)


def _asyncpg_dsn(database_url: str) -> str:
    url = make_url(database_url)
    return url.set(drivername=url.drivername.split("+", 1)[0]).render_as_string(hide_password=False)


def _parse_job_notification(payload: str) -> Optional[JobNotification]:
    try:
        raw_payload = json.loads(payload)
    except json.JSONDecodeError:
        return None

    job_id = raw_payload.get("job_id")
    scope = raw_payload.get("scope")
    task_id = raw_payload.get("task_id")

    if not isinstance(job_id, str) or not isinstance(scope, str):
        return None

    if task_id is not None and not isinstance(task_id, int):
        return None

    return JobNotification(job_id=job_id, scope=scope, task_id=task_id)
