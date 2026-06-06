from __future__ import annotations

from typing import AsyncIterator, Optional

from app.db.notifications import JobEventListener
from app.services.jobs import projections
from app.services.jobs._common import try_parse_job_id, with_repository

SSE_KEEPALIVE_SECONDS = 15.0


class JobEventStream:
    """Live job event stream — single responsibility: turn DB state changes
    into ordered SSE payloads (None marks a keepalive frame)."""

    def __init__(
        self,
        *,
        run_repository=with_repository,
        event_listener_factory=JobEventListener,
    ) -> None:
        self._run = run_repository
        self._event_listener_factory = event_listener_factory

    async def available(self, job_id: str) -> bool:
        job_uuid = try_parse_job_id(job_id)
        if job_uuid is None:
            return False
        projection = await self._run(lambda repository: repository.get_job_projection(job_uuid))
        return projection is not None

    async def stream(self, job_id: str) -> AsyncIterator[Optional[dict]]:
        job_uuid = try_parse_job_id(job_id)
        if job_uuid is None:
            return

        async with self._event_listener_factory(job_id=job_id) as listener:
            initial = await self._run(lambda repository: repository.get_job_projection(job_uuid))
            if initial is None:
                return

            last_snapshot = projections.job_snapshot_payload(initial, "job.snapshot")
            last_tasks = projections.task_payload_map(initial)
            yield last_snapshot

            if projections.is_terminal(initial):
                yield projections.job_snapshot_payload(initial, "job.completed")
                return

            while True:
                notification = await listener.next_event(timeout=SSE_KEEPALIVE_SECONDS)
                projection = await self._run(lambda repository: repository.get_job_projection(job_uuid))
                if projection is None:
                    return

                delta_events, snapshot, current_tasks = projections.projection_delta_events(
                    projection, previous_snapshot=last_snapshot, previous_tasks=last_tasks
                )
                for event in delta_events:
                    yield event

                last_snapshot = snapshot
                last_tasks = current_tasks

                if projections.is_terminal(projection):
                    yield projections.job_snapshot_payload(projection, "job.completed")
                    return

                if notification is None and not delta_events:
                    yield None
