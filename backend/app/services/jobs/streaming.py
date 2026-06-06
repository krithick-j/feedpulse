from __future__ import annotations

from typing import AsyncIterator, Optional

from app.services.jobs.gateway import JobRepositoryGateway


class JobEventStream:
    """Live job event stream (SSE payloads; None marks a keepalive frame)."""

    def __init__(self, gateway: JobRepositoryGateway) -> None:
        self._gateway = gateway

    async def available(self, job_id: str) -> bool:
        return await self._gateway.event_stream_available(job_id)

    def stream(self, job_id: str) -> AsyncIterator[Optional[dict]]:
        return self._gateway.event_stream(job_id)
