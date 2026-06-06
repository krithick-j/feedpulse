"""Server-Sent Events transport helpers.

Turns an async stream of event payloads (dicts; None for a keepalive) into an
SSE HTTP response. Lives in the API layer because the wire format is a
transport concern, not domain logic.
"""
from __future__ import annotations

import json
from typing import AsyncIterator, Optional

from fastapi.responses import StreamingResponse


def sse_response(payloads: AsyncIterator[Optional[dict]]) -> StreamingResponse:
    return StreamingResponse(_frames(payloads), media_type="text/event-stream")


async def _frames(payloads: AsyncIterator[Optional[dict]]) -> AsyncIterator[str]:
    async for payload in payloads:
        if payload is None:
            yield ": keepalive\n\n"
        else:
            yield f"data: {json.dumps(payload)}\n\n"
