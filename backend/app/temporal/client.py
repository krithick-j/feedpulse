from __future__ import annotations

from temporalio.client import Client

from app.core.settings import get_settings


async def get_temporal_client() -> Client:
    settings = get_settings()
    return await Client.connect(
        f"{settings.temporal_host}:{settings.temporal_port}",
        namespace=settings.temporal_namespace,
    )

