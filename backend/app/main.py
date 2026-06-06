import asyncio
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI

from app.api.routes.health import router as health_router
from app.api.routes.jobs import router as jobs_router
from app.core.logging import configure_json_logging
from app.core.settings import get_settings
from app.middleware import register_middleware
from app.services.job_reconciler import reconcile_running_jobs, run_reconciliation_loop

configure_json_logging()

settings = get_settings()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await reconcile_running_jobs()

    stop_event = asyncio.Event()
    reconciliation_task: asyncio.Task | None = None
    if settings.job_reconciliation_interval_seconds > 0:
        reconciliation_task = asyncio.create_task(
            run_reconciliation_loop(
                stop_event=stop_event,
                interval_seconds=settings.job_reconciliation_interval_seconds,
            )
        )

    try:
        yield
    finally:
        stop_event.set()
        if reconciliation_task is not None:
            with suppress(asyncio.CancelledError):
                await reconciliation_task


app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    summary="Processing control plane for concurrent XML ingestion.",
    lifespan=lifespan,
)

register_middleware(app, settings)

app.include_router(health_router)
app.include_router(jobs_router)