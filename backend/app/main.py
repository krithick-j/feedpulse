import asyncio
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.health import router as health_router
from app.api.routes.jobs import router as jobs_router
from app.core.settings import get_settings
from app.services.job_reconciler import reconcile_running_jobs, reconciliation_enabled, run_reconciliation_loop

settings = get_settings()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await reconcile_running_jobs()

    stop_event = asyncio.Event()
    reconciliation_task: asyncio.Task | None = None
    if (
        reconciliation_enabled(settings)
        and settings.job_reconciliation_interval_seconds > 0
    ):
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(jobs_router)


@app.get("/api/v1")
async def api_root() -> dict[str, str]:
    return {
        "service": "feedpulse-api",
        "status": "scaffold",
        "next": "jobs, tasks, and events endpoints",
    }
