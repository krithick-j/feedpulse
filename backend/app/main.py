from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.health import router as health_router
from app.api.routes.jobs import router as jobs_router
from app.core.settings import get_settings

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    summary="Processing control plane for concurrent XML ingestion.",
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
