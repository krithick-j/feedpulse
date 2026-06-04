from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    app_name: str = "Feedpulse API"
    app_env: str = "development"
    database_url: str = Field(
        default="postgresql+asyncpg://app:app@localhost:5433/app",
        alias="DATABASE_URL",
    )
    alembic_database_url: str = Field(
        default="postgresql://app:app@localhost:5433/app",
        alias="ALEMBIC_DATABASE_URL",
    )
    api_prefix: str = "/api/v1"
    data_backend: Literal["mock", "database"] = Field(default="mock", alias="DATA_BACKEND")
    job_execution_backend: Literal["simulator", "temporal"] = Field(
        default="simulator",
        alias="JOB_EXECUTION_BACKEND",
    )
    cors_origins: list[str] = [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ]
    temporal_host: str = Field(default="localhost", alias="TEMPORAL_HOST")
    temporal_port: int = Field(default=7233, alias="TEMPORAL_PORT")
    temporal_namespace: str = Field(default="default", alias="TEMPORAL_NAMESPACE")
    temporal_workflow_task_queue: str = Field(
        default="feedpulse-workflow-queue",
        alias="TEMPORAL_WORKFLOW_TASK_QUEUE",
    )
    temporal_small_activity_task_queue: str = Field(
        default="xml-small-queue",
        alias="TEMPORAL_SMALL_ACTIVITY_TASK_QUEUE",
    )
    temporal_large_activity_task_queue: str = Field(
        default="xml-large-queue",
        alias="TEMPORAL_LARGE_ACTIVITY_TASK_QUEUE",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
