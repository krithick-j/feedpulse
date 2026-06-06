from fastapi import FastAPI

from app.core.settings import Settings
from app.middleware.cors import register_cors
from app.middleware.request_logging import register_request_logging

__all__ = ["register_middleware", "register_cors", "register_request_logging"]


def register_middleware(app: FastAPI, settings: Settings) -> None:
    register_cors(app, settings)
    register_request_logging(app)
