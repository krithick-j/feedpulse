import logging
import time

from fastapi import FastAPI, Request

from app.core.logging import log_event

logger = logging.getLogger(__name__)


async def log_requests(request: Request, call_next):
    started_at = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        log_event(
            logger,
            logging.ERROR,
            "http.request.failed",
            method=request.method,
            path=request.url.path,
            duration_ms=int((time.perf_counter() - started_at) * 1000),
        )
        raise

    log_event(
        logger,
        logging.INFO,
        "http.request.completed",
        method=request.method,
        path=request.url.path,
        status_code=response.status_code,
        duration_ms=int((time.perf_counter() - started_at) * 1000),
    )
    return response


def register_request_logging(app: FastAPI) -> None:
    app.middleware("http")(log_requests)
