from __future__ import annotations

import atexit
import json
import logging
import queue
import sys
import uuid
from datetime import date, datetime
from enum import Enum
from logging.handlers import QueueHandler, QueueListener
from typing import Any


_RESERVED_LOG_RECORD_FIELDS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
        }

        event = getattr(record, "event", None)
        if isinstance(event, str) and event:
            payload["event"] = event

        for key, value in record.__dict__.items():
            if key in _RESERVED_LOG_RECORD_FIELDS or key.startswith("_"):
                continue
            if key == "event":
                continue
            payload[key] = _normalize_log_value(value)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)

        return json.dumps(payload, separators=(",", ":"), sort_keys=True)


class _RawQueueHandler(QueueHandler):
    """Enqueue the live LogRecord without pre-formatting.

    The stdlib QueueHandler.prepare() formats the record with the default
    formatter on the calling thread and clears exc_info. We want all formatting
    (including JsonLogFormatter's expensive json.dumps and the exception/stack
    fields) to happen in the listener thread, off the event loop. The queue is an
    in-process queue.Queue, so enqueuing the raw record is safe.
    """

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        return record


def configure_json_logging(level: int = logging.INFO) -> None:
    root_logger = logging.getLogger()
    if getattr(root_logger, "_feedpulse_json_configured", False):
        return

    log_queue: queue.Queue = queue.Queue(-1)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(JsonLogFormatter())

    # The listener drains the queue on a background thread and performs the
    # actual format + write + flush, so the event loop only does a non-blocking
    # in-memory put() on the hot path.
    listener = QueueListener(log_queue, stream_handler, respect_handler_level=True)
    listener.start()

    root_logger.handlers.clear()
    root_logger.addHandler(_RawQueueHandler(log_queue))
    root_logger.setLevel(level)
    root_logger._feedpulse_json_configured = True  # type: ignore[attr-defined]
    root_logger._feedpulse_log_listener = listener  # type: ignore[attr-defined]

    # Worker processes have no FastAPI lifespan; atexit guarantees a final flush.
    atexit.register(shutdown_json_logging)

    logging.captureWarnings(True)


def shutdown_json_logging() -> None:
    """Stop the background log listener, flushing any queued records.

    Idempotent: safe to call from both the FastAPI lifespan teardown and atexit.
    """
    root_logger = logging.getLogger()
    listener: QueueListener | None = getattr(root_logger, "_feedpulse_log_listener", None)
    if listener is not None:
        listener.stop()
        root_logger._feedpulse_log_listener = None  # type: ignore[attr-defined]


def log_event(
    logger: logging.Logger,
    level: int,
    event: str,
    **fields: Any,
) -> None:
    logger.log(level, event, extra={"event": event, **fields})


def _normalize_log_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {
            str(key): _normalize_log_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [_normalize_log_value(item) for item in value]
    return str(value)


configure_json_logging()
