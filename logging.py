"""Logging tools."""

import logging
import os

from opentelemetry import trace
from opentelemetry.trace import format_span_id, format_trace_id
from pythonjsonlogger import jsonlogger

log = logging.getLogger(__name__)


class OpenTelemetryFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        """Join Opentelemetry ids to record.

        Args:
            record: (logging.LogRecord) python logging record object.

        Returns:
            True (always log the object).
        """
        # These details are set so the Google logger can pick them up
        # as structured data. https://github.com/googleapis/python-logging/
        # blob/main/google/cloud/logging_v2/handlers/handlers.py
        span = trace.get_current_span()
        if span is not None and span.get_span_context().is_valid:
            record.trace = format_trace_id(span.get_span_context().trace_id)
            record.span_id = format_span_id(span.get_span_context().span_id)

        return True


def build_logger(level: str) -> None:
    # Fetch the root logger
    root = logging.getLogger()

    # Strip off any existing handlers.
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    handler = logging.StreamHandler()
    handler.addFilter(OpenTelemetryFilter())

    if os.getenv("ENVIRONMENT", "local").lower() not in ("local", "testing"):
        formatter = jsonlogger.JsonFormatter(_build_log_format_string())
        # Set the time format output to an iso8601 style.
        formatter.datefmt = "%Y-%m-%dT%H:%M:%S%Z"
        # Apply the format tot he log handler.
        handler.setFormatter(formatter)

    # Add the handler to the root logger.
    root.addHandler(handler)
    # Set the level of the root logger.
    root.setLevel(level)


def _build_log_format_string() -> str:
    # These are the supported outputs for the JSON log handler.
    # Build these into a log 'format' style string.
    supported_keys = [
        "asctime",
        "created",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "message",
        "module",
        "msecs",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "span_id",
        "thread",
        "threadName",
        "trace",
    ]

    log_format = lambda x: [f"%({i:s})s" for i in x]  # noqa
    return " ".join(log_format(supported_keys))
