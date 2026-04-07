"""Logging tools."""

import logging
import os
import sys

from azure.core.settings import settings as azure_settings
from azure.identity import ManagedIdentityCredential
from azure.monitor.opentelemetry import configure_azure_monitor
from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.instrumentation.system_metrics import SystemMetricsInstrumentor
from opentelemetry.instrumentation.threading import ThreadingInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
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
        span = trace.get_current_span()
        if span is not None and span.get_span_context().is_valid:
            record.trace_id = format_trace_id(span.get_span_context().trace_id)
            record.span_id = format_span_id(span.get_span_context().span_id)

        return True


class ExtraFormatter(logging.Formatter):
    """Log formatter which can output 'extras' fields."""

    def format(self, record: logging.LogRecord) -> str:
        # These are standard python logging attributes.
        standard_attrs = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "asctime",
            "message",
        }

        # Extract any values added to the log using the `extras` field.
        extras = {k: v for k, v in record.__dict__.items() if k not in standard_attrs}

        # Create a formatted 'extras' string that combines all of the extras.
        record.extra_str = (
            " ".join(f"{k}={v}" for k, v in extras.items()) if extras else ""
        )

        return super().format(record)


def build_logger(level: str) -> None:
    # Fetch the root logger
    root = logging.getLogger()

    # Strip off any existing handlers.
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    # Write to stderr because by default uvicorn will write HTTP logs to stdout.
    # Without this parsing the logs is a pain.
    handler = logging.StreamHandler(stream=sys.stderr)
    handler.addFilter(OpenTelemetryFilter())

    if os.getenv("ENVIRONMENT", "local").lower() not in ("local", "testing"):
        formatter: jsonlogger.JsonFormatter | ExtraFormatter = jsonlogger.JsonFormatter(
            _build_log_format_string()
        )
        # Set the time format output to an iso8601 style.
        formatter.datefmt = "%Y-%m-%dT%H:%M:%S%Z"
        # Apply the format tot he log handler.
        handler.setFormatter(formatter)
    # Apply a default local development format.
    else:
        # extra_str here prints any 'extra' appended fields.
        formatter = ExtraFormatter(
            fmt="%(asctime)s %(levelname)s:%(name)s: %(message)s extras={%(extra_str)s}"
        )
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
        "trace_id",
    ]

    log_format = lambda x: [f"%({i:s})s" for i in x]  # noqa
    return " ".join(log_format(supported_keys))


def instrument_otel(environment: str, release: str, app: FastAPI | None) -> None:
    if environment == "testing":
        log.debug("In testing environment, skipping otel instrumentation...")
        return

    SQLAlchemyInstrumentor().instrument(enable_commenter=True, commenter_options={})
    Psycopg2Instrumentor().instrument(enable_commenter=True, commenter_options={})
    HTTPXClientInstrumentor().instrument()

    configuration = {
        "system.memory.usage": ["used", "free", "cached"],
        "system.cpu.time": ["idle", "user", "system", "irq"],
        "system.network.io": ["transmit", "receive"],
        "process.runtime.memory": ["rss", "vms"],
        "process.runtime.cpu.time": ["user", "system"],
        "process.runtime.context_switches": ["involuntary", "voluntary"],
    }
    SystemMetricsInstrumentor(config=configuration).instrument()  # type: ignore[arg-type]
    ThreadingInstrumentor().instrument()

    log.debug("Instrumenting app with OTEL.")
    if environment in ("dev", "tst", "uat", "prd"):
        azure_settings.tracing_implementation = "opentelemetry"

        # Configure the Distro to authenticate with Azure Monitor
        # using a managed identity credential.
        credential = ManagedIdentityCredential(client_id=os.environ["AZURE_CLIENT_ID"])
        configure_azure_monitor(
            connection_string=os.environ["APPLICATIONINSIGHTS_CONNECTION_STRING"],
            credential=credential,
        )
    # Attempt to add an OTEL span processor for local development.
    else:  # pragma: no-cover
        otel_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
        if otel_endpoint:
            tracer = TracerProvider()
            trace.set_tracer_provider(tracer)
            tracer.add_span_processor(
                BatchSpanProcessor(
                    OTLPSpanExporter(
                        endpoint=otel_endpoint,
                    )
                )
            )
        else:
            log.debug(
                "OTEL_EXPORTER_OTLP_ENDPOINT not configured, skipping OTLP exporter setup."
            )

    if app:
        FastAPIInstrumentor.instrument_app(
            app,
            excluded_urls="/healthz/live,/livez,/healthz/ready,/readyz,/openapi.json",
        )
