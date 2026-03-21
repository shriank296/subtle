"""Worker process which pulls in events from service bus."""
import asyncio
import logging
import signal
import time
from collections.abc import Callable
from typing import Any

from brit.fastevent import EventApp
from opentelemetry import trace

from app.common import observability
from app.sb.client import get_async_sb_client
from app.settings import AppSettings, get_app_settings
from app.vault.consumer import handler as pricing_handler

log = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class Worker:
    _TERMINATING: bool = False

    def __init__(self) -> None:
        log.info("Creating EventApp and registering handlers...")
        # This is a bit of a bodge to try and allow us to generate docs from this
        # class.
        self._app = EventApp(None)  # type: ignore
        self._app.include_handler(pricing_handler)

    def main(
        self,
        *,
        app_settings: Callable[[], AppSettings] = get_app_settings,
    ) -> None:
        """Worker process that consumes messages from Service Bus."""

        settings = app_settings()
        observability.build_logger(settings.LOG_LEVEL)

        log.info("Worker starting up in environment: %s", settings.ENVIRONMENT)

        log.info("Instrumenting app with OTEL")
        observability.instrument_otel(settings.ENVIRONMENT, settings.RELEASE, None)

        log.info("Initializing Service Bus client...")
        sb_client = get_async_sb_client(settings)

        # Link our client with the apps client.
        self._app.sb_client = sb_client

        log.info("Starting app...")
        asyncio.run(self._app.run())

        # Keep the main process alive while the background thread processes messages
        log.info("Worker is now running and processing messages...")
        while not self._TERMINATING:
            log.debug("Still running...")
            time.sleep(1)

        log.info("Termination succesful.")

    def stop(self) -> None:
        """Signal the worker to stop."""
        self._TERMINATING = True
        self._app.stop()

    def asyncapi(self) -> dict[str, Any]:
        return self._app.generate_asyncapi_spec(
            title="UPP Vault Async API",
            version="1.0.0",
            description="""This is an AsyncAPI document for UPP Vault.
It contains information about every Topic owned by the service in form of channels.
It contains information about every message published by the service in form of messages and associated schema.""",
            server_host="sb-sis-dev-01.servicebus.windows.net",
            server_name="development",
            server_protocol="amqp",
            server_protocol_version="1.0",
            server_description="Shared Integration Services Development",
        )


if __name__ == "__main__":
    worker = Worker()

    signal.signal(signal.SIGINT, lambda *args, **kwargs: worker.stop())
    signal.signal(signal.SIGTERM, lambda *args, **kwargs: worker.stop())

    worker.main()
