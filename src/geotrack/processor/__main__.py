"""Entry point: ``python -m geotrack.processor``."""

import uvicorn

from geotrack.processor.service import SHUTDOWN_TIMEOUT_S, create_processor_app
from geotrack.settings import get_settings


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        create_processor_app,
        factory=True,
        host="0.0.0.0",
        port=settings.processor_http_port,
        loop="uvloop",
        # Logging is configured by the lifespan through structlog; uvicorn's own
        # dictConfig would replace those handlers.
        log_config=None,
        access_log=False,
        server_header=False,
        # The shutdown the service gives itself, plus room for the server's own teardown.
        # Anything longer only delays the moment the leases are actually handed back.
        timeout_graceful_shutdown=int(SHUTDOWN_TIMEOUT_S) + 5,
    )


if __name__ == "__main__":
    main()
