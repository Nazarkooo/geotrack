"""``python -m geotrack.processor``: the flags a container is started with."""

from importlib import import_module
from typing import Any

import pytest

from geotrack.processor.service import SHUTDOWN_TIMEOUT_S, create_processor_app
from tests.conftest import make_settings

entry_point = import_module("geotrack.processor.__main__")


def test_the_entry_point_serves_the_probe_port_on_uvloop(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run(app: Any, **options: Any) -> None:
        captured["app"] = app
        captured.update(options)

    monkeypatch.setattr(entry_point.uvicorn, "run", fake_run)
    monkeypatch.setattr(
        entry_point,
        "get_settings",
        lambda: make_settings(processor_http_port=9_876, service_name="geotrack"),
    )

    entry_point.main()

    assert captured["app"] is create_processor_app
    assert captured["factory"] is True
    assert captured["port"] == 9_876
    assert captured["loop"] == "uvloop"
    # The lifespan configures structlog; uvicorn's own dictConfig would replace it.
    assert captured["log_config"] is None
    # Long enough for the service to hand its leases back, and no longer.
    assert captured["timeout_graceful_shutdown"] > SHUTDOWN_TIMEOUT_S
