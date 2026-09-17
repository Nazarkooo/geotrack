"""The provisioned dashboard has to stay in step with the metrics the code exports.

A renamed counter is invisible until someone opens Grafana and finds an empty panel,
so every expression in the dashboard is checked against the live registry here.
"""

import json
import re
from pathlib import Path
from typing import Any

import pytest
from prometheus_client import REGISTRY

import geotrack.observability.metrics  # noqa: F401 - importing registers every metric

ROOT = Path(__file__).resolve().parents[2]
DASHBOARD = ROOT / "deploy" / "grafana" / "dashboards" / "geotrack.json"
DATASOURCES = ROOT / "deploy" / "grafana" / "provisioning" / "datasources" / "prometheus.yml"
PROVIDERS = ROOT / "deploy" / "grafana" / "provisioning" / "dashboards" / "geotrack.yml"
PROMETHEUS = ROOT / "deploy" / "prometheus" / "prometheus.yml"

# Where compose mounts deploy/grafana/dashboards inside the Grafana container.
DASHBOARD_MOUNT = "/etc/grafana/dashboards"
METRIC_REFERENCE = re.compile(r"\bgeotrack_[a-z0-9_]+")


def _exported_names() -> set[str]:
    """Every sample name the client library will actually publish."""
    suffixes = {
        "counter": ("_total", "_created"),
        "histogram": ("_bucket", "_sum", "_count", "_created"),
        "summary": ("_sum", "_count", "_created"),
    }
    names: set[str] = set()
    for family in REGISTRY.collect():
        names.add(family.name)
        names.update(family.name + suffix for suffix in suffixes.get(family.type, ()))
    return names


@pytest.fixture(scope="module")
def dashboard() -> dict[str, Any]:
    parsed: dict[str, Any] = json.loads(DASHBOARD.read_text())
    return parsed


@pytest.fixture(scope="module")
def panels(dashboard: dict[str, Any]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = list(dashboard["panels"])
    while pending:
        panel = pending.pop()
        pending.extend(panel.get("panels", []))
        if panel.get("type") != "row":
            flattened.append(panel)
    return flattened


def _datasource_uid() -> str:
    match = re.search(r"^\s*uid:\s*(\S+)\s*$", DATASOURCES.read_text(), re.MULTILINE)
    assert match, "the provisioned datasource must pin an explicit uid"
    return match.group(1)


def test_the_dashboard_is_named_and_populated(
    dashboard: dict[str, Any], panels: list[dict[str, Any]]
) -> None:
    assert dashboard["title"] == "GeoTrack"
    assert dashboard["uid"]
    assert len(panels) >= 12


def test_every_panel_is_labelled(panels: list[dict[str, Any]]) -> None:
    unnamed = [panel["id"] for panel in panels if not panel.get("title")]
    assert unnamed == []


def test_every_expression_uses_a_metric_the_code_exports(panels: list[dict[str, Any]]) -> None:
    exported = _exported_names()
    referenced: set[str] = set()
    for panel in panels:
        for target in panel.get("targets", []):
            referenced.update(METRIC_REFERENCE.findall(target.get("expr", "")))
    assert referenced, "the dashboard should query something"
    assert referenced - exported == set()


def test_every_panel_points_at_the_provisioned_datasource(panels: list[dict[str, Any]]) -> None:
    uid = _datasource_uid()
    for panel in panels:
        assert panel["datasource"]["uid"] == uid, f"panel {panel['title']!r} has a stray datasource"
        for target in panel.get("targets", []):
            assert target["datasource"]["uid"] == uid


def test_the_dashboard_provider_reads_the_mounted_directory() -> None:
    provider = PROVIDERS.read_text()
    assert f"path: {DASHBOARD_MOUNT}" in provider
    # Editing a provisioned dashboard in the browser and losing it on restart is a
    # classic surprise; allowing updates keeps the file the source of truth.
    assert "allowUiUpdates: false" in provider


def test_prometheus_discovers_both_tiers() -> None:
    config = PROMETHEUS.read_text()
    for job in ("api", "processor"):
        assert f"job_name: {job}" in config
    # Replicas come and go, so the targets have to be resolved from Docker's DNS
    # rather than written down.
    assert "dns_sd_configs" in config


def test_the_processor_is_scraped_on_its_own_port() -> None:
    # The processor serves metrics from a side-car HTTP server, not from the API port.
    from geotrack.settings import Settings

    default_port = Settings.model_fields["processor_http_port"].default
    assert f"port: {default_port}" in PROMETHEUS.read_text()
