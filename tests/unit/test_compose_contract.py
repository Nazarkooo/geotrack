"""Contract tests for the compose topology.

The effective configuration is read back through ``docker compose config`` instead of
by parsing the YAML: that resolves anchors, profiles and ``${VAR}`` interpolation the
same way the engine does, so these assertions describe what actually runs.
"""

import ipaddress
import json
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "docker-compose.yml"
ENV_EXAMPLE = ROOT / ".env.example"
NGINX_CONF = ROOT / "deploy" / "nginx" / "nginx.conf"

DOCKER = shutil.which("docker")
pytestmark = pytest.mark.skipif(DOCKER is None, reason="the docker CLI is required")

# Everything except the database: PostgreSQL needs a writable data directory and its
# own runtime paths, so it is hardened separately (see test_postgres_is_hardened).
HARDENED = ["migrate", "api", "processor", "nginx", "redis", "prometheus", "grafana", "generator"]
OUR_CODE = ["migrate", "api", "processor", "generator"]
SCALED = ["api", "processor"]


def _compose(*args: str, env_file: Path = ENV_EXAMPLE) -> subprocess.CompletedProcess[str]:
    command = [
        str(DOCKER),
        "compose",
        "--env-file",
        str(env_file),
        "-f",
        str(COMPOSE_FILE),
        "--profile",
        "monitoring",
        "--profile",
        "loadtest",
        *args,
    ]
    return subprocess.run(  # noqa: S603 - fixed argument list, no shell
        command, cwd=ROOT, capture_output=True, text=True, check=False, timeout=180
    )


def _bytes(size: str) -> int:
    """Parse a redis-style size (`640mb`, `1gb`, `1024`) into bytes."""
    match = re.fullmatch(r"(\d+)\s*([kmg]b?)?", size.strip().lower())
    assert match is not None, size
    scale = {None: 1, "k": 2**10, "kb": 2**10, "m": 2**20, "mb": 2**20, "g": 2**30, "gb": 2**30}
    return int(match.group(1)) * scale[match.group(2)]


@pytest.fixture(scope="module")
def config() -> dict[str, Any]:
    """The fully resolved topology, interpolated from the documented example values."""
    result = _compose("config", "--format", "json")
    assert result.returncode == 0, result.stderr
    parsed: dict[str, Any] = json.loads(result.stdout)
    return parsed


@pytest.fixture(scope="module")
def services(config: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = config["services"]
    return result


def test_every_expected_service_is_defined(services: dict[str, Any]) -> None:
    assert set(services) == {
        "postgres",
        "redis",
        "migrate",
        "api",
        "processor",
        "nginx",
        "prometheus",
        "grafana",
        "generator",
    }


@pytest.mark.parametrize("name", HARDENED)
def test_container_is_locked_down(services: dict[str, Any], name: str) -> None:
    service = services[name]
    assert service["read_only"] is True, f"{name} should run on a read-only root filesystem"
    assert service["cap_drop"] == ["ALL"], f"{name} should drop every capability"
    assert "no-new-privileges:true" in service["security_opt"]


@pytest.mark.parametrize("name", OUR_CODE)
def test_application_containers_run_as_an_unprivileged_user(
    services: dict[str, Any], name: str
) -> None:
    # The image's USER is geotrack; an explicit `user:` here would silently override it.
    assert "user" not in services[name]
    assert services[name]["image"] == "geotrack-app:latest"


def test_redis_pins_its_uid(services: dict[str, Any]) -> None:
    # Without an explicit uid the entrypoint cannot drop privileges once capabilities
    # are gone, and the server keeps running as root.
    assert services["redis"]["user"] == "999:999"


def test_postgres_is_hardened_without_a_read_only_root(services: dict[str, Any]) -> None:
    postgres = services["postgres"]
    assert "no-new-privileges:true" in postgres["security_opt"]
    assert postgres["cap_drop"] == ["ALL"]
    # Left as root the entrypoint would start privileged and only then step down.
    assert postgres["user"] == "999:999"
    assert postgres.get("read_only") is not True
    assert postgres["shm_size"]


def test_grafana_does_not_reinstall_its_bundled_plugins(services: dict[str, Any]) -> None:
    # Grafana 13 updates its bundled plugins on start. On a read-only root it
    # unregisters the Prometheus plugin, fails to write the replacement and leaves
    # every panel answering "plugin not registered" with no clue on the dashboard.
    assert services["grafana"]["environment"]["GF_PLUGINS_PREINSTALL_DISABLED"] == "true"


@pytest.mark.parametrize("name", [*HARDENED, "postgres"])
def test_logs_are_rotated(services: dict[str, Any], name: str) -> None:
    logging = services[name]["logging"]
    assert logging["driver"] == "json-file"
    assert logging["options"]["max-size"]
    assert logging["options"]["max-file"]


@pytest.mark.parametrize("name", [*OUR_CODE, "postgres", "redis", "nginx"])
def test_resource_limits_are_declared(services: dict[str, Any], name: str) -> None:
    limits = services[name]["deploy"]["resources"]["limits"]
    assert limits["cpus"]
    assert limits["memory"]


def test_only_the_proxy_and_grafana_publish_ports(services: dict[str, Any]) -> None:
    published = {name: svc["ports"] for name, svc in services.items() if svc.get("ports")}
    assert set(published) == {"nginx", "grafana"}
    for name, ports in published.items():
        for port in ports:
            assert port["host_ip"] == "127.0.0.1", f"{name} must not listen on every interface"


@pytest.mark.parametrize("name", SCALED)
def test_stateless_tiers_are_replicated(services: dict[str, Any], name: str) -> None:
    assert services[name]["deploy"]["replicas"] == 2
    # A published port would make the second replica fail to start.
    assert not services[name].get("ports")


def test_startup_order_is_enforced(services: dict[str, Any]) -> None:
    assert services["migrate"]["depends_on"]["postgres"]["condition"] == "service_healthy"
    for name in SCALED:
        depends = services[name]["depends_on"]
        assert depends["migrate"]["condition"] == "service_completed_successfully"
        assert depends["redis"]["condition"] == "service_healthy"
    assert services["nginx"]["depends_on"]["api"]["condition"] == "service_healthy"


def test_the_backend_network_is_internal(config: dict[str, Any]) -> None:
    assert config["networks"]["backend"]["internal"] is True
    assert config["networks"]["edge"].get("internal") is not True


def test_the_networks_have_fixed_addresses(config: dict[str, Any]) -> None:
    """The proxy's rate-limit exemption names a subnet, so the subnet cannot be a lottery."""
    subnets = {
        name: ipaddress.IPv4Network(config["networks"][name]["ipam"]["config"][0]["subnet"])
        for name in ("backend", "edge")
    }
    assert not subnets["backend"].overlaps(subnets["edge"])
    # Docker hands out 172.17.0.0/12 and 192.168.0.0/16 by default; picking from either
    # invites a collision with whatever else the reviewer happens to be running.
    for name, subnet in subnets.items():
        assert not subnet.overlaps(ipaddress.IPv4Network("172.16.0.0/12")), name
        assert not subnet.overlaps(ipaddress.IPv4Network("192.168.0.0/16")), name


def test_the_proxy_exempts_the_backend_subnet_and_nothing_wider(config: dict[str, Any]) -> None:
    """Rate limiting off for all of RFC1918 is rate limiting off.

    Every caller that is not the in-stack load generator - a browser, a device, a
    password guesser - reaches the proxy through a NAT that puts it on a private address,
    so the exemption has to name this stack's own subnet and exclude the gateway that
    published traffic is translated from.
    """
    backend = ipaddress.IPv4Network(config["networks"]["backend"]["ipam"]["config"][0]["subnet"])
    block = re.search(r"geo \$rate_limited_source \{(.*?)\}", NGINX_CONF.read_text(), re.DOTALL)
    assert block is not None, "the proxy no longer keys its limiter on the source address"

    exempt = {
        ipaddress.IPv4Network(cidr)
        for cidr, verdict in re.findall(r"([\d./]+)\s+([01]);", block.group(1))
        if verdict == "0"
    }
    limited = {
        ipaddress.IPv4Network(cidr)
        for cidr, verdict in re.findall(r"([\d./]+)\s+([01]);", block.group(1))
        if verdict == "1"
    }
    assert exempt, "nothing is exempt, so the load generator throttles itself"
    for subnet in exempt:
        assert subnet.subnet_of(backend), f"{subnet} reaches beyond this stack's own network"
    # The longest match wins in nginx, so the gateway entry has to be back in the limited set.
    gateway = ipaddress.IPv4Network(f"{next(backend.hosts())}/32")
    assert gateway in limited, f"{gateway} is where host traffic arrives from and must be limited"


def test_datastores_are_unreachable_from_the_edge(services: dict[str, Any]) -> None:
    for name in ("postgres", "redis", "api", "processor", "prometheus"):
        assert set(services[name]["networks"]) == {"backend"}, f"{name} should stay on the backend"
    assert set(services["nginx"]["networks"]) == {"backend", "edge"}
    assert set(services["grafana"]["networks"]) == {"backend", "edge"}


def test_health_is_checked_everywhere_something_waits_on_it(services: dict[str, Any]) -> None:
    for name in ("postgres", "redis", "api", "processor", "nginx"):
        assert services[name]["healthcheck"]["test"], f"{name} needs a healthcheck"


def test_api_and_processor_agree_on_the_shard_count(services: dict[str, Any]) -> None:
    # A mismatch would silently strand every report on the shards nobody consumes.
    api = services["api"]["environment"]["INGEST_SHARDS"]
    processor = services["processor"]["environment"]["INGEST_SHARDS"]
    assert api == processor


def test_the_processor_runs_the_processor_entrypoint(services: dict[str, Any]) -> None:
    assert services["processor"]["command"] == ["python", "-m", "geotrack.processor"]


def test_optional_services_stay_behind_profiles(services: dict[str, Any]) -> None:
    assert services["prometheus"]["profiles"] == ["monitoring"]
    assert services["grafana"]["profiles"] == ["monitoring"]
    assert services["generator"]["profiles"] == ["loadtest"]


def test_the_load_generator_can_saturate_the_stack(services: dict[str, Any]) -> None:
    generator = services["generator"]
    assert generator["ulimits"]["nofile"]["soft"] >= 65536
    assert generator["sysctls"]["net.ipv4.ip_local_port_range"] == "1024 65000"


def test_missing_secrets_fail_fast(tmp_path: Path) -> None:
    """A partial .env must stop compose with the name of the missing variable."""
    partial = tmp_path / "env"
    partial.write_text(
        "\n".join(
            line
            for line in ENV_EXAMPLE.read_text().splitlines()
            if not line.startswith("JWT_SECRET=")
        )
    )
    result = _compose("config", "-q", env_file=partial)
    assert result.returncode != 0
    assert "JWT_SECRET" in result.stderr


def test_the_example_environment_documents_every_required_variable() -> None:
    documented = {
        line.split("=", 1)[0].strip()
        for line in ENV_EXAMPLE.read_text().splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    }
    referenced = set(re.findall(r"\$\{([A-Z0-9_]+)[:?}-]", COMPOSE_FILE.read_text()))
    assert referenced - documented == set(), "compose refers to undocumented variables"
    assert documented - referenced == set(), ".env.example lists variables nobody reads"


def test_configuration_is_mounted_read_only(services: dict[str, Any]) -> None:
    """Nothing in the stack may rewrite the configuration it was started with."""
    for name in ("nginx", "prometheus", "grafana", "generator"):
        binds = [volume for volume in services[name]["volumes"] if volume["type"] == "bind"]
        assert binds, f"{name} should mount its configuration"
        for bind in binds:
            assert bind["read_only"], f"{name} mounts {bind['target']} writable"


def test_every_bind_mount_exists_as_the_kind_of_thing_it_is_mounted_as(
    services: dict[str, Any],
) -> None:
    """A missing bind source is created by Docker as an empty directory, silently.

    The generator then mounts a directory over /app/generator.py and dies on "can't find
    __main__ module", and the proxy serves 404 for the dashboard while its healthcheck,
    which only probes the locally answered /healthz, goes on reporting 200.
    """
    for name, service in services.items():
        for bind in (volume for volume in service.get("volumes", []) if volume["type"] == "bind"):
            source = Path(bind["source"])
            assert source.exists(), f"{name} mounts {source}, which does not exist"
            if Path(bind["target"]).suffix:
                assert source.is_file(), f"{name} mounts {source} over a file path"
            else:
                assert source.is_dir(), f"{name} mounts {source} over a directory path"


def test_redis_can_fork_for_an_append_only_rewrite(services: dict[str, Any]) -> None:
    """A rewrite forks, and copy-on-write can approach a second copy of the dataset.

    With maxmemory equal to the cgroup limit the kernel kills the process holding every
    un-acked ingest entry precisely when the write load is heaviest, and
    `restart: unless-stopped` brings it back empty mid-run.
    """
    command = services["redis"]["command"]
    maxmemory = _bytes(command[command.index("--maxmemory") + 1])
    limit = int(services["redis"]["deploy"]["resources"]["limits"]["memory"])
    assert maxmemory * 2 <= limit, (
        f"maxmemory {maxmemory / 2**20:.0f}M under a {limit / 2**20:.0f}M limit"
    )


def test_grafana_finds_the_dashboards_its_provider_points_at(services: dict[str, Any]) -> None:
    targets = {volume["target"] for volume in services["grafana"]["volumes"]}
    assert {"/etc/grafana/provisioning", "/etc/grafana/dashboards"} <= targets
