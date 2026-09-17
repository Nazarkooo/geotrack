"""The edge configuration is checked against the real nginx build, not by reading it.

Only nginx itself knows whether `server api:8000 resolve` is accepted, whether the
snippets collide and which headers survive a proxy hop, so the container is started
with the shipped configuration and driven over HTTP.

Every proxy here runs the way compose runs it - read-only root, no capabilities, a
64 MiB /tmp and the memory limit docker-compose.yml declares - so the configuration
under test is the one that ships rather than a roomier one. One of them sits in front of
a stand-in upstream, which is what makes the forwarded headers and the websocket upgrade
observable, and callers are placed on the compose backend subnet or off it to exercise
both sides of the rate-limit exemption.
"""

import base64
import hashlib
import ipaddress
import json
import re
import select
import shutil
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import docker
import httpx
import pytest
from docker.types import IPAMConfig, IPAMPool
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "deploy" / "nginx"
COMPOSE_FILE = ROOT / "docker-compose.yml"
ENV_EXAMPLE = ROOT / ".env.example"
FRONTEND = ROOT / "frontend"
IMAGE = "nginxinc/nginx-unprivileged:1.30.5-alpine"

INDEX = "<!doctype html><title>GeoTrack</title>"
ASSET = "/* fingerprinted asset */"
UNVERSIONED = "/* rebuilt in place */"

# What the stand-in upstream reports back about the request it was handed.
ECHO_BODY = (
    "{"
    + ",".join(
        f'"{name}":"${variable}"'
        for name, variable in (
            ("x_forwarded_for", "http_x_forwarded_for"),
            ("x_real_ip", "http_x_real_ip"),
            ("forwarded", "http_forwarded"),
            ("connection", "http_connection"),
            ("upgrade", "http_upgrade"),
            ("proto", "http_x_forwarded_proto"),
            ("peer", "remote_addr"),
        )
    )
    + "}"
)

# A stand-in for the API tier, written in nginx so the suite needs no second image.
# `/ws` is deliberately slow: limit_req without `nodelay` parks every request after the
# first for a minute, which holds the edge's connections open long enough to observe its
# own per-client ceiling. Every other path echoes the headers it was given.
UPSTREAM_CONF = """
worker_processes 1;
pid /tmp/nginx.pid;
error_log /var/log/nginx/error.log warn;

events { worker_connections 4096; }

http {
    access_log off;
    server_tokens off;

    limit_req_zone $binary_remote_addr zone=hold:1m rate=1r/m;

    server {
        listen 8000;
        default_type application/json;

        location = /ws {
            limit_req zone=hold burst=2000;
            root /usr/share/nginx/html;
            try_files /index.html =404;
        }

        location / {
            return 200 'ECHO_BODY';
        }
    }
}
""".replace("ECHO_BODY", ECHO_BODY)

DOCKER = shutil.which("docker")
pytestmark = pytest.mark.skipif(DOCKER is None, reason="the docker CLI is required")


@pytest.fixture(scope="module")
def compose_config() -> dict[str, Any]:
    """The resolved compose topology, so this file and the stack cannot drift apart."""
    result = subprocess.run(  # noqa: S603 - fixed argument list, no shell
        [
            str(DOCKER),
            "compose",
            "--env-file",
            str(ENV_EXAMPLE),
            "-f",
            str(COMPOSE_FILE),
            "config",
            "--format",
            "json",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    assert result.returncode == 0, result.stderr
    parsed: dict[str, Any] = json.loads(result.stdout)
    return parsed


@pytest.fixture(scope="module")
def memory_limit(compose_config: dict[str, Any]) -> int:
    return int(compose_config["services"]["nginx"]["deploy"]["resources"]["limits"]["memory"])


@pytest.fixture(scope="module")
def backend_subnet(compose_config: dict[str, Any]) -> str:
    subnet: str = compose_config["networks"]["backend"]["ipam"]["config"][0]["subnet"]
    return subnet


@pytest.fixture(scope="module")
def document_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("html")
    (root / "index.html").write_text(INDEX)
    (root / "assets").mkdir()
    (root / "assets" / "app.a1b2c3d4.css").write_text(ASSET)
    (root / "assets" / "app.css").write_text(UNVERSIONED)
    return root


def _base_url(container: DockerContainer) -> str:
    """Block until nginx answers; a bad config kills it before it ever listens."""
    host = container.get_container_host_ip()
    port = container.get_exposed_port(8080)
    base_url = f"http://{host}:{port}"
    deadline = time.monotonic() + 30
    while True:
        try:
            if httpx.get(f"{base_url}/healthz", timeout=2).status_code == 200:
                return base_url
        except httpx.HTTPError:
            pass
        if time.monotonic() > deadline:
            raise TimeoutError(f"nginx never served /healthz:\n{container.get_logs()!r}")
        time.sleep(0.25)


def _wait_for_upstream(container: DockerContainer) -> None:
    """The upstream is discovered through Docker DNS, which takes a moment to answer."""
    base_url = _base_url(container)
    deadline = time.monotonic() + 30
    while httpx.get(f"{base_url}/api/v1/devices", timeout=5).status_code != 200:
        if time.monotonic() > deadline:
            raise TimeoutError(f"the stand-in upstream never answered:\n{container.get_logs()!r}")
        time.sleep(0.5)


def _edge_container(document_root: Path, memory_limit: int) -> DockerContainer:
    """nginx configured and constrained exactly the way compose runs it."""
    return (
        DockerContainer(IMAGE)
        .with_volume_mapping(str(DEPLOY / "nginx.conf"), "/etc/nginx/nginx.conf", "ro")
        .with_volume_mapping(str(DEPLOY / "snippets"), "/etc/nginx/snippets", "ro")
        .with_volume_mapping(str(document_root), "/usr/share/nginx/html", "ro")
        .with_exposed_ports(8080)
        # The read-only root leaves nginx nowhere else to put its pid and temp files.
        .with_tmpfs_mount("/tmp", "size=64m")  # noqa: S108
        .with_kwargs(
            mem_limit=memory_limit,
            memswap_limit=memory_limit,
            read_only=True,
            cap_drop=["ALL"],
            security_opt=["no-new-privileges:true"],
            ulimits=[{"Name": "nofile", "Soft": 65536, "Hard": 65536}],
        )
    )


@pytest.fixture(scope="module")
def edge(document_root: Path, memory_limit: int) -> Iterator[DockerContainer]:
    container = _edge_container(document_root, memory_limit)
    container.start()
    try:
        _base_url(container)
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="module")
def client(edge: DockerContainer) -> Iterator[httpx.Client]:
    with httpx.Client(base_url=_base_url(edge), timeout=15) as client:
        yield client


@pytest.fixture(scope="module")
def private_network() -> Iterator[Network]:
    """A network of this module's own, so `api` resolves to the stand-in and nothing else."""
    with Network() as network:
        yield network


@pytest.fixture(scope="module")
def compose_subnet_network(backend_subnet: str) -> Iterator[Any]:
    """A network on the compose backend subnet, so source addresses match production.

    Docker refuses overlapping address pools, so when the stack itself is up it already
    owns that subnet and this joins that network rather than claiming it a second time.
    """
    engine = docker.from_env()
    for candidate in engine.networks.list():
        pools = (candidate.attrs.get("IPAM") or {}).get("Config") or []
        if any(pool.get("Subnet") == backend_subnet for pool in pools):
            yield candidate
            return

    network = engine.networks.create(
        f"geotrack-subnet-{uuid.uuid4().hex[:8]}",
        driver="bridge",
        ipam=IPAMConfig(pool_configs=[IPAMPool(subnet=backend_subnet)]),
    )
    try:
        yield network
    finally:
        network.remove()


@pytest.fixture(scope="module")
def upstream(
    private_network: Network, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[DockerContainer]:
    """The stand-in API, reachable as `api` exactly as the real one is."""
    conf = tmp_path_factory.mktemp("upstream") / "nginx.conf"
    conf.write_text(UPSTREAM_CONF)
    container = (
        DockerContainer(IMAGE)
        .with_volume_mapping(str(conf), "/etc/nginx/nginx.conf", "ro")
        .with_network(private_network)
        .with_network_aliases("api")
    )
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="module")
def proxied(
    document_root: Path,
    memory_limit: int,
    private_network: Network,
    upstream: DockerContainer,
) -> Iterator[DockerContainer]:
    """The edge with a live upstream behind it, published to the host the way it ships.

    That is the production topology: what is published to the host arrives translated
    from a gateway address the proxy does not exempt.
    """
    container = (
        _edge_container(document_root, memory_limit)
        .with_network(private_network)
        .with_network_aliases("proxy")
    )
    container.start()
    try:
        _base_url(container)
        _wait_for_upstream(container)
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="module")
def in_stack_probe(
    document_root: Path, memory_limit: int, compose_subnet_network: Any
) -> Iterator[DockerContainer]:
    """A caller sitting inside the compose backend subnet, like the load generator.

    The proxy it calls is its own: attaching the shared one to a network the real stack
    may also be on would let `api` resolve to something this module does not control.
    """
    subnet_edge = _edge_container(document_root, memory_limit)
    subnet_edge.start()
    probe = DockerContainer(IMAGE)
    probe.start()
    try:
        compose_subnet_network.connect(subnet_edge.get_wrapped_container().id, aliases=["edge"])
        compose_subnet_network.connect(probe.get_wrapped_container().id)
        yield probe
    finally:
        probe.stop()
        subnet_edge.stop()


@pytest.fixture(scope="module")
def proxied_client(proxied: DockerContainer) -> Iterator[httpx.Client]:
    with httpx.Client(base_url=_base_url(proxied), timeout=15) as client:
        yield client


@pytest.fixture(scope="module")
def off_stack_probe(
    private_network: Network, proxied: DockerContainer
) -> Iterator[DockerContainer]:
    """A caller on a private network that is not this stack's: a VPN, a corporate NAT."""
    probe = DockerContainer(IMAGE).with_network(private_network)
    probe.start()
    try:
        yield probe
    finally:
        probe.stop()


def _count_refusals(probe: DockerContainer, url: str, attempts: int) -> int:
    """How many of `attempts` requests the edge answered with 429, as busybox reports it."""
    result = probe.exec(
        [
            "sh",
            "-c",
            f"i=0; while [ $i -lt {attempts} ]; do wget -q -O /dev/null {url}; "
            "i=$((i+1)); done 2>&1 | grep -c ' 429 ' || true",
        ]
    )
    assert result.exit_code == 0, result.output
    return int(result.output.decode().strip())


def _upstream_saw(response: httpx.Response) -> dict[str, str]:
    assert response.status_code == 200, response.text
    parsed: dict[str, str] = response.json()
    return parsed


def _script_src(policy: str) -> str:
    return next(part for part in policy.split(";") if part.strip().startswith("script-src"))


def test_the_shipped_configuration_is_valid(client: httpx.Client) -> None:
    # nginx is answering at all, which it only does once the whole config parsed.
    assert client.get("/healthz").text.strip() == "ok"


def test_the_entry_document_is_never_cached(client: httpx.Client) -> None:
    for path in ("/", "/index.html"):
        response = client.get(path)
        assert response.status_code == 200
        assert response.text == INDEX
        assert response.headers["cache-control"] == "no-store"


def test_fingerprinted_assets_are_cached_forever(client: httpx.Client) -> None:
    response = client.get("/assets/app.a1b2c3d4.css")
    assert response.status_code == 200
    assert "immutable" in response.headers["cache-control"]


def test_assets_without_a_fingerprint_are_not_pinned_for_a_year(client: httpx.Client) -> None:
    """A rebuilt /assets/app.css has to be able to reach visitors who hold the old one."""
    response = client.get("/assets/app.css")
    assert response.status_code == 200
    assert response.text == UNVERSIONED
    assert response.headers["cache-control"] == "public, max-age=300"


def test_security_headers_are_served_with_the_dashboard(client: httpx.Client) -> None:
    headers = client.get("/").headers
    assert headers["x-content-type-options"] == "nosniff"
    assert headers["x-frame-options"] == "DENY"
    assert headers["referrer-policy"] == "no-referrer"
    policy = headers["content-security-policy"]
    # The map stack needs its CDN, the basemap tiles and a blob worker.
    assert "https://cdn.jsdelivr.net" in policy
    assert "https://tiles.openfreemap.org" in policy
    assert "worker-src blob:" in policy
    assert "frame-ancestors 'none'" in policy


def test_the_inline_import_map_is_allowed_by_its_own_hash(client: httpx.Client) -> None:
    """An import map can only be inline, so the policy has to name this exact block.

    The digest is recomputed from the shipped index.html, so editing the import map
    without reissuing the policy fails here rather than as a blank dashboard in a
    browser: without it Chromium refuses the block, the bare "maplibre-gl" specifier
    never resolves and the page renders nothing at all.
    """
    html = (FRONTEND / "index.html").read_text()
    inline = re.search(r'<script type="importmap">(.*?)</script>', html, re.DOTALL)
    assert inline is not None, "the dashboard no longer carries an inline import map"
    digest = base64.b64encode(hashlib.sha256(inline.group(1).encode()).digest()).decode()

    script_src = _script_src(client.get("/").headers["content-security-policy"])
    assert f"'sha256-{digest}'" in script_src, f"import map hash missing from {script_src!r}"


def test_the_policy_does_not_simply_allow_every_inline_script(client: httpx.Client) -> None:
    policy = client.get("/").headers["content-security-policy"]
    assert "'unsafe-inline'" not in _script_src(policy)
    assert "object-src 'none'" in policy
    assert "base-uri 'none'" in policy


def test_the_server_version_is_not_advertised(client: httpx.Client) -> None:
    assert client.get("/").headers["server"] == "nginx"


def test_metrics_are_refused_at_the_edge(client: httpx.Client) -> None:
    # Prometheus reaches the replicas directly; nobody scrapes through the proxy.
    assert client.get("/metrics").status_code == 403


def test_unknown_paths_do_not_fall_back_to_the_dashboard(client: httpx.Client) -> None:
    assert client.get("/not-a-real-file.js").status_code == 404


@pytest.mark.parametrize("path", ["/api/v1/geozones", "/ws", "/ws/ingest", "/health/ready"])
def test_application_paths_are_proxied_rather_than_served(client: httpx.Client, path: str) -> None:
    """Without an upstream these must fail as a gateway, never as a missing file."""
    response = client.get(path)
    assert response.status_code == 502, f"{path} was not handed to the upstream"


def test_the_worker_count_follows_the_cpu_grant_not_the_host(
    edge: DockerContainer, compose_config: dict[str, Any]
) -> None:
    """`worker_processes auto` reads the host's cores and ignores the cgroup quota."""
    result = edge.exec(["sh", "-c", "ps -o args | grep -c '[w]orker process'"])
    assert result.exit_code == 0, result.output
    workers = int(result.output.decode().strip())
    granted = float(compose_config["services"]["nginx"]["deploy"]["resources"]["limits"]["cpus"])
    assert workers == int(granted), f"{workers} workers for {granted} granted CPUs"


def test_the_edge_is_nearly_empty_at_idle(edge: DockerContainer, memory_limit: int) -> None:
    """Idle nginx has to leave room for the connections it is configured to accept.

    A worker preallocates its entire connection table, so one worker per host core turns
    an idle proxy into 83% of this limit, and the first burst of traffic then OOM-kills
    workers while the master keeps answering /healthz with 200.
    """
    result = edge.exec(["cat", "/sys/fs/cgroup/memory.current"])
    assert result.exit_code == 0, result.output
    used = int(result.output.decode().strip())
    assert used < memory_limit // 4, (
        f"idle nginx already holds {used / 2**20:.0f} MiB of {memory_limit / 2**20:.0f} MiB"
    )


def test_the_connection_budget_fits_the_memory_limit(memory_limit: int) -> None:
    """Every slot nginx will admit has to be affordable, not only the ones we expect.

    A live proxied websocket costs roughly 32 KiB of container memory for the pair of
    slots it occupies, measured on this image, so the limit has to cover the ceiling the
    configuration itself allows rather than the traffic we hope for.
    """
    config = (DEPLOY / "nginx.conf").read_text()
    workers = re.search(r"^worker_processes\s+(\d+);", config, re.MULTILINE)
    connections = re.search(r"^\s*worker_connections\s+(\d+);", config, re.MULTILINE)
    assert workers is not None, "worker_processes must be pinned to a number, not `auto`"
    assert connections is not None

    slots = int(workers.group(1)) * int(connections.group(1))
    needed = slots * 16 * 1024 + 48 * 2**20  # per-slot cost plus the preallocated tables
    assert needed <= memory_limit, (
        f"{slots} admissible connections need ~{needed / 2**20:.0f} MiB "
        f"but the container is capped at {memory_limit / 2**20:.0f} MiB"
    )


def test_accepted_connections_are_spread_across_every_worker(edge: DockerContainer) -> None:
    """Without `reuseport` one worker wins nearly every accept and exhausts its table.

    Its siblings then idle while the busy one logs "worker_connections are not enough"
    and starts dropping sockets, which looks exactly like a capacity problem and is not.
    """
    host = edge.get_container_host_ip()
    port = int(edge.get_exposed_port(8080))
    held: list[socket.socket] = []
    try:
        for _ in range(300):
            sock = socket.create_connection((host, port), timeout=10)
            sock.sendall(b"GET /healthz HTTP/1.1\r\nHost: edge\r\nConnection: keep-alive\r\n\r\n")
            sock.recv(4096)
            held.append(sock)

        result = edge.exec(
            ["sh", "-c", "for p in $(pgrep -f '[w]orker process'); do ls /proc/$p/fd | wc -l; done"]
        )
        assert result.exit_code == 0, result.output
        per_worker = [int(line) for line in result.output.decode().split()]
    finally:
        for sock in held:
            sock.close()

    assert len(per_worker) >= 2, per_worker
    assert min(per_worker) > len(held) // 4, f"connections landed unevenly: {per_worker}"


def test_an_ordinary_client_is_rate_limited_on_login(client: httpx.Client) -> None:
    """The brute-force guard has to engage for traffic arriving through the published port.

    Docker translates everything off-host to a gateway address, so an exemption written
    as "anything in RFC1918" switches the limiter off for every caller that matters.
    """
    with ThreadPoolExecutor(20) as pool:
        codes = list(
            pool.map(
                lambda i: client.post("/api/v1/auth/login", json={"username": f"n{i}"}).status_code,
                range(120),
            )
        )
    assert 429 in codes, f"the login limiter never engaged: {sorted(set(codes))}"
    # rate=10r/s with burst=20 lets a short spike through and then clamps down hard.
    assert codes.count(429) > len(codes) // 2, codes.count(429)


def test_a_client_on_someone_elses_private_network_is_still_rate_limited(
    off_stack_probe: DockerContainer,
) -> None:
    """RFC1918 is not a trust boundary: corporate NAT, VPNs and cloud load balancers live there.

    Exempting 10/8, 172.16/12 and 192.168/16 - which is what "requests from inside the
    compose networks" turns into when written that way - leaves the login guard switched
    off for every caller that matters.
    """
    # rate=10r/s plus burst=20 admits `10 * elapsed + 21`, so this margin holds even if
    # the loop crawls: 200 attempts would have to take 14 seconds before it gets tight.
    refused = _count_refusals(off_stack_probe, "http://proxy:8080/api/v1/auth/login", 200)
    assert refused >= 40, f"only {refused} of 200 logins were refused"


def test_traffic_from_inside_the_stack_is_not_rate_limited(
    in_stack_probe: DockerContainer,
) -> None:
    """The load generator legitimately drives thousands of requests a second from one IP.

    There is no upstream behind this proxy, so every attempt ends as a gateway error;
    what matters is that the limiter refuses none of them, while the same burst from
    outside the subnet is refused sixty times over.
    """
    assert _count_refusals(in_stack_probe, "http://edge:8080/api/v1/auth/login", 120) == 0


def test_a_forged_client_address_never_reaches_the_application(
    proxied_client: httpx.Client,
) -> None:
    """nginx is the trust boundary: what it forwards must describe the peer it accepted.

    Appending to the caller's header instead leaves the forged entry leftmost, and the
    leftmost entry is the one the application resolves the client from.
    """
    saw = _upstream_saw(
        proxied_client.get(
            "/api/v1/devices",
            headers={
                "X-Forwarded-For": "203.0.113.9",
                "X-Real-IP": "203.0.113.9",
                "Forwarded": "for=198.51.100.7",
                "X-Forwarded-Proto": "https",
            },
        )
    )
    assert "203.0.113.9" not in saw["x_forwarded_for"]
    assert "203.0.113.9" not in saw["x_real_ip"]
    assert saw["x_forwarded_for"] == saw["x_real_ip"]
    assert saw["forwarded"] == ""
    assert saw["proto"] == "http"


def test_the_real_client_address_is_what_the_application_sees(
    upstream: DockerContainer, proxied_client: httpx.Client
) -> None:
    """A forged header must not be the only thing the overwrite gets right."""
    assert proxied_client.get("/healthz").status_code == 200
    result = upstream.exec(["sh", "-c", "wget -q -O - http://proxy:8080/api/v1/devices"])
    assert result.exit_code == 0, result.output
    saw = json.loads(result.output.decode())

    forwarded = saw["x_forwarded_for"]
    assert "," not in forwarded, f"more than one hop was forwarded: {forwarded!r}"
    ipaddress.ip_address(forwarded)
    # The caller here is the upstream container itself, not the proxy in front of it.
    assert forwarded != saw["peer"]


def test_websocket_locations_negotiate_an_upgrade_and_rest_locations_do_not(
    proxied_client: httpx.Client,
) -> None:
    """Sending `Connection` on the REST locations would discard the keepalive pool."""
    upgrade_headers = {"Upgrade": "websocket", "Connection": "Upgrade"}

    websocket = _upstream_saw(proxied_client.get("/ws/ingest", headers=upgrade_headers))
    assert websocket["connection"] == "upgrade"
    assert websocket["upgrade"] == "websocket"

    rest = _upstream_saw(proxied_client.get("/api/v1/devices", headers=upgrade_headers))
    assert rest["connection"] == ""
    assert rest["upgrade"] == ""


def test_one_client_cannot_occupy_the_whole_websocket_table(
    proxied: DockerContainer, proxied_client: httpx.Client
) -> None:
    """Dashboard websockets are held for as long as a tab is open and cost two slots each.

    The gateway's per-user session cap cannot help here: it only ever sees a handshake
    that already completed, so the ceiling has to be enforced before the upgrade.
    """
    assert proxied_client.get("/healthz").status_code == 200
    host = proxied.get_container_host_ip()
    port = int(proxied.get_exposed_port(8080))
    held: list[socket.socket] = []
    rejected = 0
    try:
        for _ in range(600):
            sock = socket.create_connection((host, port), timeout=10)
            sock.sendall(b"GET /ws HTTP/1.1\r\nHost: edge\r\nConnection: keep-alive\r\n\r\n")
            held.append(sock)
        # Whatever the edge admitted is still parked on the slow upstream and silent;
        # anything it refused has already answered.
        answered, _, _ = select.select(held, [], [], 5)
        for sock in answered:
            if b" 429 " in sock.recv(64):
                rejected += 1
    finally:
        for sock in held:
            sock.close()

    accepted = len(held) - rejected
    assert rejected > 0, f"one address opened {len(held)} dashboard websockets unimpeded"
    assert accepted >= 256, f"the ceiling is too tight for an ordinary client: {accepted}"
