"""Real PostgreSQL/PostGIS and Redis for integration tests.

By default the containers are started with testcontainers (random host ports, so
parallel runs never collide). Set ``TEST_DATABASE_URL`` / ``TEST_REDIS_URL`` to reuse
already running services, as CI does.
"""

import asyncio
import os
import time
from collections.abc import AsyncIterator, Callable, Coroutine, Iterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import asyncpg
import docker
import pytest
import uvicorn
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool
from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage

from geotrack.api.app import create_app
from geotrack.messaging.codec import LocationRecord, decode_record
from geotrack.messaging.keys import STREAM_FIELD, ingest_stream
from geotrack.messaging.redis import close_redis, create_redis
from geotrack.settings import Settings
from tests.conftest import make_settings

ROOT = Path(__file__).resolve().parents[2]
POSTGIS_IMAGE = "geotrack-postgis:test"
REDIS_IMAGE = "redis:8.10.1-alpine"
TABLES = ("users", "geozones", "device_positions", "zone_presence", "alerts", "location_history")


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if Path(str(item.fspath)).is_relative_to(Path(__file__).parent):
            item.add_marker(pytest.mark.integration)


def _ensure_postgis_image() -> None:
    client = docker.from_env()
    try:
        client.images.get(POSTGIS_IMAGE)
    except docker.errors.ImageNotFound:
        DockerImage(path=ROOT / "deploy" / "postgres", tag=POSTGIS_IMAGE, clean_up=False).build()
    finally:
        client.close()


def _wait_until(
    check: Callable[[], Coroutine[Any, Any, None]], *, timeout_s: float, what: str
) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            asyncio.run(check())
        except Exception as exc:
            last_error = exc
            time.sleep(0.25)
        else:
            return
    raise TimeoutError(f"{what} did not become ready: {last_error!r}")


def _asyncpg_dsn(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    if url := os.environ.get("TEST_DATABASE_URL"):
        yield url
        return
    _ensure_postgis_image()
    container = (
        DockerContainer(POSTGIS_IMAGE)
        .with_env("POSTGRES_USER", "geotrack")
        .with_env("POSTGRES_PASSWORD", "geotrack")
        .with_env("POSTGRES_DB", "geotrack")
        .with_exposed_ports(5432)
        # Durability is irrelevant for throwaway test data; this keeps the suite fast.
        .with_command(
            "postgres -c fsync=off -c synchronous_commit=off -c full_page_writes=off "
            "-c max_connections=300"
        )
    )
    container.start()
    try:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5432)
        url = f"postgresql+asyncpg://geotrack:geotrack@{host}:{port}/geotrack"

        async def ready() -> None:
            conn = await asyncpg.connect(_asyncpg_dsn(url), timeout=2)
            try:
                await conn.fetchval("SELECT postgis_full_version()")
            finally:
                await conn.close()

        _wait_until(ready, timeout_s=90, what="PostGIS")
        yield url
    finally:
        container.stop()


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    if url := os.environ.get("TEST_REDIS_URL"):
        yield url
        return
    container = DockerContainer(REDIS_IMAGE).with_exposed_ports(6379)
    container.start()
    try:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        url = f"redis://{host}:{port}/0"

        async def ready() -> None:
            client = Redis.from_url(url)
            try:
                await client.ping()
            finally:
                await client.aclose()

        _wait_until(ready, timeout_s=30, what="Redis")
        yield url
    finally:
        container.stop()


def alembic_config(database_url: str) -> Config:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    config.attributes["database_url"] = database_url
    config.attributes["configure_logging"] = False
    return config


@pytest.fixture(scope="session")
def migrated_database(postgres_url: str) -> str:
    command.upgrade(alembic_config(postgres_url), "head")
    return postgres_url


@pytest.fixture(scope="session")
def settings(migrated_database: str, redis_url: str) -> Settings:
    return make_settings(database_url=migrated_database, redis_url=redis_url)


@pytest.fixture(scope="session")
async def engine(migrated_database: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(migrated_database, poolclass=NullPool)
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture(scope="session")
async def redis_client(redis_url: str) -> AsyncIterator[Redis]:
    client = create_redis(redis_url, purpose="commands")
    yield client
    await close_redis(client)


@pytest.fixture(autouse=True)
async def clean_state(engine: AsyncEngine, redis_client: Redis) -> None:
    # Cleaned before (not after) each test, so a failing test leaves its data behind
    # for inspection while the next test still starts from a known state.
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY CASCADE"))
    await redis_client.flushdb()


@asynccontextmanager
async def running_app(app_settings: Settings) -> AsyncIterator[AsyncClient]:
    """An application with its real lifespan, driven in-process over ASGI."""
    app = create_app(app_settings)
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client


@pytest.fixture
async def api(settings: Settings) -> AsyncIterator[AsyncClient]:
    async with running_app(settings) as client:
        yield client


async def queued_records(redis: Redis, *, shards: int = 8) -> dict[str, list[LocationRecord]]:
    """Every entry waiting on the ingest streams, keyed by stream name."""
    found: dict[str, list[LocationRecord]] = {}
    for shard in range(shards):
        stream = ingest_stream(shard)
        entries = cast(list[tuple[bytes, dict[bytes, bytes]]], await redis.xrange(stream))
        if entries:
            found[stream] = [decode_record(fields[STREAM_FIELD]) for _, fields in entries]
    return found


async def login(client: AsyncClient, username: str) -> dict[str, str]:
    """Sign in and return the Authorization header for that account."""
    response = await client.post("/api/v1/auth/login", json={"username": username})
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


class _QuietServer(uvicorn.Server):
    # Signal handling belongs to pytest here, not to the server under test.
    def install_signal_handlers(self) -> None:
        return


@asynccontextmanager
async def serving_app(app_settings: Settings) -> AsyncIterator[str]:
    """Run the application on a real socket and yield its base URL.

    Websocket behaviour — close codes, frame types, backpressure — is only honest over
    a real connection, so those tests get a genuine server on an ephemeral port instead
    of an in-process ASGI shim.
    """
    config = uvicorn.Config(
        create_app(app_settings),
        host="127.0.0.1",
        port=0,
        log_config=None,
        lifespan="on",
        ws="websockets-sansio",
        access_log=False,
    )
    server = _QuietServer(config)
    task = asyncio.get_running_loop().create_task(server.serve())
    try:
        await _await_startup(server)
        port = server.servers[0].sockets[0].getsockname()[1]
        yield f"127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


async def _await_startup(server: uvicorn.Server, *, timeout_s: float = 20.0) -> None:
    # uvicorn signals readiness with a plain flag rather than an awaitable.
    async with asyncio.timeout(timeout_s):
        while not server.started:  # noqa: ASYNC110
            await asyncio.sleep(0.01)
