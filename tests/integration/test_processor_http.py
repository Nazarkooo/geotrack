"""The processor's probe and metrics surface, served by its own ASGI app.

Running the app also runs the service: the lifespan is what starts the consumers and
what hands the leases back when the container is asked to stop.
"""

from collections.abc import AsyncIterator

import httpx
import pytest
from redis.asyncio import Redis

from geotrack.messaging.keys import PROCESSORS_ZSET, shard_lease_key
from geotrack.processor.service import ProcessorService, create_processor_app
from geotrack.settings import Settings
from tests.conftest import make_settings
from tests.integration.processor_fixtures import wait_for

SHARDS = 2


@pytest.fixture
def http_settings(migrated_database: str, redis_url: str) -> Settings:
    return make_settings(
        database_url=migrated_database,
        redis_url=redis_url,
        ingest_shards=SHARDS,
        processor_block_ms=50,
        log_format="console",
    )


@pytest.fixture
async def client(http_settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    app = create_processor_app(http_settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://processor") as client:
            yield client


async def test_liveness_answers_as_soon_as_the_loop_turns(client: httpx.AsyncClient) -> None:
    response = await client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_readiness_reports_the_shards_this_replica_reads(
    client: httpx.AsyncClient,
) -> None:
    await wait_for(lambda: _ready_with_all_shards(client))

    response = await client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "shards": list(range(SHARDS))}


async def _ready_with_all_shards(client: httpx.AsyncClient) -> bool:
    response = await client.get("/health/ready")
    return response.status_code == 200 and len(response.json()["shards"]) == SHARDS


async def test_metrics_are_exposed_in_the_prometheus_format(client: httpx.AsyncClient) -> None:
    await wait_for(lambda: _ready_with_all_shards(client))

    response = await client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    body = response.text
    assert "geotrack_processor_shards_owned 2.0" in body
    assert "geotrack_processor_batches_total" in body
    assert "geotrack_db_pool_size 4.0" in body


async def test_the_lifespan_owns_the_service_lifecycle(
    http_settings: Settings, redis_client: Redis
) -> None:
    """Shutting the app down is what releases the leases in a rolling deploy."""
    app = create_processor_app(http_settings)

    async with app.router.lifespan_context(app):
        service: ProcessorService = app.state.service
        await wait_for(lambda: _owns_all(service))
        assert await redis_client.get(shard_lease_key(0)) == service.instance_id.encode()

    assert await redis_client.get(shard_lease_key(0)) is None
    assert await redis_client.zcard(PROCESSORS_ZSET) == 0


async def _owns_all(service: ProcessorService) -> bool:
    return len(service.owned_shards) == SHARDS
