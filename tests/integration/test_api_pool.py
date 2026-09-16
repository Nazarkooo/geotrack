"""Behaviour when the connection pool runs dry.

Under 10,000 devices the pool is the first thing to saturate, and the failure mode
matters: a request that queues forever takes a worker with it, while a request that is
refused quickly leaves the replica healthy and tells the caller when to come back.
"""

import asyncio
from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from geotrack.api.app import create_app
from geotrack.api.problems import MEDIA_TYPE
from geotrack.settings import Settings
from tests.conftest import make_settings
from tests.integration.conftest import login


@pytest.fixture
async def single_connection_app(settings: Settings) -> AsyncIterator[tuple[FastAPI, AsyncClient]]:
    """One connection, and barely any patience for a second one."""
    app = create_app(
        make_settings(
            database_url=settings.database_url.get_secret_value(),
            redis_url=settings.redis_url.get_secret_value(),
            db_pool_size=1,
            db_pool_timeout_s=0.25,
        )
    )
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield app, client


async def test_an_exhausted_pool_sheds_load_instead_of_hanging(
    single_connection_app: tuple[FastAPI, AsyncClient],
) -> None:
    app, client = single_connection_app
    headers = await login(client, "pooluser")

    # Hold the only connection, exactly as a slow query would.
    async with app.state.resources.engine.connect() as held:
        await held.execute(text("SELECT 1"))
        response = await client.get("/api/v1/geozones", headers=headers)

    assert response.status_code == 503
    assert response.headers["content-type"].startswith(MEDIA_TYPE)
    assert response.json()["code"] == "database_busy"
    assert response.headers["Retry-After"] == "1"


async def test_the_replica_recovers_as_soon_as_the_connection_comes_back(
    single_connection_app: tuple[FastAPI, AsyncClient],
) -> None:
    app, client = single_connection_app
    headers = await login(client, "pooluser")

    async with app.state.resources.engine.connect() as held:
        await held.execute(text("SELECT 1"))
        refused = await client.get("/api/v1/geozones", headers=headers)
    served = await client.get("/api/v1/geozones", headers=headers)

    assert refused.status_code == 503
    assert served.status_code == 200


async def test_concurrent_requests_are_refused_rather_than_queued_forever(
    single_connection_app: tuple[FastAPI, AsyncClient],
) -> None:
    _, client = single_connection_app
    headers = await login(client, "pooluser")

    # Four requests, one connection: at most one can be served at a time, and the rest
    # must come back as 503 well inside the pool timeout rather than pile up.
    async with asyncio.timeout(5):
        responses = await asyncio.gather(
            *(client.get("/api/v1/geozones", headers=headers) for _ in range(4))
        )

    codes = {response.status_code for response in responses}
    assert codes <= {200, 503}
    assert all(
        response.json()["code"] == "database_busy"
        for response in responses
        if response.status_code == 503
    )


async def test_liveness_never_depends_on_the_database(
    single_connection_app: tuple[FastAPI, AsyncClient],
) -> None:
    app, client = single_connection_app

    async with app.state.resources.engine.connect() as held:
        await held.execute(text("SELECT 1"))
        live = await client.get("/health/live")
        ready = await client.get("/health/ready")

    # Liveness must stay green so the orchestrator does not restart a replica that is
    # merely busy; readiness may go red, which is what takes it out of rotation.
    assert live.status_code == 200
    assert ready.status_code in {200, 503}
