from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient

from geotrack.api.app import create_app
from geotrack.settings import Settings


@pytest.fixture
async def client(settings: Settings) -> AsyncIterator[AsyncClient]:
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client


async def test_liveness_and_readiness(client: AsyncClient) -> None:
    live = await client.get("/health/live")
    ready = await client.get("/health/ready")

    assert live.status_code == 200
    assert ready.status_code == 200
    assert ready.json() == {"status": "ok"}


async def test_metrics_are_served_without_a_redirect(client: AsyncClient) -> None:
    response = await client.get("/metrics")

    assert response.status_code == 200
    assert "geotrack_db_pool_size" in response.text


async def test_unknown_routes_return_problem_json(client: AsyncClient) -> None:
    response = await client.get("/api/v1/does-not-exist")

    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json()["code"] == "not_found"


async def test_openapi_document_is_generated(client: AsyncClient) -> None:
    response = await client.get("/openapi.json")

    assert response.status_code == 200
    assert response.json()["info"]["title"] == "GeoTrack"
