import asyncio

from fastapi import APIRouter
from fastapi.responses import Response
from sqlalchemy import text

from geotrack.api.deps import Resources
from geotrack.api.problems import problem_response
from geotrack.schemas.common import Schema

router = APIRouter(tags=["health"])


class Health(Schema):
    status: str


@router.get("/health/live", response_model=Health)
async def live() -> Health:
    """Liveness: the process is up and its event loop is turning."""
    return Health(status="ok")


@router.get("/health/ready", response_model=Health, responses={503: {"description": "Not ready"}})
async def ready(resources: Resources) -> Health | Response:
    """Readiness: dependencies answer, so this replica can take traffic."""

    async def check_database() -> None:
        async with resources.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

    async def check_redis() -> None:
        await resources.redis.ping()

    try:
        async with asyncio.timeout(3):
            await asyncio.gather(check_database(), check_redis())
    except Exception as exc:
        return problem_response(
            status=503,
            title="Service unavailable",
            code="not_ready",
            detail=f"A dependency is not reachable: {exc}",
            headers={"Retry-After": "1"},
        )

    # A replica whose fan-out reader is gone still answers HTTP while every websocket
    # on it has gone quiet. That is exactly the state a load balancer must route away
    # from, so readiness reports it rather than hiding it behind a green process.
    if not resources.gateway.delivering:
        return problem_response(
            status=503,
            title="Service unavailable",
            code="not_ready",
            detail="The realtime fan-out reader is not running on this replica.",
            headers={"Retry-After": "1"},
        )
    return Health(status="ok")
