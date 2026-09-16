"""Application factory: wires resources, middleware, routers and error handling."""

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from geotrack.api.problems import install_problem_handlers
from geotrack.api.resources import AppResources
from geotrack.api.routes import (
    alerts,
    auth,
    devices,
    geozones,
    health,
    ingest,
    ws_client,
    ws_ingest,
)
from geotrack.db.engine import create_engine, create_session_factory
from geotrack.ingest.backlog import BacklogMonitor
from geotrack.ingest.service import IngestService
from geotrack.ingest.shards import verify_shard_count
from geotrack.logging import configure_logging
from geotrack.messaging.redis import close_redis, create_redis
from geotrack.observability.loop_lag import EventLoopLagMonitor
from geotrack.observability.metrics import (
    bind_pool_metrics,
    http_request_duration_seconds,
    http_requests_total,
    render_metrics,
)
from geotrack.realtime.gateway import Gateway
from geotrack.settings import Settings, get_settings

logger = structlog.get_logger(__name__)


class MetricsMiddleware:
    """Plain ASGI middleware: no per-request task or thread, unlike BaseHTTPMiddleware."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        started = time.perf_counter()
        status_code = 500

        async def wrapped_send(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = int(message["status"])
            await send(message)

        try:
            await self._app(scope, receive, wrapped_send)
        finally:
            # The router stores the matched route on the scope; unmatched paths are
            # reported as "unmatched" so a scan cannot explode the metric cardinality.
            route = scope.get("route")
            path = getattr(route, "path", "unmatched")
            method = scope.get("method", "GET")
            http_requests_total.labels(method, path, str(status_code)).inc()
            http_request_duration_seconds.labels(method, path).observe(
                time.perf_counter() - started
            )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    configure_logging(settings.service_name, level=settings.log_level, fmt=settings.log_format)

    # Before anything is opened: a replica that disagrees with the queue's partitioning
    # would accept reports and route them where nothing is listening.
    redis_url = settings.redis_url.get_secret_value()
    await verify_shard_count(redis_url, shards=settings.ingest_shards)

    engine = create_engine(
        settings, pool_size=settings.db_pool_size, application_name=f"{settings.service_name}-api"
    )
    bind_pool_metrics(engine, pool_size=settings.db_pool_size)
    session_factory = create_session_factory(engine)
    redis = create_redis(redis_url, purpose="commands")
    redis_pubsub = create_redis(redis_url, purpose="pubsub", max_connections=8)
    backlog = BacklogMonitor(
        redis,
        shards=settings.ingest_shards,
        high=settings.ingest_backlog_high,
        low=settings.ingest_backlog_low,
        poll_ms=settings.ingest_backlog_poll_ms,
    )
    resources = AppResources(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
        redis=redis,
        redis_pubsub=redis_pubsub,
        gateway=Gateway(
            settings, redis=redis, redis_pubsub=redis_pubsub, session_factory=session_factory
        ),
        loop_lag=EventLoopLagMonitor(),
        backlog=backlog,
        ingest=IngestService(redis, backlog, settings=settings),
    )
    app.state.resources = resources
    resources.loop_lag.start()
    await backlog.start()
    await resources.gateway.start()
    logger.info("api started", shards=settings.ingest_shards, pool_size=settings.db_pool_size)

    try:
        yield
    finally:
        await resources.gateway.stop()
        await backlog.stop()
        await resources.loop_lag.stop()
        await close_redis(resources.redis_pubsub)
        await close_redis(resources.redis)
        await engine.dispose()
        logger.info("api stopped")


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or get_settings()
    app = FastAPI(
        title="GeoTrack",
        version="1.0.0",
        summary="Real-time geo-tracking and geofence alerting",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url=None,
        openapi_url="/openapi.json",
    )
    app.state.settings = resolved
    app.add_middleware(MetricsMiddleware)
    install_problem_handlers(app)
    for router in (
        health.router,
        auth.router,
        geozones.router,
        alerts.router,
        devices.router,
        ingest.router,
        ws_client.router,
        ws_ingest.router,
    ):
        app.include_router(router)

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        payload, content_type = render_metrics()
        return Response(payload, media_type=content_type)

    return app
