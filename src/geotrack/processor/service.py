"""Composition root of a processor replica.

A replica owns no shard permanently. It heartbeats into a membership set, works out
its fair share of the shards, leases what it can and hands back what it should not
hold. Everything else — consumers, renewals, partition maintenance — follows from that
one decision, which is what lets the tier be scaled by changing a replica count.

The one rule the control plane lives by: it never waits for a consumer. Stopping a
consumer means waiting for whatever batch it is applying, and a lease expires in a
fraction of the time a batch is allowed to take. A shard being handed back therefore
winds down on a task of its own, while the loops that keep the *other* leases alive
carry on. The module also carries the replica's HTTP surface, because the ASGI server
is what turns SIGTERM into the shutdown that hands those leases back.
"""

import asyncio
import os
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import structlog
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from geotrack.clock import now_ms
from geotrack.db.engine import create_engine, create_session_factory
from geotrack.ids import new_uuid
from geotrack.logging import configure_logging
from geotrack.messaging.keys import SHARDS_META_KEY
from geotrack.messaging.redis import close_redis, create_redis
from geotrack.observability.loop_lag import EventLoopLagMonitor
from geotrack.observability.metrics import (
    bind_pool_metrics,
    processor_shard_backlog,
    processor_shards_owned,
    render_metrics,
)
from geotrack.processor.batch import BatchProcessor
from geotrack.processor.consumer import ShardConsumer
from geotrack.processor.leases import ShardLeaseManager
from geotrack.processor.maintenance import INTERVAL_S, PartitionMaintenance
from geotrack.processor.publisher import ResultPublisher
from geotrack.settings import Settings, get_settings

logger = structlog.get_logger(__name__)

CONTROL_INTERVAL_S = 2.0
# A batch has to finish before its shard can move on; past this the task is cancelled
# and its entries stay pending for whoever takes the shard next.
CONSUMER_STOP_TIMEOUT_S = 30.0
CANCEL_GRACE_S = 5.0
# Shutting down runs on somebody else's clock. An orchestrator that sends SIGTERM and
# waits ten seconds before killing the container must still get the leases back, so
# shutdown gives up on an in-flight batch long before that: the entries stay pending and
# the next owner of the shard finishes them.
SHUTDOWN_TIMEOUT_S = 8.0
READINESS_TIMEOUT_S = 3.0


class ShardCountMismatchError(RuntimeError):
    """The cluster was started with a different shard count than the one in Redis.

    Devices are mapped to shards by hash, so changing the count would send a device's
    reports to a shard that another consumer is already reading: two writers for one
    device, and no ordering. Refusing to start is the only safe answer.
    """


@dataclass(slots=True)
class _Running:
    consumer: ShardConsumer
    task: asyncio.Task[None]


@dataclass(slots=True)
class _Draining:
    """A shard this replica has let go of, still finishing its last batch."""

    running: _Running
    task: asyncio.Task[None]
    # Whether the lease is ours to give back. It is not when another replica took it.
    release: bool


@dataclass(frozen=True, slots=True)
class _Resources:
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    streams: Redis
    commands: Redis
    lease: ShardLeaseManager
    maintenance: PartitionMaintenance
    batch_processor: BatchProcessor
    publisher: ResultPublisher


def _default_instance_id() -> str:
    # Host and pid make a log line traceable back to a container; the suffix keeps two
    # replicas distinct even when a container id is reused.
    return f"{socket.gethostname()}-{os.getpid()}-{new_uuid().hex[-6:]}"


class ProcessorService:
    def __init__(self, settings: Settings, *, instance_id: str | None = None) -> None:
        self._settings = settings
        self._instance_id = instance_id or _default_instance_id()
        self._resources: _Resources | None = None
        self._loop_lag = EventLoopLagMonitor()
        self._consumers: dict[int, _Running] = {}
        self._draining: dict[int, _Draining] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._last_heartbeat_ms = 0

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def owned_shards(self) -> set[int]:
        return set(self._consumers)

    def is_consuming(self, shard: int) -> bool:
        """True while this replica holds the shard and its consumer is still running."""
        running = self._consumers.get(shard)
        return running is not None and not running.task.done()

    @property
    def _live(self) -> _Resources:
        if self._resources is None:
            raise RuntimeError("the processor service is not running")
        return self._resources

    async def start(self) -> None:
        settings = self._settings
        # One connection per shard that can be applying a batch, plus headroom for the
        # maintenance round and the readiness probe.
        pool_size = settings.ingest_shards + 2
        engine = create_engine(
            settings, pool_size=pool_size, application_name=f"{settings.service_name}-processor"
        )
        bind_pool_metrics(engine, pool_size=pool_size)
        session_factory = create_session_factory(engine)

        url = settings.redis_url.get_secret_value()
        streams = create_redis(url, purpose="streams", max_connections=settings.ingest_shards + 4)
        commands = create_redis(url, purpose="commands")

        self._resources = _Resources(
            engine=engine,
            session_factory=session_factory,
            streams=streams,
            commands=commands,
            lease=ShardLeaseManager(
                commands,
                shards=settings.ingest_shards,
                ttl_ms=settings.processor_lease_ttl_ms,
                instance_id=self._instance_id,
                membership_ttl_ms=max(
                    settings.processor_lease_ttl_ms, int(CONTROL_INTERVAL_S * 4_000)
                ),
            ),
            maintenance=PartitionMaintenance(
                session_factory,
                commands,
                retention_days=settings.history_retention_days,
                instance_id=self._instance_id,
            ),
            batch_processor=BatchProcessor(session_factory, settings=settings),
            publisher=ResultPublisher(commands),
        )

        # Starting is the riskiest part of a replica's life and the part with no second
        # chance: an ASGI lifespan that raises before its yield never reaches the shutdown
        # half that would have cleaned up. So everything past the first allocation shares
        # one exit, and it is the same one a healthy replica uses.
        try:
            await self._verify_shard_count(commands)
            # Before anything is consumed: without a partition for today every batch
            # would fail on the history insert, and no retry could fix it.
            await self._live.maintenance.run_once()

            self._loop_lag.start()
            self._tasks = [
                asyncio.create_task(self._control_loop(), name="processor-control"),
                asyncio.create_task(self._renew_loop(), name="processor-renew"),
                asyncio.create_task(self._maintenance_loop(), name="processor-maintenance"),
            ]
        except BaseException:
            await self.stop()
            raise

        logger.info(
            "processor started",
            instance=self._instance_id,
            shards=settings.ingest_shards,
            pool_size=pool_size,
        )

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        # return_exceptions keeps the cancellations of the loops from surfacing here.
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        held = set(self._consumers) | set(self._draining)
        for shard in list(self._consumers):
            self._begin_drain(shard, release=True)
        await self._await_drains()
        processor_shards_owned.set(0)
        await self._loop_lag.stop()

        resources = self._resources
        if resources is None:
            return
        # Whatever a drain was cut short before releasing. The lease is compare-and-delete,
        # so releasing one twice — or one that was lost to another replica — costs nothing,
        # and another replica must be able to take the shard the moment this one is gone.
        for shard in held:
            await self._release_quietly(shard)
        self._resources = None
        try:
            await resources.lease.deregister()
        except RedisError as exc:
            logger.warning("could not deregister", error=str(exc))
        await close_redis(resources.streams)
        await close_redis(resources.commands)
        await resources.engine.dispose()
        logger.info("processor stopped", instance=self._instance_id)

    async def is_ready(self) -> bool:
        """Ready when both dependencies answer and the control loop is still beating."""
        if self._resources is None:
            return False
        if now_ms() - self._last_heartbeat_ms > self._settings.processor_lease_ttl_ms:
            return False

        async def check_database() -> None:
            async with self._live.engine.connect() as conn:
                await conn.execute(text("SELECT 1"))

        try:
            async with asyncio.timeout(READINESS_TIMEOUT_S):
                await asyncio.gather(check_database(), self._live.commands.ping())
        except Exception as exc:
            logger.warning("readiness check failed", error=str(exc))
            return False
        return True

    async def _verify_shard_count(self, commands: Redis) -> None:
        shards = self._settings.ingest_shards
        await commands.set(SHARDS_META_KEY, shards, nx=True)
        recorded = await commands.get(SHARDS_META_KEY)
        if recorded is not None and int(recorded) != shards:
            raise ShardCountMismatchError(
                f"this replica is configured for {shards} ingest shards, "
                f"but the cluster is running {int(recorded)}"
            )

    async def _control_loop(self) -> None:
        while True:
            try:
                await self.reconcile_shards()
            except RedisError as exc:
                logger.warning("shard reconciliation failed", error=str(exc))
            await asyncio.sleep(CONTROL_INTERVAL_S)

    async def reconcile_shards(self) -> None:
        """One round of the ownership decision: heartbeat, hand back, pick up."""
        lease = self._live.lease
        self._reap_finished_consumers()
        plan = await lease.rebalance(self.owned_shards)
        self._last_heartbeat_ms = now_ms()

        for shard in plan.release:
            self._begin_drain(shard, release=True)
        for shard in plan.candidates:
            if len(self._consumers) >= plan.target:
                break
            # A shard that is still winding down belongs to this replica until its
            # consumer has finished: starting a second one would put two readers behind
            # one consumer name.
            if shard in self._draining:
                continue
            if await lease.acquire(shard):
                self._start_consumer(shard)
        processor_shards_owned.set(len(self._consumers))

    def _reap_finished_consumers(self) -> None:
        """Give back any shard whose consumer has stopped without being asked to.

        The consumer loop is written to survive its dependencies going away, so this
        should never fire; when it does, releasing the lease lets another replica take
        the shard instead of leaving it owned by a reader that no longer reads.
        """
        for shard in [shard for shard, running in self._consumers.items() if running.task.done()]:
            logger.error("consumer ended unexpectedly", shard=shard)
            self._begin_drain(shard, release=True)

    async def _renew_loop(self) -> None:
        # A third of the lease: two renewals may be lost before the lease can expire.
        interval_s = self._settings.processor_lease_ttl_ms / 3_000
        while True:
            await asyncio.sleep(interval_s)
            try:
                await self.renew_leases()
            except RedisError as exc:
                logger.warning("lease renewal failed", error=str(exc))

    async def renew_leases(self) -> None:
        # Shards that are winding down are renewed too. This replica has not let go of
        # one until its consumer has finished, and a lease that expired underneath a
        # batch in flight would hand the shard over while it is still being written.
        shards = list(self._consumers) + [
            shard for shard, draining in self._draining.items() if draining.release
        ]
        if not shards:
            return
        lease = self._live.lease
        held = await asyncio.gather(*(lease.renew(shard) for shard in shards))
        lost = [shard for shard, still_held in zip(shards, held, strict=True) if not still_held]
        if not lost:
            return
        logger.warning("shard leases lost", shards=lost)
        # Someone else already owns these; stop reading them. The winding down happens
        # elsewhere, because this task is also what keeps the remaining leases alive.
        for shard in lost:
            draining = self._draining.get(shard)
            if draining is not None:
                # Already winding down, and now no longer ours to hand back.
                draining.release = False
            else:
                self._begin_drain(shard, release=False)
        processor_shards_owned.set(len(self._consumers))

    async def _maintenance_loop(self) -> None:
        while True:
            await asyncio.sleep(INTERVAL_S)
            try:
                await self._live.maintenance.run_once()
            except Exception as exc:
                logger.warning("partition maintenance failed", error=str(exc))

    def _start_consumer(self, shard: int) -> None:
        resources = self._live
        consumer = ShardConsumer(
            shard,
            redis=resources.streams,
            batch_processor=resources.batch_processor,
            publisher=resources.publisher,
            settings=self._settings,
            lease=resources.lease,
        )
        task = asyncio.create_task(consumer.run(), name=f"shard-consumer-{shard}")
        self._consumers[shard] = _Running(consumer=consumer, task=task)
        logger.info("shard acquired", shard=shard, instance=self._instance_id)

    def _begin_drain(self, shard: int, *, release: bool) -> None:
        """Let go of a shard now and let its consumer finish in the background."""
        running = self._consumers.pop(shard, None)
        if running is None:
            return
        running.consumer.request_stop()
        # The task cannot run before this statement finishes, so it will find its own
        # record in place, and with whatever `release` has become by the time it ends.
        self._draining[shard] = _Draining(
            running=running,
            task=asyncio.create_task(self._drain(shard, running), name=f"shard-drain-{shard}"),
            release=release,
        )

    async def _drain(self, shard: int, running: _Running) -> None:
        try:
            # asyncio.wait rather than awaiting the task: awaiting a task that was
            # cancelled re-raises its CancelledError here, which would look like this
            # coroutine being cancelled.
            await asyncio.wait({running.task}, timeout=CONSUMER_STOP_TIMEOUT_S)
            if not running.task.done():
                logger.warning("consumer did not stop in time, cancelling", shard=shard)
                running.task.cancel()
                await asyncio.wait({running.task}, timeout=CANCEL_GRACE_S)
            elif not running.task.cancelled() and running.task.exception() is not None:
                logger.error("consumer failed", shard=shard, error=str(running.task.exception()))
        finally:
            # A gauge left at its last reading would keep claiming a backlog on a shard
            # this replica no longer reads.
            processor_shard_backlog.labels(str(shard)).set(0)
            draining = self._draining.pop(shard, None)
            release = draining is not None and draining.release
            if release:
                await self._release_quietly(shard)
            logger.info(
                "shard released" if release else "shard lease lost",
                shard=shard,
                instance=self._instance_id,
            )

    async def _await_drains(self) -> None:
        """Wait out the shards that are winding down, within the shutdown budget."""
        drains = [draining.task for draining in self._draining.values()]
        if not drains:
            return
        await asyncio.wait(drains, timeout=SHUTDOWN_TIMEOUT_S)
        for shard, draining in list(self._draining.items()):
            logger.warning("shard still draining at shutdown", shard=shard)
            draining.running.task.cancel()
            draining.task.cancel()
            self._draining.pop(shard, None)
        await asyncio.gather(*drains, return_exceptions=True)

    async def _release_quietly(self, shard: int) -> None:
        if self._resources is None:
            return
        try:
            await self._resources.lease.release(shard)
        except RedisError as exc:
            logger.warning("could not release shard", shard=shard, error=str(exc))


async def live(_: Request) -> Response:
    """Liveness: the process is up and its event loop is turning."""
    return JSONResponse({"status": "ok"})


async def ready(request: Request) -> Response:
    service: ProcessorService = request.app.state.service
    if not await service.is_ready():
        return JSONResponse({"status": "not_ready"}, status_code=503, headers={"Retry-After": "1"})
    return JSONResponse({"status": "ok", "shards": sorted(service.owned_shards)})


async def metrics(_: Request) -> Response:
    payload, content_type = render_metrics()
    return Response(payload, media_type=content_type)


def create_processor_app(settings: Settings | None = None) -> Starlette:
    """The replica's own HTTP surface: probes and metrics, nothing else.

    Serving it through the ASGI server also gives the process its lifecycle for free —
    uvicorn turns SIGTERM into a lifespan shutdown, which is exactly the point at which
    consumers should finish their batch and leases should be handed back.
    """
    resolved = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        configure_logging(
            f"{resolved.service_name}-processor", level=resolved.log_level, fmt=resolved.log_format
        )
        service = ProcessorService(resolved)
        app.state.service = service
        await service.start()
        try:
            yield
        finally:
            await service.stop()

    return Starlette(
        lifespan=lifespan,
        routes=[
            Route("/health/live", live),
            Route("/health/ready", ready),
            Route("/metrics", metrics),
        ],
    )
