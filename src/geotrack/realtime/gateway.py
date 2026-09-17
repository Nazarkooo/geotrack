"""Composition root of the realtime gateway.

It owns the hub, the connection registry, the pub/sub bridge and the cross-replica
session directory, and it runs the three loops that drive them: position ticks,
periodic stats and session heartbeats. Everything a websocket route needs is behind
``serve``; everything a REST handler needs is behind ``publish_user_frame``.

No database connection is held for the lifetime of a socket: the only query the
gateway makes is the one-off warm start, and its session is released immediately.
"""

import asyncio
import os
import socket
from collections import deque
from collections.abc import Iterator, Sequence
from contextlib import contextmanager, suppress
from uuid import UUID

import structlog
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.websockets import WebSocket, WebSocketDisconnect

from geotrack.clock import now_ms, utc_now
from geotrack.ids import new_uuid
from geotrack.messaging.keys import user_channel
from geotrack.observability.metrics import ingest_backlog, ws_messages_sent_total
from geotrack.realtime.bridge import RedisBridge
from geotrack.realtime.connection import ClientConnection
from geotrack.realtime.hub import PositionHub
from geotrack.realtime.protocol import (
    CLOSE_PROTOCOL_ERROR,
    CLOSE_TOO_MANY_SESSIONS,
    PingMessage,
    ProtocolError,
    SessionInfo,
    ViewportMessage,
    error_frame,
    hello_frame,
    parse_client_message,
    pong_frame,
    sessions_frame,
    stats_frame,
)
from geotrack.realtime.registry import ConnectionRegistry
from geotrack.realtime.sessions import SessionDirectory, describe_client
from geotrack.schemas.auth import UserOut
from geotrack.settings import Settings

logger = structlog.get_logger(__name__)

# The subprotocol a browser offers alongside "bearer.<jwt>"; browsers cannot set an
# Authorization header on a websocket handshake, and a token in the query string ends
# up in access logs.
WS_SUBPROTOCOL = "geotrack.v1"
_BEARER_PREFIX = "bearer."

_STATS_INTERVAL_S = 2.0
_SESSIONS_INTERVAL_S = 10.0
_SWEEP_INTERVAL_S = 5.0
_HELLO_TIMEOUT_S = 5.0
# Long enough for a cold replica to read a full fleet, short enough that a database
# nobody can reach does not decide when this replica starts answering health checks.
_WARM_START_TIMEOUT_S = 15.0
# A misbehaving client gets told what is wrong, but not forever.
_MALFORMED_BUDGET = 20
_MALFORMED_WINDOW_S = 60.0
_MAX_CLIENT_MESSAGE_BYTES = 16 * 1024


class Gateway:
    def __init__(
        self,
        settings: Settings,
        *,
        redis: Redis,
        redis_pubsub: Redis,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._settings = settings
        self._redis = redis
        self._session_factory = session_factory
        self.instance = f"{socket.gethostname()}-{os.getpid()}"
        self._hub = PositionHub(
            cell_size_deg=settings.ws_grid_cell_deg, stale_after_s=settings.ws_device_stale_s
        )
        self._registry = ConnectionRegistry(
            on_first_user_connection=self._subscribe_user,
            on_last_user_disconnect=self._unsubscribe_user,
        )
        self._bridge = RedisBridge(
            redis_pubsub, on_positions=self._hub.apply, on_user_frame=self._deliver
        )
        self._sessions = SessionDirectory(redis, instance=self.instance)
        self._tasks: list[asyncio.Task[None]] = []
        self._updates_mark = (0, 0.0)

    async def start(self) -> None:
        await self._warm_start()
        await self._bridge.start()
        self._updates_mark = (self._hub.updates, asyncio.get_running_loop().time())
        self._tasks = [
            asyncio.create_task(self._position_loop(), name="ws-positions"),
            asyncio.create_task(self._stats_loop(), name="ws-stats"),
            asyncio.create_task(self._sessions_loop(), name="ws-sessions"),
        ]

    async def _warm_start(self) -> None:
        """Fill the hub from the database, or start empty if it cannot be reached.

        A replica that refuses to boot during a database blip is strictly worse than
        one that serves live traffic with a cold hub: positions keep arriving from the
        processor, and each client's first viewport is answered from whatever the hub
        holds by then. asyncpg reports an unreachable server as a plain ``OSError``
        rather than anything SQLAlchemy wraps, and a blackholed one as nothing at all
        until its own minute-long connect timeout — so both shapes are handled here
        (``TimeoutError`` is an ``OSError`` too).
        """
        try:
            async with asyncio.timeout(_WARM_START_TIMEOUT_S), self._session_factory() as session:
                devices = await self._hub.warm_start(session)
        except Exception as exc:  # best effort by design: any failure means a cold hub
            logger.warning(
                "gateway warm start skipped, serving with a cold hub",
                error=f"{type(exc).__name__}: {exc}",
                instance=self.instance,
            )
            return
        logger.info("gateway warm start", devices=devices, instance=self.instance)

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()
        for connection in self._registry.all():
            connection.request_close(1001, "server shutting down")
        await self._bridge.stop()

    @property
    def delivering(self) -> bool:
        """Whether this replica is still receiving fan-out from Redis."""
        return self._bridge.delivering

    async def publish_user_frame(self, user_id: UUID, frame: bytes) -> None:
        """Send a ready-made frame to every session of a user, on every replica."""
        await self._redis.publish(user_channel(user_id), frame)

    async def serve(self, websocket: WebSocket, *, user: UserOut, user_agent: str | None) -> None:
        """Run one accepted client socket to completion."""
        session = SessionInfo(
            id=new_uuid(), label=describe_client(user_agent), connected_at=utc_now()
        )
        connection = ClientConnection(
            websocket,
            user_id=user.id,
            session=session,
            hub=self._hub,
            settings=self._settings,
        )
        # Admission comes first, so a client that gets its hello is a client this
        # replica has really accepted, and simultaneous sockets cannot pass the cap.
        if not await self._registry.add(connection, limit=self._settings.ws_max_sessions_per_user):
            await websocket.close(CLOSE_TOO_MANY_SESSIONS, "too many sessions")
            return

        try:
            if not await self._session_joined(user.id, session):
                connection.request_close(CLOSE_TOO_MANY_SESSIONS, "too many sessions")
                return
            if not await self._greet(websocket, user, session):
                return
            logger.info("client connected", user=user.username, session_id=str(session.id))
            await self._pump(connection, websocket)
        finally:
            connection.request_close(connection.close_code, connection.close_reason)
            await self._registry.remove(connection)
            await connection.aclose()
            await self._session_left(user.id, session.id)
            logger.info(
                "client disconnected",
                user=user.username,
                session_id=str(session.id),
                code=connection.close_code,
            )

    async def _pump(self, connection: ClientConnection, websocket: WebSocket) -> None:
        """Run the reader and the writer until either of them finishes."""
        sender = asyncio.create_task(connection.sender(), name=f"ws-send-{connection.session_id}")
        receiver = asyncio.create_task(
            self._receive(connection, websocket), name=f"ws-recv-{connection.session_id}"
        )
        # Whichever side finishes first ends the connection: a closed socket stops the
        # reader, and a slow-consumer close stops the sender.
        _, pending = await asyncio.wait((sender, receiver), return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    async def _greet(self, websocket: WebSocket, user: UserOut, session: SessionInfo) -> bool:
        frame = hello_frame(
            session_id=session.id,
            user=user,
            tick_ms=self._settings.ws_tick_ms,
            server_t=now_ms(),
        )
        try:
            async with asyncio.timeout(_HELLO_TIMEOUT_S):
                await websocket.send_text(frame.decode())
        except (TimeoutError, WebSocketDisconnect, RuntimeError, OSError) as exc:
            logger.info("client left during the handshake", error=str(exc))
            return False
        ws_messages_sent_total.labels("hello").inc()
        return True

    async def _receive(self, connection: ClientConnection, websocket: WebSocket) -> None:
        violations: deque[float] = deque()
        loop = asyncio.get_running_loop()
        while True:
            try:
                message = await websocket.receive()
            except (WebSocketDisconnect, RuntimeError) as exc:
                logger.debug(
                    "receive loop ended", session_id=str(connection.session_id), error=str(exc)
                )
                return
            if message["type"] == "websocket.disconnect":
                return

            payload: bytes | None = message.get("bytes")
            if payload is None:
                text: str | None = message.get("text")
                if text is None:
                    continue
                # The limit is in bytes. Measuring the string would count characters
                # and let four times as much multibyte text through, and the parser
                # would have to encode it again anyway.
                payload = text.encode()
            if len(payload) > _MAX_CLIENT_MESSAGE_BYTES:
                if not self._reject(
                    connection,
                    violations,
                    loop.time(),
                    "message_too_large",
                    f"messages are limited to {_MAX_CLIENT_MESSAGE_BYTES} bytes",
                ):
                    return
                continue

            try:
                parsed = parse_client_message(payload)
            except ProtocolError as exc:
                if not self._reject(connection, violations, loop.time(), "bad_message", str(exc)):
                    return
                continue

            match parsed:
                case ViewportMessage(bbox=bbox):
                    # Spamming this is harmless: only one snapshot is ever pending.
                    connection.set_viewport(bbox)
                case PingMessage(t=sent_at):
                    connection.send_control(pong_frame(t=sent_at, server_t=now_ms()))

    def _reject(
        self,
        connection: ClientConnection,
        violations: deque[float],
        at: float,
        code: str,
        detail: str,
    ) -> bool:
        """Answer a bad message; ``False`` means the client used up its budget."""
        while violations and violations[0] < at - _MALFORMED_WINDOW_S:
            violations.popleft()
        violations.append(at)
        if len(violations) > _MALFORMED_BUDGET:
            connection.request_close(CLOSE_PROTOCOL_ERROR, "too many malformed messages")
            return False
        connection.send_control(error_frame(code, detail))
        return True

    async def _position_loop(self) -> None:
        loop = asyncio.get_running_loop()
        interval_s = self._settings.ws_tick_ms / 1_000
        sweep_every = max(1, round(_SWEEP_INTERVAL_S / interval_s))
        next_tick = loop.time()
        ticks = 0
        while True:
            # An absolute schedule, so a slow tick does not push every later one back.
            next_tick = max(next_tick + interval_s, loop.time())
            await asyncio.sleep(next_tick - loop.time())
            ticks += 1
            with _keep_running("position tick"):
                moment = now_ms()
                if ticks % sweep_every == 0:
                    self._hub.sweep(moment)
                fan_out(self._hub, self._registry.all(), t_ms=moment)

    async def _stats_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(_STATS_INTERVAL_S)
            with _keep_running("stats tick"):
                connections = self._registry.all()
                updates, now = self._hub.updates, loop.time()
                previous_updates, previous_at = self._updates_mark
                self._updates_mark = (updates, now)
                if not connections:
                    continue
                frame = stats_frame(
                    t_ms=now_ms(),
                    devices=self._hub.device_count,
                    updates_per_s=(updates - previous_updates) / max(now - previous_at, 1e-6),
                    connections=len(connections),
                    backlog=_backlog(),
                )
                for connection in connections:
                    connection.send_control(frame)

    async def _sessions_loop(self) -> None:
        while True:
            await asyncio.sleep(_SESSIONS_INTERVAL_S)
            for user_id, connections in self._by_user().items():
                with _keep_running("session heartbeat"):
                    await self._sessions.refresh(
                        user_id, [connection.session for connection in connections]
                    )
                    frame = sessions_frame(await self._sessions.list_for(user_id))
                    # Delivered locally rather than published: every replica refreshes
                    # its own clients, so nobody gets the same list once per replica.
                    for connection in connections:
                        connection.send_control(frame)

    def _by_user(self) -> dict[UUID, list[ClientConnection]]:
        grouped: dict[UUID, list[ClientConnection]] = {}
        for connection in self._registry.all():
            grouped.setdefault(connection.user_id, []).append(connection)
        return grouped

    async def _session_joined(self, user_id: UUID, session: SessionInfo) -> bool:
        """Announce a session; ``False`` means the user is over their cap fleet-wide.

        The cap is per user, but a registry only knows its own replica, so the
        directory is what makes it hold across all of them. Nothing extra is
        fetched for it: registering and listing are the round trips a connect already
        makes. The verdict is read from the shared list rather than from a local
        tally, so two replicas admitting the same user at once agree on which session
        is the surplus one instead of both backing off. A directory that cannot be
        reached falls back to the per-replica cap — losing precision is better than
        losing connections.
        """
        try:
            await self._sessions.register(user_id, session)
            sessions = await self._sessions.list_for(user_id)
            if _rank_of(session.id, sessions) >= self._settings.ws_max_sessions_per_user:
                logger.info(
                    "session refused, user is at their cap",
                    user=str(user_id),
                    sessions=len(sessions),
                )
                return False
            await self.publish_user_frame(user_id, sessions_frame(sessions))
        except RedisError as exc:
            # A directory hiccup must not cost the client its connection: the session
            # list is a convenience, and the next heartbeat repairs it.
            logger.warning("session not announced", error=str(exc))
        return True

    async def _session_left(self, user_id: UUID, session_id: UUID) -> None:
        try:
            await self._sessions.remove(user_id, session_id)
            await self._publish_sessions(user_id)
        except RedisError as exc:
            logger.warning("session removal not announced", error=str(exc))

    async def _publish_sessions(self, user_id: UUID) -> None:
        sessions = await self._sessions.list_for(user_id)
        await self.publish_user_frame(user_id, sessions_frame(sessions))

    def _deliver(self, user_id: UUID, frame: bytes) -> None:
        for connection in self._registry.for_user(user_id):
            connection.send_control(frame)

    async def _subscribe_user(self, user_id: UUID) -> None:
        await self._bridge.subscribe_user(user_id)

    async def _unsubscribe_user(self, user_id: UUID) -> None:
        await self._bridge.unsubscribe_user(user_id)


@contextmanager
def _keep_running(step: str) -> Iterator[None]:
    """Log and swallow a failed loop iteration: a background loop must not die.

    Cancellation is a BaseException and still propagates, so shutdown is unaffected.
    """
    try:
        yield
    except Exception:
        logger.exception("background step failed", step=step)


def fan_out(hub: PositionHub, connections: Sequence[ClientConnection], *, t_ms: int) -> None:
    """Hand one tick's changes to every client that has somewhere to put them.

    Serialising a tick costs the same whether one client is watching or none, so the
    hub is only drained once somebody has a viewport. A replica whose clients are all
    still on the login screen would otherwise spend milliseconds of CPU every second
    encoding frames nobody will read, and that cost grows with the fleet. Nothing is
    lost by discarding those ticks: a viewport is always answered with a full snapshot
    of whatever the hub holds by then.
    """
    watching = [connection for connection in connections if connection.wants_positions]
    if not watching:
        hub.discard_pending()
        return
    delta = hub.drain(t_ms=t_ms)
    if delta.is_empty:
        return
    for connection in watching:
        connection.offer_positions(delta)


def token_from_handshake(subprotocols: Sequence[str], access_token: str | None) -> str | None:
    """The bearer token a client offered, from its subprotocols or the query string."""
    for offered in subprotocols:
        if offered.startswith(_BEARER_PREFIX):
            return offered[len(_BEARER_PREFIX) :]
    return access_token


def _rank_of(session_id: UUID, sessions: Sequence[SessionInfo]) -> int:
    """How many of a user's sessions, fleet-wide, are older than this one.

    Ordering by ``(connected_at, id)`` is decided by the directory's own copy of every
    session, so replicas reading the same list rank them identically without talking
    to each other. A session the directory does not list yet ranks first: the list is
    the only evidence a connection is refused on, and a missing entry is not evidence.
    """
    mine = next((entry for entry in sessions if entry.id == session_id), None)
    if mine is None:
        return 0
    key = (mine.connected_at, mine.id)
    return sum(1 for entry in sessions if (entry.connected_at, entry.id) < key)


def _backlog() -> int:
    """Current ingest backlog, as the ingest monitor last measured it."""
    for metric in ingest_backlog.collect():
        for sample in metric.samples:
            return int(sample.value)
    return 0
