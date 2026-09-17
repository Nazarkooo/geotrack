"""One connected dashboard client: what it should receive next, and how it is written.

The rule that shapes this module: broadcasting must never await a socket. Producers
(the tick loop, the pub/sub bridge) only leave work behind; a single task per
connection does the writing. A client that reads slowly therefore costs bounded
memory — one position frame plus a bounded control queue — instead of stalling the
whole gateway or growing an unbounded backlog.
"""

import asyncio
import time
from collections import deque
from typing import Protocol
from uuid import UUID

import orjson
import structlog
from starlette.websockets import WebSocketDisconnect

from geotrack.clock import now_ms
from geotrack.geo import BBox
from geotrack.observability.metrics import (
    ws_messages_sent_total,
    ws_position_frames_dropped_total,
    ws_send_seconds,
    ws_slow_consumer_disconnects_total,
)
from geotrack.realtime.grid import CellRect, covers, rects_for
from geotrack.realtime.hub import PositionHub, TickDelta
from geotrack.realtime.protocol import CLOSE_SLOW_CONSUMER, SessionInfo, positions_frame
from geotrack.settings import Settings

logger = structlog.get_logger(__name__)

# A close frame is written to a peer we already suspect is not reading: never wait long.
_CLOSE_TIMEOUT_S = 1.0

_FRAME_TYPE_PREFIX = b'{"type":"'
# Only known types become a metric label, so a stray payload cannot explode cardinality.
_FRAME_TYPES = frozenset(
    {"hello", "positions", "alert", "zone", "sessions", "stats", "pong", "error", "ack", "throttle"}
)


class ClientSocket(Protocol):
    """The part of ``starlette.websockets.WebSocket`` that a connection writes to."""

    async def send_bytes(self, data: bytes) -> None: ...

    async def send_text(self, data: str) -> None: ...

    async def close(self, code: int = 1000, reason: str | None = None) -> None: ...


def frame_type(frame: bytes) -> str:
    """The frame's ``type``, read off the wire bytes.

    Every frame this service builds starts with the same key, so the common path is a
    ten-byte comparison rather than a JSON parse per frame per connection. Anything
    shaped differently is parsed properly instead of being written off as unknown.
    """
    if frame.startswith(_FRAME_TYPE_PREFIX):
        end = frame.find(b'"', len(_FRAME_TYPE_PREFIX))
        if end > 0:
            kind = frame[len(_FRAME_TYPE_PREFIX) : end].decode(errors="replace")
            if kind in _FRAME_TYPES:
                return kind
    return _parsed_frame_type(frame)


def _parsed_frame_type(frame: bytes) -> str:
    try:
        payload = orjson.loads(frame)
    except orjson.JSONDecodeError:
        return "unknown"
    kind = payload.get("type") if isinstance(payload, dict) else None
    return kind if kind in _FRAME_TYPES else "unknown"


class ClientConnection:
    def __init__(
        self,
        socket: ClientSocket,
        *,
        user_id: UUID,
        session: SessionInfo,
        hub: PositionHub,
        settings: Settings,
    ) -> None:
        self._socket = socket
        self.user_id = user_id
        self.session = session
        self.close_code = 1000
        self.close_reason = ""
        self._hub = hub
        self._cell_size_deg = settings.ws_grid_cell_deg
        self._send_timeout_s = settings.ws_send_timeout_s
        self._control_max = settings.ws_control_queue_max
        self._control: deque[bytes] = deque()
        self._wake = asyncio.Event()
        self._viewport: tuple[CellRect, ...] | None = None
        self._viewport_cells = 0
        self._pending_delta: bytes | None = None
        self._needs_snapshot = False
        self._closing = False
        self._writable = True

    @property
    def session_id(self) -> UUID:
        return self.session.id

    @property
    def closing(self) -> bool:
        return self._closing

    @property
    def wants_positions(self) -> bool:
        """Whether this client has a window for a position frame to land in."""
        return self._viewport is not None and not self._closing

    def set_viewport(self, bbox: BBox) -> None:
        """Move the client's window; the next frame is a snapshot of the new one."""
        self._viewport = rects_for(bbox, self._cell_size_deg)
        self._viewport_cells = sum(
            (rect.max_x - rect.min_x + 1) * (rect.max_y - rect.min_y + 1) for rect in self._viewport
        )
        # A delta built for the previous window is worthless now.
        self._pending_delta = None
        self._needs_snapshot = True
        self._wake.set()

    def offer_positions(self, delta: TickDelta) -> None:
        """Called by the tick loop for every connection; must never block or await."""
        viewport = self._viewport
        if self._closing or viewport is None or self._needs_snapshot:
            return

        chunks = self._visible_chunks(delta, viewport)
        removed = [
            departure.device_id
            for cell, departures in delta.departures.items()
            if covers(viewport, cell)
            for departure in departures
            if departure.moved_to is None or not covers(viewport, departure.moved_to)
        ]
        if not chunks and not removed:
            return

        if self._pending_delta is None:
            self._pending_delta = positions_frame(
                full=False, t_ms=delta.t_ms, item_chunks=chunks, removed=removed
            )
        else:
            # The client has not drained the previous frame. Queueing deltas would only
            # grow the lag, so replace the queue with the current truth.
            self._pending_delta = None
            self._needs_snapshot = True
            ws_position_frames_dropped_total.inc()
        self._wake.set()

    def _visible_chunks(self, delta: TickDelta, viewport: tuple[CellRect, ...]) -> list[bytes]:
        """Pick whichever side of the join is smaller.

        A dashboard usually watches a few hundred cells while a busy tick touches
        thousands, so looking the window's cells up in the delta beats testing every
        changed cell against the window. Zoomed all the way out it is the other way
        round, and this measurably dominates the tick at a few hundred viewers.
        """
        if self._viewport_cells <= len(delta.chunks):
            return [
                chunk
                for rect in viewport
                for x in range(rect.min_x, rect.max_x + 1)
                for y in range(rect.min_y, rect.max_y + 1)
                if (chunk := delta.chunks.get((x, y))) is not None
            ]
        return [chunk for cell, chunk in delta.chunks.items() if covers(viewport, cell)]

    def send_control(self, frame: bytes) -> None:
        """Queue an alert, zone, session, stats or error frame. Never blocks."""
        if self._closing:
            return
        if len(self._control) >= self._control_max:
            ws_slow_consumer_disconnects_total.inc()
            logger.info(
                "closing slow consumer", session_id=str(self.session_id), reason="control overflow"
            )
            self.request_close(CLOSE_SLOW_CONSUMER, "client is not keeping up")
            return
        self._control.append(frame)
        self._wake.set()

    def request_close(self, code: int, reason: str = "") -> None:
        """Ask the sender to stop; the socket itself is closed by ``aclose``.

        Pending frames are dropped: the close code and reason are what the client
        needs, and a peer that made us close is not going to read a backlog.
        """
        if self._closing:
            return
        self._closing = True
        self.close_code = code
        self.close_reason = reason
        self._control.clear()
        self._pending_delta = None
        self._needs_snapshot = False
        self._wake.set()

    async def sender(self) -> None:
        """The only task that writes to this socket."""
        while not self._closing:
            await self._wake.wait()
            self._wake.clear()
            while not self._closing:
                frame = self._next_frame()
                if frame is None:
                    break
                if not await self._write(frame):
                    return

    async def aclose(self) -> None:
        """Close the socket, giving up quickly if the peer stopped reading."""
        self._closing = True
        if not self._writable:
            return
        self._writable = False
        try:
            async with asyncio.timeout(_CLOSE_TIMEOUT_S):
                await self._socket.close(self.close_code, self.close_reason)
        except (TimeoutError, WebSocketDisconnect, RuntimeError, OSError) as exc:
            logger.debug(
                "close frame not delivered", session_id=str(self.session_id), error=str(exc)
            )

    def _next_frame(self) -> bytes | None:
        if self._control:
            return self._control.popleft()
        if self._needs_snapshot and self._viewport is not None:
            self._needs_snapshot = False
            return positions_frame(
                full=True, t_ms=now_ms(), item_chunks=self._hub.snapshot_chunks(self._viewport)
            )
        if self._pending_delta is not None:
            frame, self._pending_delta = self._pending_delta, None
            return frame
        return None

    async def _write(self, frame: bytes) -> bool:
        """Write one frame; ``False`` means this connection is finished."""
        if not self._writable:
            return False
        kind = frame_type(frame)
        # Position frames are pre-serialised bytes shared by every client that can see
        # them, so they go out as binary rather than being decoded back to text once
        # per connection. The rest are ordinary JSON text.
        text: str | None = None
        if kind != "positions":
            try:
                text = frame.decode()
            except UnicodeDecodeError as exc:
                # Relayed frames come from another process. One that is not text costs
                # this client that frame, never its connection.
                logger.warning(
                    "dropping a frame that is not valid utf-8",
                    session_id=str(self.session_id),
                    error=str(exc),
                )
                return True
        started = time.perf_counter()
        try:
            async with asyncio.timeout(self._send_timeout_s):
                if text is None:
                    await self._socket.send_bytes(frame)
                else:
                    await self._socket.send_text(text)
        except TimeoutError:
            # The transport's write buffer is full and the peer is not draining it.
            ws_slow_consumer_disconnects_total.inc()
            logger.info(
                "closing slow consumer", session_id=str(self.session_id), reason="send timeout"
            )
            self._writable = False
            self.request_close(CLOSE_SLOW_CONSUMER, "write timed out")
            return False
        except (WebSocketDisconnect, RuntimeError, OSError) as exc:
            logger.debug("peer went away", session_id=str(self.session_id), error=str(exc))
            self._writable = False
            self.request_close(1000, "peer disconnected")
            return False
        finally:
            ws_send_seconds.observe(time.perf_counter() - started)
        ws_messages_sent_total.labels(kind).inc()
        return True
