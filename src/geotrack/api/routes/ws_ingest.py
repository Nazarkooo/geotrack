"""Websocket ingestion for devices and device gateways.

The interesting part is what happens when the processors fall behind. Refusing to read
the socket would push back through TCP, which is the most honest backpressure there is —
but a websocket carries the protocol's own keepalive on that same stream. A handler that
stops reading also stops reading the pongs that prove the device is alive, and the server
hangs up on it one ping timeout later: the feature would disconnect precisely the devices
it exists to slow down, and each of them would come straight back onto a service that is
already behind.

So the socket is drained at all times and the load is shed above it. While the gate is
shut a frame is dropped without being parsed and the device is told how long to hold off,
at most once per retry window. Those reports are lost on purpose — that is what shedding
is — and the device learns about it immediately rather than by discovering a dead socket.
"""

import asyncio
import time
from collections import deque
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, Header, Query, WebSocket, WebSocketDisconnect
from pydantic import ValidationError
from redis.exceptions import RedisError
from starlette.types import Message

from geotrack.api.deps import get_websocket_resources, verify_ingest_key
from geotrack.api.problems import ProblemError
from geotrack.api.resources import AppResources
from geotrack.api.routes.ingest import describe_errors, report_window
from geotrack.clock import utc_now
from geotrack.ingest.service import BackpressureError, IngestService
from geotrack.observability.metrics import (
    ingest_rejected_total,
    ingest_shed_frames_total,
    ws_connections,
    ws_messages_sent_total,
)
from geotrack.realtime.protocol import (
    CLOSE_PROTOCOL_ERROR,
    CLOSE_UNAUTHORIZED,
    ack_frame,
    error_frame,
    throttle_frame,
)
from geotrack.schemas.ingest import (
    BatchTooLargeError,
    ReportWindowError,
    check_report_window,
    max_payload_bytes,
    parse_ingest_payload,
)

logger = structlog.get_logger(__name__)
router = APIRouter(tags=["ingest"])

MALFORMED_LIMIT = 20
MALFORMED_WINDOW_S = 60.0

WsResources = Annotated[AppResources, Depends(get_websocket_resources)]
IngestKeyQuery = Annotated[
    str | None, Query(description="Ingestion key, for clients that cannot set headers")
]


class MalformedBudget:
    """Allows a few bad frames, then hangs up on a client that only sends noise."""

    def __init__(self, *, limit: int = MALFORMED_LIMIT, window_s: float = MALFORMED_WINDOW_S):
        self._limit = limit
        self._window_s = window_s
        self._events: deque[float] = deque()

    def record(self) -> bool:
        """Note one bad frame; returns ``False`` once the budget is spent."""
        now = time.monotonic()
        self._events.append(now)
        while self._events and now - self._events[0] > self._window_s:
            self._events.popleft()
        return len(self._events) <= self._limit


@router.websocket("/ws/ingest")
async def ws_ingest(
    websocket: WebSocket,
    resources: WsResources,
    key: IngestKeyQuery = None,
    x_ingest_key: Annotated[str | None, Header()] = None,
) -> None:
    """Stream reports over one connection.

    Each frame is a report, an array of reports, or ``{"seq": n, "items": [...]}``; a
    frame carrying ``seq`` is answered with ``{"type":"ack","seq":n,"accepted":k}``.
    """
    await websocket.accept()
    try:
        verify_ingest_key(resources, x_ingest_key or key)
    except ProblemError:
        ingest_rejected_total.labels("ws", "auth").inc()
        await websocket.close(CLOSE_UNAUTHORIZED, "invalid ingest key")
        return

    ws_connections.labels("ingest").inc()
    try:
        await _DeviceSession(websocket, resources).run()
    except WebSocketDisconnect:
        pass
    finally:
        ws_connections.labels("ingest").dec()


class _DeviceSession:
    """One device connection: read, validate, enqueue, acknowledge."""

    def __init__(self, websocket: WebSocket, resources: AppResources) -> None:
        self._ws = websocket
        self._service: IngestService = resources.ingest
        self._backlog = resources.backlog
        self._settings = resources.settings
        self._max_age, self._max_future = report_window(resources.settings)
        self._budget = MalformedBudget()
        # Kept below the transport's own frame ceiling (uvicorn runs with
        # --ws-max-size 1048576) so an overlong frame gets a readable error and keeps its
        # socket, with the transport limit left as the hard backstop.
        self._max_frame = max_payload_bytes(resources.settings.ingest_max_batch)
        self._quiet_until = 0.0

    async def run(self) -> None:
        # A device that arrives during a throttle window is told before it sends anything.
        if self._backlog.throttled:
            await self._notify_throttled()

        while True:
            raw = _payload_of(await self._ws.receive())
            if raw is None:
                return
            if self._backlog.throttled:
                # Read, then dropped unparsed: the socket has to stay drained for the
                # keepalive to work, but the work behind the frame is what we are
                # shedding. It is counted in frames rather than reports, because the
                # frame was never parsed closely enough to know how many it held.
                ingest_shed_frames_total.labels("ws").inc()
                await self._notify_throttled()
                continue
            if not await self._handle(raw):
                return

    async def _notify_throttled(self, *, force: bool = False) -> None:
        """Ask the device to hold off, at most once per retry window it was given.

        ``force`` answers a frame we did parse and then refused: a device that sent data
        and got neither an ack nor a refusal would have no way to tell the two apart.
        """
        now = time.monotonic()
        if now < self._quiet_until and not force:
            return
        retry_after_ms = self._backlog.retry_after_ms
        self._quiet_until = now + retry_after_ms / 1000.0
        await self._send(throttle_frame(retry_after_ms=retry_after_ms), "throttle")

    async def _handle(self, raw: bytes) -> bool:
        """Process one frame; returns ``False`` when the connection should end."""
        if len(raw) > self._max_frame:
            return await self._reject(
                "payload_too_large",
                f"Frame exceeds {self._max_frame} bytes; send smaller batches.",
            )

        try:
            seq, reports = parse_ingest_payload(raw, max_items=self._settings.ingest_max_batch)
        except BatchTooLargeError as exc:
            return await self._reject("payload_too_large", str(exc), reports=exc.offered)
        except ValidationError as exc:
            return await self._reject("validation_error", describe_errors(exc))
        except ValueError as exc:
            return await self._reject("validation_error", str(exc))

        try:
            check_report_window(
                reports, now=utc_now(), max_age=self._max_age, max_future=self._max_future
            )
        except ReportWindowError as exc:
            self._service.count_rejected(len(reports), transport="ws", reason="window")
            await self._send(error_frame("out_of_window", str(exc), seq=seq), "error")
            return True

        try:
            accepted = await self._service.submit(reports, transport="ws")
        except BackpressureError:
            # The gate closed between the check and the write. The reports are already
            # counted as shed by the service; the device only needs to hear about it,
            # and it hears about it every time, because this frame was its question.
            await self._notify_throttled(force=True)
            return True
        except RedisError as exc:
            logger.warning("ingest queue unavailable", error=str(exc))
            await self._send(
                error_frame("ingest_unavailable", "The ingestion queue is not reachable.", seq=seq),
                "error",
            )
            return True

        if seq is not None:
            await self._send(ack_frame(seq=seq, accepted=accepted), "ack")
        return True

    async def _reject(self, code: str, detail: str, *, reports: int = 1) -> bool:
        """Answer one unusable frame, and count the reports it tried to carry.

        No ``seq`` goes back: a frame that could not be parsed is a frame whose sequence
        number was never read.
        """
        self._service.count_rejected(reports, transport="ws", reason="validation")
        await self._send(error_frame(code, detail), "error")
        if self._budget.record():
            return True
        await self._ws.close(CLOSE_PROTOCOL_ERROR, "too many malformed frames")
        return False

    async def _send(self, frame: bytes, kind: str) -> None:
        """Write one control frame, never waiting forever on a stalled socket.

        Text rather than binary: these are low-rate acknowledgements read by device
        firmware and debugging tools, where an inspectable frame beats a saved decode.
        """
        async with asyncio.timeout(self._settings.ws_send_timeout_s):
            await self._ws.send_text(frame.decode())
        ws_messages_sent_total.labels(kind).inc()


def _payload_of(message: Message) -> bytes | None:
    """Raw bytes of a client frame, or ``None`` once the peer has gone."""
    if message["type"] == "websocket.disconnect":
        return None
    text: str | None = message.get("text")
    if text is not None:
        return text.encode()
    data: bytes | None = message.get("bytes")
    return data if data is not None else b""
