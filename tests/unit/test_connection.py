import asyncio
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID

import orjson
import pytest
from prometheus_client import REGISTRY
from starlette.websockets import WebSocketDisconnect

from geotrack.clock import utc_now
from geotrack.geo import BBox
from geotrack.ids import new_uuid
from geotrack.realtime.connection import ClientConnection, frame_type
from geotrack.realtime.hub import PositionHub
from geotrack.realtime.protocol import (
    CLOSE_SLOW_CONSUMER,
    SessionInfo,
    error_frame,
    pong_frame,
)
from geotrack.settings import Settings
from tests.conftest import make_settings
from tests.waiting import wait_until

KYIV = BBox(west=30.0, south=50.0, east=31.0, north=51.0)
USER_ID = UUID("01929f6e-0000-7000-8000-000000000001")


class FakeSocket:
    """A websocket that records frames and can stall on demand."""

    def __init__(self) -> None:
        self.sent: list[bytes] = []
        self.binary: list[bytes] = []
        self.attempts = 0
        self.closed_with: int | None = None
        self.writable = asyncio.Event()
        self.writable.set()
        self.fail_with: Exception | None = None

    async def send_bytes(self, data: bytes) -> None:
        self.attempts += 1
        await self.writable.wait()
        if self.fail_with is not None:
            raise self.fail_with
        self.sent.append(data)
        self.binary.append(data)

    async def send_text(self, data: str) -> None:
        await self.send_bytes(data.encode())
        self.binary.pop()

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        await self.writable.wait()
        self.closed_with = code

    def frames(self) -> list[Any]:
        return [orjson.loads(frame) for frame in self.sent]


def counter(name: str) -> float:
    value = REGISTRY.get_sample_value(name)
    return 0.0 if value is None else value


@pytest.fixture
def hub() -> PositionHub:
    return PositionHub(cell_size_deg=0.05, stale_after_s=300)


@pytest.fixture
def socket() -> FakeSocket:
    return FakeSocket()


def build(socket: FakeSocket, hub: PositionHub, **overrides: Any) -> ClientConnection:
    settings: Settings = make_settings(**overrides)
    session = SessionInfo(id=new_uuid(), label="tests", connected_at=utc_now())
    return ClientConnection(socket, user_id=USER_ID, session=session, hub=hub, settings=settings)


@pytest.fixture
async def connection(socket: FakeSocket, hub: PositionHub) -> AsyncIterator[ClientConnection]:
    connection = build(socket, hub)
    task = asyncio.create_task(connection.sender())
    try:
        yield connection
    finally:
        connection.request_close(1000)
        await asyncio.wait_for(task, timeout=2)


async def test_frames_go_out_in_the_order_they_were_queued(
    socket: FakeSocket, connection: ClientConnection
) -> None:
    connection.send_control(pong_frame(t=1, server_t=2))
    connection.send_control(error_frame("bad_message", "nope"))

    await wait_until(lambda: len(socket.sent) == 2, what="two frames")
    assert [frame["type"] for frame in socket.frames()] == ["pong", "error"]


async def test_control_frames_overtake_position_frames(
    socket: FakeSocket, hub: PositionHub, connection: ClientConnection
) -> None:
    socket.writable.clear()
    connection.set_viewport(KYIV)
    hub.apply([("dev-1", 50.45, 30.52, 1_000)])
    connection.offer_positions(hub.drain(t_ms=1))
    connection.send_control(pong_frame(t=7, server_t=8))
    socket.writable.set()

    await wait_until(lambda: len(socket.sent) >= 2, what="both frames")
    assert [frame["type"] for frame in socket.frames()][:2] == ["pong", "positions"]


async def test_a_client_without_a_viewport_receives_no_positions(
    socket: FakeSocket, hub: PositionHub, connection: ClientConnection
) -> None:
    hub.apply([("dev-1", 50.45, 30.52, 1_000)])
    connection.offer_positions(hub.drain(t_ms=1))
    await asyncio.sleep(0.05)

    assert socket.sent == []


async def test_a_viewport_is_answered_with_a_full_snapshot(
    socket: FakeSocket, hub: PositionHub, connection: ClientConnection
) -> None:
    hub.apply([("kyiv", 50.45, 30.52, 1_000), ("sydney", -33.87, 151.21, 1_000)])
    hub.drain(t_ms=1)

    connection.set_viewport(KYIV)

    await wait_until(lambda: len(socket.sent) == 1, what="snapshot")
    frame = socket.frames()[0]
    assert frame["full"] is True
    assert [item[0] for item in frame["items"]] == ["kyiv"]


async def test_positions_outside_the_viewport_are_filtered_out(
    socket: FakeSocket, hub: PositionHub, connection: ClientConnection
) -> None:
    connection.set_viewport(KYIV)
    await wait_until(lambda: len(socket.sent) == 1, what="snapshot")

    hub.apply([("kyiv", 50.45, 30.52, 2_000), ("sydney", -33.87, 151.21, 2_000)])
    connection.offer_positions(hub.drain(t_ms=2))

    await wait_until(lambda: len(socket.sent) == 2, what="delta")
    frame = socket.frames()[1]
    assert frame["full"] is False
    assert [item[0] for item in frame["items"]] == ["kyiv"]


async def test_a_device_leaving_the_viewport_is_removed(
    socket: FakeSocket, hub: PositionHub, connection: ClientConnection
) -> None:
    hub.apply([("dev-1", 50.45, 30.52, 1_000)])
    hub.drain(t_ms=1)
    connection.set_viewport(KYIV)
    await wait_until(lambda: len(socket.sent) == 1, what="snapshot")

    hub.apply([("dev-1", -33.87, 151.21, 2_000)])
    connection.offer_positions(hub.drain(t_ms=2))

    await wait_until(lambda: len(socket.sent) == 2, what="delta")
    frame = socket.frames()[1]
    assert frame["removed"] == ["dev-1"]
    assert frame["items"] == []


async def test_a_device_moving_within_the_viewport_is_not_removed(
    socket: FakeSocket, hub: PositionHub, connection: ClientConnection
) -> None:
    hub.apply([("dev-1", 50.45, 30.52, 1_000)])
    hub.drain(t_ms=1)
    connection.set_viewport(KYIV)
    await wait_until(lambda: len(socket.sent) == 1, what="snapshot")

    hub.apply([("dev-1", 50.95, 30.92, 2_000)])
    connection.offer_positions(hub.drain(t_ms=2))

    await wait_until(lambda: len(socket.sent) == 2, what="delta")
    frame = socket.frames()[1]
    assert frame["removed"] == []
    assert [item[0] for item in frame["items"]] == ["dev-1"]


async def test_a_backed_up_client_gets_a_snapshot_instead_of_a_queue(
    socket: FakeSocket, hub: PositionHub, connection: ClientConnection
) -> None:
    connection.set_viewport(KYIV)
    await wait_until(lambda: len(socket.sent) == 1, what="snapshot")
    dropped_before = counter("geotrack_ws_position_frames_dropped_total")
    stalled_attempt = socket.attempts + 1

    socket.writable.clear()
    for tick, lat in enumerate((50.45, 50.46, 50.47), start=2):
        hub.apply([("dev-1", lat, 30.52, tick * 1_000)])
        connection.offer_positions(hub.drain(t_ms=tick))
        if tick == 2:
            # The sender takes the first frame and stalls inside the write; the second
            # frame then waits, and only the third has something to conflate with.
            await wait_until(lambda: socket.attempts == stalled_attempt, what="a stalled write")
    socket.writable.set()

    await wait_until(lambda: len(socket.sent) == 3, what="the resync")
    assert counter("geotrack_ws_position_frames_dropped_total") == dropped_before + 1
    last = socket.frames()[-1]
    assert last["full"] is True
    assert last["items"] == [["dev-1", 50.47, 30.52, 4_000]]


async def test_an_overflowing_control_queue_closes_the_connection(
    socket: FakeSocket, hub: PositionHub
) -> None:
    connection = build(socket, hub, ws_control_queue_max=2)
    disconnects_before = counter("geotrack_ws_slow_consumer_disconnects_total")

    for _ in range(3):
        connection.send_control(pong_frame(t=1, server_t=2))

    assert connection.closing
    assert connection.close_code == CLOSE_SLOW_CONSUMER
    assert counter("geotrack_ws_slow_consumer_disconnects_total") == disconnects_before + 1

    await connection.aclose()
    assert socket.closed_with == CLOSE_SLOW_CONSUMER


async def test_a_write_that_never_completes_closes_the_connection(
    socket: FakeSocket, hub: PositionHub
) -> None:
    connection = build(socket, hub, ws_send_timeout_s=0.05)
    socket.writable.clear()
    task = asyncio.create_task(connection.sender())

    connection.send_control(pong_frame(t=1, server_t=2))
    await asyncio.wait_for(task, timeout=2)

    assert connection.close_code == CLOSE_SLOW_CONSUMER
    # The socket is stuck, so teardown must not try to write a close frame down it.
    await asyncio.wait_for(connection.aclose(), timeout=1)
    assert socket.closed_with is None


async def test_a_disconnected_peer_ends_the_sender(socket: FakeSocket, hub: PositionHub) -> None:
    connection = build(socket, hub)
    socket.fail_with = WebSocketDisconnect(1001)
    task = asyncio.create_task(connection.sender())

    connection.send_control(pong_frame(t=1, server_t=2))
    await asyncio.wait_for(task, timeout=2)

    assert connection.closing
    assert socket.sent == []


async def test_a_relayed_frame_that_is_not_text_does_not_kill_the_sender(
    socket: FakeSocket, connection: ClientConnection
) -> None:
    """Frames on a user channel are relayed verbatim from another process.

    One that is not valid UTF-8 used to end the sender task with an uncaught
    UnicodeDecodeError: the client was dropped with a 1000 close code, no metric moved
    and nothing said why.
    """
    connection.send_control(b'{"type":"alert","alert":{"zone":"\xff"}}')
    connection.send_control(pong_frame(t=5, server_t=6))

    await wait_until(lambda: len(socket.sent) == 1, what="the frame after the bad one")
    assert [frame["type"] for frame in socket.frames()] == ["pong"]
    assert connection.closing is False


async def test_frame_type_is_read_from_the_wire_bytes() -> None:
    assert frame_type(pong_frame(t=1, server_t=2)) == "pong"
    assert frame_type(error_frame("x", "y")) == "error"
    assert frame_type(b'{"type":"not-a-frame-type"}') == "unknown"
    assert frame_type(b"garbage") == "unknown"
    # A producer that formats its JSON differently still gets labelled correctly.
    assert frame_type(b'{"alert": {}, "type": "alert"}') == "alert"
    assert frame_type(b'["not", "an", "object"]') == "unknown"


async def test_only_position_frames_travel_as_binary(
    socket: FakeSocket, hub: PositionHub, connection: ClientConnection
) -> None:
    hub.apply([("dev-1", 50.45, 30.52, 1_000)])
    hub.drain(t_ms=1)

    connection.send_control(pong_frame(t=1, server_t=2))
    connection.set_viewport(KYIV)

    await wait_until(lambda: len(socket.sent) == 2, what="both frames")
    assert [frame["type"] for frame in socket.frames()] == ["pong", "positions"]
    assert socket.binary == [socket.sent[1]]


async def test_a_zoomed_in_client_is_filtered_correctly_on_a_wide_delta(
    socket: FakeSocket, hub: PositionHub, connection: ClientConnection
) -> None:
    connection.set_viewport(KYIV)
    await wait_until(lambda: len(socket.sent) == 1, what="snapshot")

    # More changed cells than the viewport holds, which is the case the fan-out
    # switches strategy for.
    inside = [(f"in-{i}", 50.0 + i * 0.05, 30.0 + i * 0.05, 2_000) for i in range(20)]
    outside = [(f"out-{i}", 40.0 + i * 0.05, 10.0 + i * 0.05, 2_000) for i in range(600)]
    hub.apply([*inside, *outside])
    delta = hub.drain(t_ms=2)
    assert len(delta.chunks) > 500
    connection.offer_positions(delta)

    await wait_until(lambda: len(socket.sent) == 2, what="delta")
    devices = {item[0] for item in socket.frames()[1]["items"]}
    assert devices == {name for name, *_ in inside}
