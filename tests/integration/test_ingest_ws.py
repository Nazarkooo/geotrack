"""Websocket ingestion over a real socket.

Close codes, frame types and "the server stopped reading" only mean something on a real
connection, so these tests talk to an actual uvicorn instance rather than to an ASGI
shim: whatever passes here is what a device will see.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Any
from urllib.parse import quote

import orjson
import pytest
import uvicorn
from httpx import AsyncClient
from prometheus_client import REGISTRY
from redis.asyncio import Redis
from redis.exceptions import RedisError
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from geotrack.api.app import create_app
from geotrack.clock import utc_now
from geotrack.ingest.service import IngestService
from geotrack.messaging.keys import STREAM_FIELD, ingest_stream
from geotrack.realtime.protocol import CLOSE_PROTOCOL_ERROR, CLOSE_UNAUTHORIZED
from geotrack.schemas.ingest import max_payload_bytes
from geotrack.settings import Settings
from geotrack.sharding import shard_for
from tests.conftest import TEST_INGEST_KEY, make_settings
from tests.integration.conftest import (
    _await_startup,
    _QuietServer,
    queued_records,
    serving_app,
)
from tests.waiting import wait_until

KEY_HEADER = {"X-Ingest-Key": TEST_INGEST_KEY}
# The ceiling the container starts uvicorn with (``--ws-max-size 1048576``).
TRANSPORT_FRAME_LIMIT = 1024 * 1024
# What the handler itself accepts, derived from the batch limit the tests run with.
MAX_MESSAGE_BYTES = max_payload_bytes(1_000)
# uvicorn pings every 20 s and hangs up 20 s after an unanswered ping. Waiting that out
# would make the keepalive test a forty-second sleep, so the same machinery is driven on
# a scale of milliseconds; the mechanism under test is identical.
KEEPALIVE_S = 0.25


def _report(device_id: str, **overrides: Any) -> dict[str, Any]:
    return {
        "device_id": device_id,
        "latitude": 50.4501,
        "longitude": 30.5234,
        "timestamp": utc_now().isoformat(),
    } | overrides


async def _recv(socket: ClientConnection) -> dict[str, Any]:
    async with asyncio.timeout(5):
        payload: dict[str, Any] = orjson.loads(await socket.recv())
        return payload


@pytest.fixture
async def endpoint(settings: Settings) -> AsyncIterator[str]:
    async with serving_app(settings) as address:
        yield address


async def test_reports_sent_with_a_sequence_number_are_acknowledged(
    endpoint: str, redis_client: Redis
) -> None:
    async with connect(f"ws://{endpoint}/ws/ingest", additional_headers=KEY_HEADER) as socket:
        await socket.send(orjson.dumps({"seq": 1, "items": [_report("truck-1")]}).decode())
        first = await _recv(socket)
        await socket.send(
            orjson.dumps({"seq": 2, "items": [_report("truck-2"), _report("truck-3")]}).decode()
        )
        second = await _recv(socket)

    assert first == {"type": "ack", "seq": 1, "accepted": 1}
    assert second == {"type": "ack", "seq": 2, "accepted": 2}
    stream = ingest_stream(shard_for("truck-1", 8))
    assert [record.device_id for record in (await queued_records(redis_client))[stream]] == [
        "truck-1"
    ]


async def test_a_frame_without_a_sequence_number_is_queued_silently(
    endpoint: str, redis_client: Redis
) -> None:
    async with connect(f"ws://{endpoint}/ws/ingest", additional_headers=KEY_HEADER) as socket:
        await socket.send(orjson.dumps(_report("quiet")).decode())
        # Nothing comes back, so the only evidence is the queue itself.
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.5):
                await socket.recv()

    queued = await queued_records(redis_client)
    assert [record.device_id for records in queued.values() for record in records] == ["quiet"]


async def test_a_binary_frame_carries_reports_just_as_well(
    endpoint: str, redis_client: Redis
) -> None:
    # Constrained devices send packed bytes rather than a text frame; the payload is the
    # same JSON either way.
    async with connect(f"ws://{endpoint}/ws/ingest", additional_headers=KEY_HEADER) as socket:
        await socket.send(orjson.dumps({"seq": 2, "items": [_report("binary")]}))

        assert await _recv(socket) == {"type": "ack", "seq": 2, "accepted": 1}

    queued = await queued_records(redis_client)
    assert [record.device_id for records in queued.values() for record in records] == ["binary"]


async def test_an_oversized_frame_is_refused_without_parsing_it(
    endpoint: str, redis_client: Redis
) -> None:
    # Sized between the handler's ceiling and the transport's, which is the range where
    # the device gets an explanation rather than an unexplained protocol close.
    assert MAX_MESSAGE_BYTES < TRANSPORT_FRAME_LIMIT
    oversized = b"[" + b" " * MAX_MESSAGE_BYTES

    async with connect(
        f"ws://{endpoint}/ws/ingest", additional_headers=KEY_HEADER, max_size=None
    ) as socket:
        await socket.send(oversized)
        error = await _recv(socket)
        # The socket is still usable afterwards.
        await socket.send(orjson.dumps({"seq": 8, "items": [_report("after-big")]}).decode())
        ack = await _recv(socket)

    assert error["code"] == "payload_too_large"
    assert ack == {"type": "ack", "seq": 8, "accepted": 1}
    queued = await queued_records(redis_client)
    assert [record.device_id for records in queued.values() for record in records] == ["after-big"]


async def test_the_key_may_also_travel_as_a_query_parameter(endpoint: str) -> None:
    async with connect(f"ws://{endpoint}/ws/ingest?key={TEST_INGEST_KEY}") as socket:
        await socket.send(orjson.dumps({"seq": 9, "items": [_report("cli")]}).decode())

        assert (await _recv(socket))["accepted"] == 1


@pytest.mark.parametrize("suffix", ["", "?key=wrong", f"?key={quote('ключ')}"])
async def test_a_bad_key_is_closed_with_4401(endpoint: str, suffix: str) -> None:
    # The last one is a key outside ASCII: a query string is decoded as UTF-8, so a
    # device only has to be configured in another alphabet to reach this path, and it
    # has to end in the documented close code rather than in a crashed handler.
    async with connect(f"ws://{endpoint}/ws/ingest{suffix}") as socket:
        with pytest.raises(ConnectionClosed):
            await asyncio.wait_for(socket.recv(), timeout=5)

    assert socket.close_code == CLOSE_UNAUTHORIZED


async def test_a_key_outside_ascii_still_admits_the_fleet_configured_with_it(
    settings: Settings,
) -> None:
    # Header values travel as ISO-8859-1 on the wire, so a key in another alphabet can
    # only reach the server through the query parameter — which is exactly why that path
    # has to work rather than merely not crash.
    key = "ключ-достатньої-довжини"

    async with (
        serving_app(
            make_settings(
                database_url=settings.database_url.get_secret_value(),
                redis_url=settings.redis_url.get_secret_value(),
                ingest_api_key=key,
            )
        ) as address,
        connect(f"ws://{address}/ws/ingest?key={quote(key)}") as socket,
    ):
        await socket.send(orjson.dumps({"seq": 1, "items": [_report("kyiv")]}).decode())
        ack = await _recv(socket)

    assert ack == {"type": "ack", "seq": 1, "accepted": 1}


async def test_a_malformed_frame_answers_with_an_error_and_keeps_the_socket(
    endpoint: str,
) -> None:
    async with connect(f"ws://{endpoint}/ws/ingest", additional_headers=KEY_HEADER) as socket:
        await socket.send("this is not json")
        error = await _recv(socket)
        await socket.send(orjson.dumps({"seq": 3, "items": [_report("after-error")]}).decode())
        ack = await _recv(socket)

    assert error["type"] == "error"
    assert error["code"] == "validation_error"
    assert ack == {"type": "ack", "seq": 3, "accepted": 1}


async def test_a_validation_error_names_the_field_that_failed(endpoint: str) -> None:
    async with connect(f"ws://{endpoint}/ws/ingest", additional_headers=KEY_HEADER) as socket:
        await socket.send(orjson.dumps({"items": [_report("a", latitude=91.0)]}).decode())
        error = await _recv(socket)

    assert error["code"] == "validation_error"
    # Device firmware logs this line verbatim, so it has to point at the actual field.
    assert error["detail"].startswith("items.0.latitude: ")


async def test_a_report_outside_the_window_is_reported_against_its_sequence(
    endpoint: str, redis_client: Redis
) -> None:
    stale = _report("late", timestamp="2000-01-01T00:00:00Z")

    async with connect(f"ws://{endpoint}/ws/ingest", additional_headers=KEY_HEADER) as socket:
        await socket.send(orjson.dumps({"seq": 5, "items": [stale]}).decode())
        error = await _recv(socket)

    assert error["type"] == "error"
    assert error["code"] == "out_of_window"
    assert error["seq"] == 5
    assert await redis_client.xlen(ingest_stream(shard_for("late", 8))) == 0


async def _flood_with_garbage(socket: ClientConnection, rounds: int = 40) -> None:
    async with asyncio.timeout(10):
        for _ in range(rounds):
            await socket.send("garbage")
            await socket.recv()


async def test_a_client_that_only_sends_noise_is_disconnected(endpoint: str) -> None:
    async with connect(f"ws://{endpoint}/ws/ingest", additional_headers=KEY_HEADER) as socket:
        with pytest.raises(ConnectionClosed):
            await _flood_with_garbage(socket)

    assert socket.close_code == CLOSE_PROTOCOL_ERROR


def _throttled_settings(settings: Settings) -> Settings:
    return make_settings(
        database_url=settings.database_url.get_secret_value(),
        redis_url=settings.redis_url.get_secret_value(),
        ingest_backlog_high=1,
        ingest_backlog_low=0,
        ingest_backlog_poll_ms=20,
    )


def _requests_with_status(status: str) -> float:
    """How many HTTP responses this process has sent with a given status."""
    total = 0.0
    for metric in REGISTRY.collect():
        if metric.name != "geotrack_http_requests":
            continue
        for sample in metric.samples:
            if sample.name.endswith("_total") and sample.labels.get("status") == status:
                total += sample.value
    return total


def _counter(name: str, **labels: str) -> float:
    value = REGISTRY.get_sample_value(name, labels)
    return float(value or 0.0)


def _gauge(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


async def _gate_reopens() -> None:
    """Wait for the backlog monitor of the running application to open the gate."""
    await wait_until(
        lambda: _gauge("geotrack_ingest_throttled") == 0.0,
        timeout_s=5.0,
        what="the ingest gate reopening",
    )


@asynccontextmanager
async def _serving_with_keepalive(app_settings: Settings) -> AsyncIterator[str]:
    """A real server whose websocket keepalive runs on the test's timescale."""
    config = uvicorn.Config(
        create_app(app_settings),
        host="127.0.0.1",
        port=0,
        log_config=None,
        lifespan="on",
        ws="websockets-sansio",
        ws_ping_interval=KEEPALIVE_S,
        ws_ping_timeout=KEEPALIVE_S,
        access_log=False,
    )
    server = _QuietServer(config)
    task = asyncio.get_running_loop().create_task(server.serve())
    try:
        await _await_startup(server)
        yield f"127.0.0.1:{server.servers[0].sockets[0].getsockname()[1]}"
    finally:
        server.should_exit = True
        await task


@pytest.fixture
async def throttled_endpoint(settings: Settings, redis_client: Redis) -> AsyncIterator[str]:
    await redis_client.xadd(ingest_stream(0), {STREAM_FIELD: b"[]"})
    async with serving_app(_throttled_settings(settings)) as address:
        yield address


@pytest.fixture
async def throttled_keepalive_endpoint(
    settings: Settings, redis_client: Redis
) -> AsyncIterator[str]:
    await redis_client.xadd(ingest_stream(0), {STREAM_FIELD: b"[]"})
    async with _serving_with_keepalive(_throttled_settings(settings)) as address:
        yield address


async def test_a_throttled_server_announces_it_and_sheds_the_work(
    throttled_endpoint: str, redis_client: Redis
) -> None:
    async with connect(
        f"ws://{throttled_endpoint}/ws/ingest", additional_headers=KEY_HEADER
    ) as socket:
        throttle = await _recv(socket)
        assert throttle["type"] == "throttle"
        assert throttle["retry_after_ms"] > 0

        # The frame is taken off the socket — that part has to keep happening — but the
        # work behind it is refused: nothing is parsed, queued or acknowledged, and the
        # device is not told twice inside the retry window it was just given.
        await socket.send(orjson.dumps({"seq": 1, "items": [_report("eager")]}).decode())
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.5):
                await socket.recv()
        assert await redis_client.xlen(ingest_stream(shard_for("eager", 8))) == 0

        # Draining the backlog reopens the gate and the next batch is taken as usual.
        await redis_client.delete(ingest_stream(0))
        await _gate_reopens()
        await socket.send(orjson.dumps({"seq": 2, "items": [_report("resumed")]}).decode())
        ack = await _recv(socket)

    assert ack == {"type": "ack", "seq": 2, "accepted": 1}
    queued = await queued_records(redis_client)
    assert [record.device_id for records in queued.values() for record in records] == ["resumed"]


async def test_a_throttled_device_outlives_the_keepalive_timeout_and_resumes(
    throttled_keepalive_endpoint: str, redis_client: Redis
) -> None:
    """Shedding load must not cost the connection it is shedding.

    A handler that stops reading its socket also stops reading the keepalive pongs that
    travel on it, and the server's own transport hangs up on the device a ping timeout
    later — every throttled device reconnecting at once, on a service that is already
    behind.
    """
    async with connect(
        f"ws://{throttled_keepalive_endpoint}/ws/ingest", additional_headers=KEY_HEADER
    ) as socket:
        assert (await _recv(socket))["type"] == "throttle"
        # One frame is what arms it: the transport stops reading as soon as a message is
        # handed to the application and only resumes when the application asks for the
        # next one.
        await socket.send(orjson.dumps({"seq": 1, "items": [_report("shed")]}).decode())

        await asyncio.sleep(KEEPALIVE_S * 12)

        assert socket.close_code is None
        # A round trip on the wire, not just an absence of a close frame.
        async with asyncio.timeout(5):
            await (await socket.ping())

        await redis_client.delete(ingest_stream(0))
        await _gate_reopens()
        await socket.send(orjson.dumps({"seq": 2, "items": [_report("resumed")]}).decode())
        ack = await _recv(socket)

    assert ack == {"type": "ack", "seq": 2, "accepted": 1}


async def test_a_device_that_vanishes_while_throttled_is_forgotten_at_once(
    throttled_endpoint: str,
) -> None:
    before = _gauge("geotrack_ws_connections", kind="ingest")

    socket = await connect(f"ws://{throttled_endpoint}/ws/ingest", additional_headers=KEY_HEADER)
    assert (await _recv(socket))["type"] == "throttle"
    await socket.send(orjson.dumps(_report("ghost")).decode())
    assert _gauge("geotrack_ws_connections", kind="ingest") == before + 1

    # No close frame: a device loses power or its modem drops, which is how most of them
    # leave. The handler has to be sitting on the socket to notice.
    socket.transport.abort()

    await wait_until(
        lambda: _gauge("geotrack_ws_connections", kind="ingest") == before,
        timeout_s=5.0,
        what="the handler noticing a device that went away",
    )


async def test_an_unreachable_queue_is_reported_against_the_batch_that_was_lost(
    endpoint: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A device cannot see Redis; all it sees is a batch that was never acknowledged, so
    # the reason has to travel back on the socket instead of ending it.
    async def unreachable(*_: Any, **__: Any) -> int:
        raise RedisError("connection reset by peer")

    monkeypatch.setattr(IngestService, "submit", unreachable)

    async with connect(f"ws://{endpoint}/ws/ingest", additional_headers=KEY_HEADER) as socket:
        await socket.send(orjson.dumps({"seq": 4, "items": [_report("orphan")]}).decode())
        error = await _recv(socket)

    assert error == {
        "type": "error",
        "code": "ingest_unavailable",
        "detail": "The ingestion queue is not reachable.",
        "seq": 4,
    }


async def test_shed_frames_are_counted_even_though_they_are_never_parsed(
    throttled_endpoint: str,
) -> None:
    """Load shedding that leaves no trace is indistinguishable from data loss."""
    before = _counter("geotrack_ingest_shed_frames_total", transport="ws")

    async with connect(
        f"ws://{throttled_endpoint}/ws/ingest", additional_headers=KEY_HEADER
    ) as socket:
        assert (await _recv(socket))["type"] == "throttle"
        for index in range(3):
            await socket.send(orjson.dumps({"items": [_report(f"shed-{index}")]}).decode())
        await wait_until(
            lambda: _counter("geotrack_ingest_shed_frames_total", transport="ws") == before + 3,
            timeout_s=5,
            what="shed frames counted",
        )


async def test_a_refused_batch_is_always_answered_even_inside_a_quiet_window(
    throttled_endpoint: str,
) -> None:
    """A device that sent data and heard nothing cannot tell refusal from silence."""
    async with connect(
        f"ws://{throttled_endpoint}/ws/ingest", additional_headers=KEY_HEADER
    ) as socket:
        assert (await _recv(socket))["type"] == "throttle"

        # The gate is closed, so the handler sheds without parsing: no answer is due.
        await socket.send(orjson.dumps({"seq": 7, "items": [_report("quiet")]}).decode())
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.5):
                await socket.recv()


async def test_a_client_that_hangs_up_mid_body_is_not_a_server_error(endpoint: str) -> None:
    """Under load a fleet abandons slow requests; that is not a fault of the service."""
    host, _, port = endpoint.rpartition(":")
    before = _requests_with_status("500")

    _reader, writer = await asyncio.open_connection(host, int(port))
    body = orjson.dumps({"items": [_report("half-sent")]})
    writer.write(
        b"POST /api/v1/ingest/locations HTTP/1.1\r\n"
        b"Host: %s\r\n" % host.encode() + b"X-Ingest-Key: " + TEST_INGEST_KEY.encode() + b"\r\n"
        b"Content-Type: application/json\r\n"
        b"Content-Length: " + str(len(body) + 64).encode() + b"\r\n\r\n" + body[: len(body) // 2]
    )
    await writer.drain()
    writer.close()
    with suppress(ConnectionError):
        await writer.wait_closed()

    # The next request still works, and nothing was charged to the 5xx counter.
    async with AsyncClient(base_url=f"http://{endpoint}", timeout=10) as client:
        response = await client.post(
            "/api/v1/ingest/locations",
            json={"items": [_report("after-hangup")]},
            headers=KEY_HEADER,
        )
    assert response.status_code == 202
    assert _requests_with_status("500") == before
