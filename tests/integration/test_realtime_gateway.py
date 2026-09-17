"""Two real API replicas, one Redis, several sessions per user.

This is the test the design exists for: a user's sessions may land on any replica, and
an alert produced somewhere else entirely has to reach all of them — and nobody else.
Everything here runs over real uvicorn servers and real websockets, because the parts
that break in production (subprotocol negotiation, close codes, fan-out ordering) do
not exist in an in-process test client.
"""

import asyncio
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

import orjson
import pytest
from httpx import AsyncClient
from prometheus_client import REGISTRY
from redis.asyncio import Redis
from uvicorn import Config, Server
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed
from websockets.typing import Subprotocol

from geotrack.api.app import create_app
from geotrack.api.security import create_access_token
from geotrack.clock import utc_now
from geotrack.ids import new_uuid
from geotrack.messaging.codec import PositionItem, encode_positions
from geotrack.messaging.keys import POSITIONS_CHANNEL, user_channel
from geotrack.realtime import gateway as gateway_module
from geotrack.realtime.gateway import WS_SUBPROTOCOL
from geotrack.realtime.protocol import (
    CLOSE_PROTOCOL_ERROR,
    CLOSE_SLOW_CONSUMER,
    CLOSE_TOO_MANY_SESSIONS,
    CLOSE_UNAUTHORIZED,
    alert_frame,
)
from geotrack.schemas.alerts import AlertKind, AlertOut, AlertZoneRef
from geotrack.settings import Settings
from tests.conftest import make_settings
from tests.integration.test_realtime_bridge import wait_for_subscribers

KYIV_VIEWPORT = {"type": "viewport", "bbox": [30.0, 50.0, 31.0, 51.0]}
# Nothing listens on port 1, so connecting to it is what a restarting database looks
# like to asyncpg: a refused connection, not a SQLAlchemy error.
CLOSED_DATABASE_URL = "postgresql+asyncpg://geotrack:geotrack@127.0.0.1:1/geotrack"


def counter(name: str) -> float:
    value = REGISTRY.get_sample_value(name)
    return 0.0 if value is None else value


@pytest.fixture
def unreachable_database(settings: Settings) -> Settings:
    return make_settings(
        database_url=CLOSED_DATABASE_URL, redis_url=settings.redis_url.get_secret_value()
    )


@asynccontextmanager
async def running_api(settings: Settings) -> AsyncIterator[int]:
    """A real uvicorn server on an ephemeral port, shut down cleanly afterwards."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    port = int(listener.getsockname()[1])

    server = Server(
        Config(
            create_app(settings),
            log_level="warning",
            access_log=False,
            ws_per_message_deflate=False,
            timeout_graceful_shutdown=5,
        )
    )
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        deadline = asyncio.get_running_loop().time() + 20
        while not server.started:
            if serving.done():
                await serving
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError("uvicorn did not start")
            await asyncio.sleep(0.02)
        yield port
    finally:
        server.should_exit = True
        await asyncio.wait_for(serving, timeout=20)
        listener.close()


@asynccontextmanager
async def client_session(
    port: int, token: str, *, user_agent: str = "tests"
) -> AsyncIterator[ClientConnection]:
    async with connect(
        f"ws://127.0.0.1:{port}/ws",
        subprotocols=[Subprotocol(WS_SUBPROTOCOL), Subprotocol(f"bearer.{token}")],
        user_agent_header=user_agent,
        open_timeout=10,
    ) as websocket:
        yield websocket


async def next_frame(websocket: ClientConnection, kind: str, *, timeout_s: float = 5.0) -> Any:
    """The next frame of a given type, skipping the ones this test does not care about."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise AssertionError(f"no {kind!r} frame arrived within {timeout_s}s")
        try:
            raw = await asyncio.wait_for(websocket.recv(), timeout=remaining)
        except TimeoutError:
            raise AssertionError(f"no {kind!r} frame arrived within {timeout_s}s") from None
        frame = orjson.loads(raw)
        if frame["type"] == kind:
            return frame


async def no_frame(websocket: ClientConnection, kind: str, *, within_s: float = 1.0) -> None:
    with pytest.raises(AssertionError):
        await next_frame(websocket, kind, timeout_s=within_s)


def token_for(username: str, settings: Settings) -> tuple[UUID, str]:
    user_id = new_uuid()
    token, _ = create_access_token(user_id=user_id, username=username, settings=settings)
    return user_id, token


def an_alert(zone_name: str) -> bytes:
    moment = utc_now()
    return alert_frame(
        AlertOut(
            id=1,
            kind=AlertKind.ENTER,
            zone=AlertZoneRef(id=new_uuid(), name=zone_name),
            device_id="dev-1",
            latitude=50.45,
            longitude=30.52,
            occurred_at=moment,
            created_at=moment,
        )
    )


@pytest.fixture
async def two_replicas(settings: Settings) -> AsyncIterator[tuple[int, int]]:
    async with running_api(settings) as first, running_api(settings) as second:
        yield first, second


async def test_an_alert_reaches_every_session_of_its_user_and_nobody_else(
    two_replicas: tuple[int, int], settings: Settings, redis_client: Redis
) -> None:
    first, second = two_replicas
    alice, alice_token = token_for("alice", settings)
    _, bob_token = token_for("bob", settings)

    async with (
        client_session(first, alice_token) as alice_one,
        client_session(second, alice_token) as alice_two,
        client_session(first, bob_token) as bob_one,
    ):
        await next_frame(alice_one, "hello")
        await next_frame(alice_two, "hello")
        await next_frame(bob_one, "hello")
        # Both replicas must hold the subscription before anything is published.
        await wait_for_subscribers(redis_client, user_channel(alice), count=2)

        await redis_client.publish(user_channel(alice), an_alert("Depot"))

        for session in (alice_one, alice_two):
            frame = await next_frame(session, "alert")
            assert frame["alert"]["zone"]["name"] == "Depot"
            assert frame["alert"]["kind"] == "enter"
        await no_frame(bob_one, "alert")


async def test_a_frame_published_by_a_rest_handler_reaches_both_replicas(
    two_replicas: tuple[int, int], settings: Settings, redis_client: Redis
) -> None:
    first, second = two_replicas
    alice, alice_token = token_for("alice", settings)

    async with (
        client_session(first, alice_token) as alice_one,
        client_session(second, alice_token) as alice_two,
    ):
        await next_frame(alice_one, "hello")
        await next_frame(alice_two, "hello")
        await wait_for_subscribers(redis_client, user_channel(alice), count=2)

        await redis_client.publish(
            user_channel(alice), b'{"type":"zone","op":"created","zone":{"name":"Depot"}}'
        )

        for session in (alice_one, alice_two):
            frame = await next_frame(session, "zone")
            assert frame["op"] == "created"


async def test_the_session_list_spans_replicas(
    two_replicas: tuple[int, int], settings: Settings
) -> None:
    first, second = two_replicas
    _, alice_token = token_for("alice", settings)

    async with client_session(first, alice_token, user_agent="") as alice_one:
        await next_frame(alice_one, "hello")
        async with client_session(
            second,
            alice_token,
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
            ),
        ) as alice_two:
            hello = await next_frame(alice_two, "hello")

            frame = await next_frame(alice_one, "sessions")
            while frame["count"] < 2:
                frame = await next_frame(alice_one, "sessions")

            assert {entry["label"] for entry in frame["sessions"]} == {
                "Unknown device",
                "Chrome on macOS",
            }
            assert hello["session_id"] in {entry["id"] for entry in frame["sessions"]}


async def test_positions_are_delivered_only_inside_the_viewport(
    two_replicas: tuple[int, int], settings: Settings, redis_client: Redis
) -> None:
    first, _ = two_replicas
    _, alice_token = token_for("alice", settings)
    items: list[PositionItem] = [
        ("kyiv-1", 50.45, 30.52, 1_700_000_000_000),
        ("sydney-1", -33.87, 151.21, 1_700_000_000_000),
    ]

    async with client_session(first, alice_token) as alice_one:
        await next_frame(alice_one, "hello")
        await alice_one.send(orjson.dumps(KYIV_VIEWPORT).decode())
        snapshot = await next_frame(alice_one, "positions")
        assert snapshot["full"] is True

        await wait_for_subscribers(redis_client, POSITIONS_CHANNEL, count=2)
        await redis_client.publish(POSITIONS_CHANNEL, encode_positions(items))

        frame = await next_frame(alice_one, "positions")
        while not frame["items"]:
            frame = await next_frame(alice_one, "positions")
        assert [item[0] for item in frame["items"]] == ["kyiv-1"]


async def test_a_client_that_never_sends_a_viewport_receives_no_positions(
    two_replicas: tuple[int, int], settings: Settings, redis_client: Redis
) -> None:
    first, _ = two_replicas
    _, alice_token = token_for("alice", settings)
    items: list[PositionItem] = [("kyiv-2", 50.45, 30.52, 1_700_000_000_000)]

    async with client_session(first, alice_token) as alice_one:
        await next_frame(alice_one, "hello")
        await wait_for_subscribers(redis_client, POSITIONS_CHANNEL, count=2)

        await redis_client.publish(POSITIONS_CHANNEL, encode_positions(items))

        await no_frame(alice_one, "positions")


async def test_a_ping_is_answered_and_a_bad_message_is_reported(
    two_replicas: tuple[int, int], settings: Settings
) -> None:
    first, _ = two_replicas
    _, alice_token = token_for("alice", settings)

    async with client_session(first, alice_token) as alice_one:
        await next_frame(alice_one, "hello")

        await alice_one.send('{"type":"ping","t":17}')
        pong = await next_frame(alice_one, "pong")
        assert pong["t"] == 17

        await alice_one.send("not json at all")
        error = await next_frame(alice_one, "error")
        assert error["code"] == "bad_message"


async def test_a_client_that_keeps_sending_rubbish_is_disconnected_and_can_return(
    two_replicas: tuple[int, int], settings: Settings
) -> None:
    first, _ = two_replicas
    _, alice_token = token_for("alice", settings)

    async with client_session(first, alice_token) as alice_one:
        await next_frame(alice_one, "hello")
        for _ in range(25):
            await alice_one.send("still not json")
        with pytest.raises(ConnectionClosed):
            async for _ in alice_one:
                pass
        assert alice_one.close_code == CLOSE_PROTOCOL_ERROR

    async with client_session(first, alice_token) as reconnected:
        assert (await next_frame(reconnected, "hello"))["protocol"] == 1


async def test_an_invalid_token_is_closed_with_an_application_code(
    two_replicas: tuple[int, int],
) -> None:
    first, _ = two_replicas

    async with client_session(first, "not-a-token") as rejected:
        with pytest.raises(ConnectionClosed):
            await asyncio.wait_for(rejected.recv(), timeout=5)
        assert rejected.close_code == CLOSE_UNAUTHORIZED


async def test_a_user_cannot_exceed_their_session_cap(settings: Settings) -> None:
    capped = settings.model_copy(update={"ws_max_sessions_per_user": 2})
    _, alice_token = token_for("alice", capped)

    async with (
        running_api(capped) as port,
        client_session(port, alice_token) as first,
        client_session(port, alice_token) as second,
    ):
        await next_frame(first, "hello")
        await next_frame(second, "hello")

        async with client_session(port, alice_token) as third:
            with pytest.raises(ConnectionClosed):
                await asyncio.wait_for(third.recv(), timeout=5)
            assert third.close_code == CLOSE_TOO_MANY_SESSIONS


async def test_a_connected_client_receives_periodic_stats(
    two_replicas: tuple[int, int], settings: Settings
) -> None:
    first, _ = two_replicas
    _, alice_token = token_for("alice", settings)

    async with client_session(first, alice_token) as alice_one:
        await next_frame(alice_one, "hello")

        frame = await next_frame(alice_one, "stats", timeout_s=8.0)

        assert frame["connections"] >= 1
        assert frame["devices"] >= 0
        assert frame["backlog"] >= 0


async def test_a_shutdown_closes_connected_clients_without_hanging(
    settings: Settings,
) -> None:
    _, alice_token = token_for("alice", settings)

    async with running_api(settings) as port:
        websocket = await connect(
            f"ws://127.0.0.1:{port}/ws",
            subprotocols=[Subprotocol(WS_SUBPROTOCOL), Subprotocol(f"bearer.{alice_token}")],
            open_timeout=10,
        )
        await next_frame(websocket, "hello")
        # The socket is deliberately still open when the server shuts down.

    # Leaving the context above already waited for uvicorn to finish; the client must
    # have been closed by the server rather than left hanging.
    await asyncio.wait_for(websocket.wait_closed(), timeout=5)
    assert websocket.close_code is not None


async def test_an_oversized_message_is_rejected_without_closing(
    two_replicas: tuple[int, int], settings: Settings
) -> None:
    first, _ = two_replicas
    _, alice_token = token_for("alice", settings)

    async with client_session(first, alice_token) as alice_one:
        await next_frame(alice_one, "hello")

        await alice_one.send("x" * (16 * 1024 + 1))

        error = await next_frame(alice_one, "error")
        assert error["code"] == "message_too_large"
        await alice_one.send('{"type":"ping","t":3}')
        assert (await next_frame(alice_one, "pong"))["t"] == 3


async def test_a_poisoned_positions_payload_does_not_take_the_replica_off_the_air(
    two_replicas: tuple[int, int], settings: Settings, redis_client: Redis
) -> None:
    """The defect as a connected client experienced it.

    One publish with a non-numeric latitude — a producer bug, a version skew, an
    operator at a redis-cli — used to end the replica's pub/sub reader, and every
    client on it stopped receiving positions, alerts, zone events and session lists
    while still looking perfectly connected.
    """
    first, _ = two_replicas
    alice, alice_token = token_for("alice", settings)
    items: list[PositionItem] = [("kyiv-4", 50.45, 30.52, 1_700_000_000_000)]

    async with client_session(first, alice_token) as alice_one:
        await next_frame(alice_one, "hello")
        await alice_one.send(orjson.dumps(KYIV_VIEWPORT).decode())
        assert (await next_frame(alice_one, "positions"))["full"] is True
        await wait_for_subscribers(redis_client, POSITIONS_CHANNEL, count=2)
        await wait_for_subscribers(redis_client, user_channel(alice), count=1)

        await redis_client.publish(POSITIONS_CHANNEL, b'{"items": [["dev-1", "north", 30.0, 1]]}')

        await redis_client.publish(user_channel(alice), an_alert("Depot"))
        assert (await next_frame(alice_one, "alert"))["alert"]["zone"]["name"] == "Depot"

        await redis_client.publish(POSITIONS_CHANNEL, encode_positions(items))
        frame = await next_frame(alice_one, "positions")
        while not frame["items"]:
            frame = await next_frame(alice_one, "positions")
        assert [item[0] for item in frame["items"]] == ["kyiv-4"]


async def test_a_message_that_is_only_oversized_in_bytes_is_rejected(
    two_replicas: tuple[int, int], settings: Settings
) -> None:
    """The limit is 16 KiB of bytes, not of characters.

    A multibyte payload measured with ``len()`` on the decoded string passed a guard
    it was four times over, and the transport had already buffered every byte of it.
    """
    first, _ = two_replicas
    _, alice_token = token_for("alice", settings)
    # 16,370 characters — under the limit — and 65,390 bytes, which is not.
    padded = orjson.dumps({"type": "ping", "t": 1, "pad": "\U0001f600" * 16_340}).decode()
    assert len(padded) < 16 * 1024 < len(padded.encode())

    async with client_session(first, alice_token) as alice_one:
        await next_frame(alice_one, "hello")

        await alice_one.send(padded)

        error = await next_frame(alice_one, "error")
        assert error["code"] == "message_too_large"
        await alice_one.send('{"type":"ping","t":4}')
        assert (await next_frame(alice_one, "pong"))["t"] == 4


async def test_a_user_cannot_exceed_their_session_cap_across_replicas(
    settings: Settings,
) -> None:
    """The cap is per user; a registry only knows its own replica.

    With the cap at one, a second session on a second replica used to be greeted
    normally, because nothing compared notes across replicas.
    """
    capped = settings.model_copy(update={"ws_max_sessions_per_user": 1})
    _, alice_token = token_for("alice", capped)

    async with (
        running_api(capped) as first,
        running_api(capped) as second,
        client_session(first, alice_token) as accepted,
    ):
        await next_frame(accepted, "hello")

        async with client_session(second, alice_token) as refused:
            with pytest.raises(ConnectionClosed):
                await asyncio.wait_for(refused.recv(), timeout=5)
            assert refused.close_code == CLOSE_TOO_MANY_SESSIONS

        # The session that was already there is not disturbed by the refusal.
        await accepted.send('{"type":"ping","t":11}')
        assert (await next_frame(accepted, "pong"))["t"] == 11


async def test_a_client_shed_as_a_slow_consumer_can_reconnect(settings: Settings) -> None:
    """The 1013 shed, end to end over a real socket, and the reconnect after it.

    A client that keeps asking for work without reading fills its control queue; the
    gateway closes it with 1013 and the client is expected to come straight back.
    """
    shedding = settings.model_copy(update={"ws_control_queue_max": 8})
    _, alice_token = token_for("alice", shedding)
    shed_before = counter("geotrack_ws_slow_consumer_disconnects_total")

    async with running_api(shedding) as port:
        async with client_session(port, alice_token) as flooding:
            await next_frame(flooding, "hello")
            for sent_at in range(500):
                await flooding.send(f'{{"type":"ping","t":{sent_at}}}')

            with pytest.raises(ConnectionClosed):
                async for _ in flooding:
                    pass
            assert flooding.close_code == CLOSE_SLOW_CONSUMER
        assert counter("geotrack_ws_slow_consumer_disconnects_total") > shed_before

        async with client_session(port, alice_token) as reconnected:
            hello = await next_frame(reconnected, "hello")
            # The shed session left nothing behind: the directory settles on the new
            # one alone, whichever way the two cleanups interleaved.
            sessions = await next_frame(reconnected, "sessions")
            while sessions["count"] != 1:
                sessions = await next_frame(reconnected, "sessions")
            assert [entry["id"] for entry in sessions["sessions"]] == [hello["session_id"]]

            await reconnected.send('{"type":"ping","t":13}')
            assert (await next_frame(reconnected, "pong"))["t"] == 13


async def test_the_api_serves_clients_while_the_database_is_unreachable(
    unreachable_database: Settings, redis_client: Redis
) -> None:
    """A database blip must not stop a replica from starting.

    asyncpg reports a refused connection as a plain OSError, which the warm start did
    not catch: the lifespan raised, uvicorn exited with a startup failure and the
    replica never bound a port — no websockets, no health endpoint, nothing.
    """
    _, alice_token = token_for("alice", unreachable_database)
    items: list[PositionItem] = [("kyiv-3", 50.45, 30.52, 1_700_000_000_000)]

    async with running_api(unreachable_database) as port:
        async with AsyncClient(base_url=f"http://127.0.0.1:{port}") as http:
            assert (await http.get("/health/live")).status_code == 200
            # Honest about the reason it is degraded, so a load balancer keeps away.
            assert (await http.get("/health/ready")).status_code == 503

        async with client_session(port, alice_token) as alice_one:
            await next_frame(alice_one, "hello")
            await alice_one.send(orjson.dumps(KYIV_VIEWPORT).decode())
            assert (await next_frame(alice_one, "positions"))["full"] is True

            await wait_for_subscribers(redis_client, POSITIONS_CHANNEL, count=1)
            await redis_client.publish(POSITIONS_CHANNEL, encode_positions(items))

            frame = await next_frame(alice_one, "positions")
            while not frame["items"]:
                frame = await next_frame(alice_one, "positions")
            assert [item[0] for item in frame["items"]] == ["kyiv-3"]


async def test_the_heartbeat_keeps_republishing_the_session_list(
    settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(gateway_module, "_SESSIONS_INTERVAL_S", 0.3)
    _, alice_token = token_for("alice", settings)

    async with running_api(settings) as port, client_session(port, alice_token) as alice_one:
        await next_frame(alice_one, "hello")
        await next_frame(alice_one, "sessions")

        repeated = await next_frame(alice_one, "sessions", timeout_s=3.0)
        assert repeated["count"] == 1
