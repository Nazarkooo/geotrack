import asyncio
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import Any, cast
from uuid import UUID

import pytest
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from geotrack.clock import utc_now
from geotrack.geo import BBox
from geotrack.ids import new_uuid
from geotrack.messaging.codec import PositionItem
from geotrack.messaging.keys import POSITIONS_CHANNEL
from geotrack.realtime import gateway as gateway_module
from geotrack.realtime.connection import ClientConnection
from geotrack.realtime.gateway import (
    WS_SUBPROTOCOL,
    Gateway,
    _keep_running,
    _rank_of,
    fan_out,
    token_from_handshake,
)
from geotrack.realtime.hub import PositionHub
from geotrack.realtime.protocol import SessionInfo, encode_position_item
from tests.conftest import make_settings

KYIV = BBox(west=30.0, south=50.0, east=31.0, north=51.0)
USER_ID = UUID("01929f6e-0000-7000-8000-000000000001")


class SilentSocket:
    async def send_bytes(self, data: bytes) -> None:
        return None

    async def send_text(self, data: str) -> None:
        return None

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        return None


class FakePubSub:
    """Enough of a pub/sub connection for the bridge to run against."""

    def __init__(self) -> None:
        self.channels: set[str] = set()

    async def subscribe(self, *channels: str) -> None:
        self.channels.update(channels)

    async def unsubscribe(self, *channels: str) -> None:
        self.channels.difference_update(channels)

    async def get_message(
        self,
        ignore_subscribe_messages: bool = False,
        timeout: float = 0.0,  # noqa: ASYNC109 - redis-py's signature, not ours
    ) -> None:
        await asyncio.sleep(timeout)
        return

    async def aclose(self) -> None:
        return None


class FakeRedis:
    def __init__(self) -> None:
        self.pubsub_object = FakePubSub()

    def pubsub(self) -> FakePubSub:
        return self.pubsub_object


class UnreachableDatabase:
    """A session factory for a database that refuses or swallows connections."""

    def __init__(self, *, hangs: bool = False) -> None:
        self.hangs = hangs

    def __call__(self) -> UnreachableDatabase:
        return self

    async def __aenter__(self) -> UnreachableDatabase:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def stream(self, *args: Any, **kwargs: Any) -> None:
        # asyncpg reaches the server only when the first statement runs, and reports a
        # refused connection as a plain OSError.
        if self.hangs:
            await asyncio.Event().wait()
        raise ConnectionRefusedError(61, "Connect call failed")


def build_gateway(redis: FakeRedis, *, hangs: bool = False, **overrides: Any) -> Gateway:
    return Gateway(
        make_settings(**overrides),
        redis=cast(Redis, redis),
        redis_pubsub=cast(Redis, redis),
        session_factory=cast(async_sessionmaker[AsyncSession], UnreachableDatabase(hangs=hangs)),
    )


def connection_for(hub: PositionHub, *, viewport: BBox | None = None) -> ClientConnection:
    connection = ClientConnection(
        SilentSocket(),
        user_id=USER_ID,
        session=SessionInfo(id=new_uuid(), label="tests", connected_at=utc_now()),
        hub=hub,
        settings=make_settings(),
    )
    if viewport is not None:
        connection.set_viewport(viewport)
    return connection


@pytest.fixture
def hub() -> PositionHub:
    return PositionHub(cell_size_deg=0.05, stale_after_s=300)


@pytest.fixture
def encodes(monkeypatch: pytest.MonkeyPatch) -> Callable[[], int]:
    """Counts how many position items the hub serialises."""
    calls = 0

    def counted(item: PositionItem) -> bytes:
        nonlocal calls
        calls += 1
        return encode_position_item(item)

    monkeypatch.setattr("geotrack.realtime.hub.encode_position_item", counted)
    return lambda: calls


def test_the_token_is_read_from_the_offered_subprotocols() -> None:
    offered = [WS_SUBPROTOCOL, "bearer.abc.def.ghi"]

    assert token_from_handshake(offered, None) == "abc.def.ghi"


def test_the_subprotocol_wins_over_the_query_string() -> None:
    offered = [WS_SUBPROTOCOL, "bearer.from-handshake"]

    assert token_from_handshake(offered, "from-query") == "from-handshake"


def test_the_query_string_is_the_fallback_for_tools_without_subprotocols() -> None:
    assert token_from_handshake([], "from-query") == "from-query"
    assert token_from_handshake([WS_SUBPROTOCOL], "from-query") == "from-query"


def test_an_anonymous_handshake_yields_no_token() -> None:
    assert token_from_handshake([WS_SUBPROTOCOL], None) is None


def test_a_failing_background_step_does_not_kill_its_loop() -> None:
    with _keep_running("test step"):
        raise RuntimeError("the step blew up")


def test_cancellation_still_stops_a_background_loop() -> None:
    with pytest.raises(asyncio.CancelledError), _keep_running("test step"):
        raise asyncio.CancelledError


async def test_a_replica_starts_degraded_when_the_database_refuses_connections() -> None:
    """A database blip must cost the hub its warm start, not the replica its boot.

    asyncpg raises a bare ``ConnectionRefusedError``, which is an ``OSError`` and not
    anything SQLAlchemy wraps, so catching ``SQLAlchemyError`` alone let it escape the
    lifespan and uvicorn exited with a startup failure instead of binding a port.
    """
    redis = FakeRedis()
    gateway = build_gateway(redis)

    await gateway.start()
    try:
        assert POSITIONS_CHANNEL in redis.pubsub_object.channels
    finally:
        await gateway.stop()


async def test_a_database_that_never_answers_does_not_hold_up_the_boot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gateway_module, "_WARM_START_TIMEOUT_S", 0.05)
    redis = FakeRedis()
    gateway = build_gateway(redis, hangs=True)

    await asyncio.wait_for(gateway.start(), timeout=2)
    try:
        assert POSITIONS_CHANNEL in redis.pubsub_object.channels
    finally:
        await gateway.stop()


async def test_a_tick_serialises_nothing_while_no_client_has_a_viewport(
    hub: PositionHub, encodes: Callable[[], int]
) -> None:
    """An idle replica still receives every position in the fleet.

    Encoding those ticks for nobody is pure waste, and it grows with the fleet rather
    than with the number of viewers.
    """
    idle = connection_for(hub)
    hub.apply([(f"dev-{i}", 50.0 + i * 0.05, 30.0, 1_000) for i in range(200)])

    fan_out(hub, (idle,), t_ms=1)

    assert encodes() == 0
    # The discarded tick is not left behind to be paid for by the next one.
    hub.apply([("dev-0", 50.1, 30.1, 2_000)])
    fan_out(hub, (idle,), t_ms=2)
    assert encodes() == 0


async def test_a_tick_is_serialised_once_for_every_client_that_is_watching(
    hub: PositionHub, encodes: Callable[[], int]
) -> None:
    watching = [connection_for(hub, viewport=KYIV) for _ in range(3)]
    idle = connection_for(hub)
    hub.apply([("dev-1", 50.45, 30.52, 1_000)])

    fan_out(hub, [*watching, idle], t_ms=1)

    # One encode for the changed cell, reused by every client that can see it.
    assert encodes() == 1


def session(connected_at: Any, session_id: UUID | None = None) -> SessionInfo:
    return SessionInfo(id=session_id or new_uuid(), label="tests", connected_at=connected_at)


def test_a_session_ranks_behind_the_ones_that_connected_before_it() -> None:
    moment = utc_now()
    older = session(moment - timedelta(seconds=5))
    mine = session(moment)
    newer = session(moment + timedelta(seconds=5))

    assert _rank_of(mine.id, [older, mine, newer]) == 1
    assert _rank_of(older.id, [older, mine, newer]) == 0
    assert _rank_of(newer.id, [older, mine, newer]) == 2


def test_sessions_that_share_a_timestamp_are_ranked_the_same_way_everywhere() -> None:
    """Replicas do not compare notes, so the tie-break has to be in the data."""
    moment = utc_now()
    first = session(moment, UUID("01929f6e-0000-7000-8000-00000000000a"))
    second = session(moment, UUID("01929f6e-0000-7000-8000-00000000000b"))
    listed: Sequence[SessionInfo] = [first, second]

    assert _rank_of(first.id, listed) == 0
    assert _rank_of(second.id, listed) == 1
    assert _rank_of(second.id, list(reversed(listed))) == 1


def test_a_session_the_directory_does_not_list_is_not_refused() -> None:
    assert _rank_of(new_uuid(), [session(utc_now()), session(utc_now())]) == 0
