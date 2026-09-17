from collections.abc import Callable, Mapping, Sequence
from types import TracebackType
from typing import Any, cast

import pytest
from redis.asyncio import Redis

from geotrack.clock import from_epoch_ms
from geotrack.ids import new_uuid
from geotrack.messaging.keys import sessions_meta_key, sessions_zset_key
from geotrack.realtime import sessions as sessions_module
from geotrack.realtime.protocol import SessionInfo
from geotrack.realtime.sessions import UNKNOWN_CLIENT, SessionDirectory, describe_client

TTL_S = 30.0
TTL_MS = 30_000
START_MS = 1_700_000_000_000

CHROME_MAC = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
)
EDGE_WINDOWS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
)
FIREFOX_LINUX = "Mozilla/5.0 (X11; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0"
SAFARI_IPHONE = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_0 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1"
)
CHROME_ANDROID = (
    "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36"
)


@pytest.mark.parametrize(
    ("user_agent", "expected"),
    [
        (CHROME_MAC, "Chrome on macOS"),
        (EDGE_WINDOWS, "Edge on Windows"),
        (FIREFOX_LINUX, "Firefox on Linux"),
        (SAFARI_IPHONE, "Safari on iOS"),
        (CHROME_ANDROID, "Chrome on Android"),
    ],
)
def test_browser_sessions_get_a_readable_label(user_agent: str, expected: str) -> None:
    assert describe_client(user_agent) == expected


def test_non_browser_clients_keep_their_product_token() -> None:
    assert describe_client("python-httpx/0.28.1") == "python-httpx"


@pytest.mark.parametrize("user_agent", [None, "", "   ", "///"])
def test_unusable_user_agents_fall_back(user_agent: str | None) -> None:
    assert describe_client(user_agent) == UNKNOWN_CLIENT


def test_labels_are_bounded_and_stripped_of_markup() -> None:
    label = describe_client("<script>alert('x')</script>" + "a" * 200)

    assert len(label) <= 40
    assert "<" not in label


def _bound(value: str | float) -> tuple[float, bool]:
    """Redis score bounds: ``-inf``, ``+inf``, a number, or ``(`` for an exclusive one."""
    text = str(value)
    if text.startswith("("):
        return float(text[1:]), True
    return float(text), False


class FakePipeline:
    """Queues commands and applies them together, the way a MULTI does."""

    def __init__(self, redis: FakeRedis) -> None:
        self._redis = redis
        self._queued: list[Callable[[], Any]] = []

    async def __aenter__(self) -> FakePipeline:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        return False

    def zadd(self, key: str, mapping: Mapping[str, float]) -> None:
        self._queued.append(lambda: self._redis.zadd(key, mapping))

    def zrem(self, key: str, *members: bytes) -> None:
        self._queued.append(lambda: self._redis.zrem(key, *members))

    def zrangebyscore(self, key: str, low: str | float, high: str | float) -> None:
        self._queued.append(lambda: self._redis.zrangebyscore(key, low, high))

    def hset(self, key: str, *, mapping: Mapping[str, bytes]) -> None:
        self._queued.append(lambda: self._redis.hset(key, mapping))

    def hdel(self, key: str, *fields: bytes) -> None:
        self._queued.append(lambda: self._redis.hdel(key, *fields))

    def pexpire(self, key: str, ms: int) -> None:
        self._queued.append(lambda: self._redis.pexpire(key, ms))

    async def execute(self) -> list[Any]:
        results = [command() for command in self._queued]
        self._queued.clear()
        return results


class FakeRedis:
    """The handful of commands the session directory uses, in memory."""

    def __init__(self) -> None:
        self.zsets: dict[str, dict[bytes, float]] = {}
        self.hashes: dict[str, dict[bytes, bytes]] = {}
        self.expires_in_ms: dict[str, int] = {}

    def pipeline(self, transaction: bool = False) -> FakePipeline:
        return FakePipeline(self)

    def zadd(self, key: str, mapping: Mapping[str, float]) -> None:
        entries = self.zsets.setdefault(key, {})
        entries.update({member.encode(): score for member, score in mapping.items()})

    def zrem(self, key: str, *members: bytes) -> None:
        entries = self.zsets.get(key, {})
        for member in members:
            entries.pop(member, None)

    def zrangebyscore(self, key: str, low: str | float, high: str | float) -> list[bytes]:
        lowest, low_exclusive = _bound(low)
        highest, high_exclusive = _bound(high)
        entries = self.zsets.get(key, {})
        chosen = [
            member
            for member, score in entries.items()
            if (score > lowest if low_exclusive else score >= lowest)
            and (score < highest if high_exclusive else score <= highest)
        ]
        return sorted(chosen, key=lambda member: (entries[member], member))

    def hset(self, key: str, mapping: Mapping[str, bytes]) -> None:
        self.hashes.setdefault(key, {}).update(
            {field.encode(): value for field, value in mapping.items()}
        )

    def hdel(self, key: str, *fields: bytes) -> None:
        entries = self.hashes.get(key, {})
        for field in fields:
            entries.pop(field, None)

    def pexpire(self, key: str, ms: int) -> None:
        self.expires_in_ms[key] = ms

    async def hmget(self, key: str, fields: Sequence[bytes]) -> list[bytes | None]:
        entries = self.hashes.get(key, {})
        return [entries.get(field) for field in fields]


@pytest.fixture
def redis() -> FakeRedis:
    return FakeRedis()


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Callable[[int], None]:
    """Moves the directory's clock, so a lease can expire without waiting for it."""
    moment = START_MS

    def set_to(value: int) -> None:
        nonlocal moment
        moment = value

    monkeypatch.setattr(sessions_module, "now_ms", lambda: moment)
    return set_to


@pytest.fixture
def directory(redis: FakeRedis) -> SessionDirectory:
    return SessionDirectory(cast(Redis, redis), instance="api-1", ttl_s=TTL_S)


def a_session() -> SessionInfo:
    return SessionInfo(id=new_uuid(), label="Chrome on macOS", connected_at=from_epoch_ms(START_MS))


async def test_a_session_is_listed_until_its_lease_runs_out(
    directory: SessionDirectory, clock: Callable[[int], None]
) -> None:
    user_id, session = new_uuid(), a_session()
    await directory.register(user_id, session)

    clock(START_MS + TTL_MS - 1)
    assert [entry.id for entry in await directory.list_for(user_id)] == [session.id]

    clock(START_MS + TTL_MS)
    assert await directory.list_for(user_id) == []


async def test_an_expired_lease_takes_its_metadata_with_it(
    directory: SessionDirectory, redis: FakeRedis, clock: Callable[[int], None]
) -> None:
    """A replica that dies leaves entries behind; reading the list is what clears them."""
    user_id, session = new_uuid(), a_session()
    await directory.register(user_id, session)
    clock(START_MS + TTL_MS)

    assert await directory.list_for(user_id) == []

    assert redis.zsets[sessions_zset_key(user_id)] == {}
    assert redis.hashes[sessions_meta_key(user_id)] == {}


async def test_a_heartbeat_pushes_the_lease_out(
    directory: SessionDirectory, clock: Callable[[int], None]
) -> None:
    user_id, session = new_uuid(), a_session()
    await directory.register(user_id, session)

    clock(START_MS + 20_000)
    await directory.refresh(user_id, [session])

    clock(START_MS + TTL_MS + 1)
    assert [entry.id for entry in await directory.list_for(user_id)] == [session.id]


async def test_the_keys_outlive_a_single_lease(
    directory: SessionDirectory, redis: FakeRedis
) -> None:
    """Long enough for a replica that fell behind to heartbeat its sessions back."""
    user_id = new_uuid()
    await directory.register(user_id, a_session())

    assert redis.expires_in_ms[sessions_zset_key(user_id)] == TTL_MS * 3
    assert redis.expires_in_ms[sessions_meta_key(user_id)] == TTL_MS * 3


async def test_a_session_whose_metadata_is_unreadable_is_left_out(
    directory: SessionDirectory, redis: FakeRedis
) -> None:
    user_id, readable, damaged = new_uuid(), a_session(), a_session()
    await directory.register(user_id, readable)
    await directory.register(user_id, damaged)
    redis.hashes[sessions_meta_key(user_id)][str(damaged.id).encode()] = b"{not json"

    assert [entry.id for entry in await directory.list_for(user_id)] == [readable.id]
