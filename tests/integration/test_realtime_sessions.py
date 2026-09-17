"""The cross-replica session directory against a real Redis."""

from datetime import timedelta
from uuid import UUID

import pytest
from redis.asyncio import Redis

from geotrack.clock import now_ms, utc_now
from geotrack.ids import new_uuid
from geotrack.messaging.keys import sessions_meta_key, sessions_zset_key
from geotrack.realtime.protocol import SessionInfo
from geotrack.realtime.sessions import SessionDirectory


def session(label: str, *, age_s: float = 0.0) -> SessionInfo:
    return SessionInfo(
        id=new_uuid(), label=label, connected_at=utc_now() - timedelta(seconds=age_s)
    )


async def expire(client: Redis, user_id: UUID, session_id: UUID) -> None:
    """Rewind a session's lease so it is due for pruning, without waiting for it."""
    await client.zadd(sessions_zset_key(user_id), {str(session_id): now_ms() - 1}, xx=True)


@pytest.fixture
def replica_one(redis_client: Redis) -> SessionDirectory:
    return SessionDirectory(redis_client, instance="api-1")


@pytest.fixture
def replica_two(redis_client: Redis) -> SessionDirectory:
    return SessionDirectory(redis_client, instance="api-2")


async def test_sessions_of_both_replicas_appear_in_one_list(
    replica_one: SessionDirectory, replica_two: SessionDirectory
) -> None:
    user_id = new_uuid()
    older, newer = session("Chrome on macOS", age_s=30), session("Firefox on Linux")

    await replica_one.register(user_id, older)
    await replica_two.register(user_id, newer)

    listed = await replica_one.list_for(user_id)
    assert [entry.id for entry in listed] == [older.id, newer.id]
    assert [entry.label for entry in listed] == ["Chrome on macOS", "Firefox on Linux"]
    assert await replica_two.list_for(user_id) == listed


async def test_a_user_without_sessions_has_an_empty_list(replica_one: SessionDirectory) -> None:
    assert await replica_one.list_for(new_uuid()) == []


async def test_removing_a_session_removes_it_everywhere(
    replica_one: SessionDirectory, replica_two: SessionDirectory, redis_client: Redis
) -> None:
    user_id = new_uuid()
    kept, dropped = session("kept"), session("dropped")
    await replica_one.register(user_id, kept)
    await replica_two.register(user_id, dropped)

    await replica_two.remove(user_id, dropped.id)

    assert [entry.id for entry in await replica_one.list_for(user_id)] == [kept.id]
    assert not await redis_client.hexists(sessions_meta_key(user_id), str(dropped.id))


async def test_an_expired_lease_drops_the_session_and_its_metadata(
    replica_one: SessionDirectory, redis_client: Redis
) -> None:
    user_id = new_uuid()
    entry = session("Chrome on macOS")
    await replica_one.register(user_id, entry)
    await expire(redis_client, user_id, entry.id)

    assert await replica_one.list_for(user_id) == []
    assert await redis_client.hgetall(sessions_meta_key(user_id)) == {}
    assert await redis_client.zcard(sessions_zset_key(user_id)) == 0


async def test_a_heartbeat_restores_a_session_that_was_pruned(
    replica_one: SessionDirectory, redis_client: Redis
) -> None:
    user_id = new_uuid()
    entry = session("Chrome on macOS")
    await replica_one.register(user_id, entry)
    await expire(redis_client, user_id, entry.id)
    assert await replica_one.list_for(user_id) == []

    await replica_one.refresh(user_id, [entry])

    restored = await replica_one.list_for(user_id)
    assert [(item.id, item.label) for item in restored] == [(entry.id, entry.label)]


async def test_the_keys_expire_once_nobody_refreshes_them(redis_client: Redis) -> None:
    directory = SessionDirectory(redis_client, instance="api-1", ttl_s=0.2)
    user_id = new_uuid()
    await directory.register(user_id, session("Chrome on macOS"))

    assert 0 < await redis_client.pttl(sessions_zset_key(user_id)) <= 600
    assert 0 < await redis_client.pttl(sessions_meta_key(user_id)) <= 600
