"""Shard leases against a real Redis: only one replica may own a shard at a time."""

import asyncio

import pytest
from redis.asyncio import Redis

from geotrack.clock import now_ms
from geotrack.messaging.keys import PROCESSORS_ZSET, shard_lease_key
from geotrack.processor.leases import ShardLeaseManager

SHORT_TTL_MS = 200


@pytest.fixture
def alice(redis_client: Redis) -> ShardLeaseManager:
    return ShardLeaseManager(redis_client, shards=8, ttl_ms=5_000, instance_id="alice")


@pytest.fixture
def bob(redis_client: Redis) -> ShardLeaseManager:
    return ShardLeaseManager(redis_client, shards=8, ttl_ms=5_000, instance_id="bob")


async def test_only_one_replica_can_take_a_shard(
    alice: ShardLeaseManager, bob: ShardLeaseManager, redis_client: Redis
) -> None:
    assert await alice.acquire(1) is True
    assert await bob.acquire(1) is False

    assert await redis_client.get(shard_lease_key(1)) == b"alice"


async def test_a_lease_can_be_renewed_by_its_owner_only(
    alice: ShardLeaseManager, bob: ShardLeaseManager, redis_client: Redis
) -> None:
    await alice.acquire(2)

    assert await alice.renew(2) is True
    assert await bob.renew(2) is False

    ttl = await redis_client.pttl(shard_lease_key(2))
    assert 0 < ttl <= 5_000


async def test_renewing_a_shard_nobody_owns_fails(alice: ShardLeaseManager) -> None:
    assert await alice.renew(3) is False


async def test_a_lease_expires_on_its_own_and_can_be_stolen(redis_client: Redis) -> None:
    """A replica that dies without releasing must not hold its shards forever."""
    dying = ShardLeaseManager(redis_client, shards=8, ttl_ms=SHORT_TTL_MS, instance_id="dying")
    successor = ShardLeaseManager(redis_client, shards=8, ttl_ms=SHORT_TTL_MS, instance_id="next")

    assert await dying.acquire(4) is True
    await asyncio.sleep(SHORT_TTL_MS / 1000 + 0.1)

    assert await successor.acquire(4) is True
    # The old owner must notice, so that its consumer stops before the new one starts.
    assert await dying.renew(4) is False


async def test_releasing_only_touches_our_own_lease(
    alice: ShardLeaseManager, bob: ShardLeaseManager, redis_client: Redis
) -> None:
    await alice.acquire(5)

    await bob.release(5)
    assert await redis_client.get(shard_lease_key(5)) == b"alice"

    await alice.release(5)
    assert await redis_client.get(shard_lease_key(5)) is None


async def test_heartbeat_counts_the_live_replicas(
    alice: ShardLeaseManager, bob: ShardLeaseManager
) -> None:
    assert await alice.heartbeat() == 1
    assert await bob.heartbeat() == 2
    assert await alice.heartbeat() == 2


async def test_heartbeat_forgets_a_replica_that_stopped_reporting(
    redis_client: Redis, alice: ShardLeaseManager
) -> None:
    await redis_client.zadd(PROCESSORS_ZSET, {"ghost": now_ms() - 60_000})

    assert await alice.heartbeat() == 1
    assert await redis_client.zscore(PROCESSORS_ZSET, "ghost") is None


async def test_deregistering_removes_the_replica_from_the_membership_set(
    alice: ShardLeaseManager, bob: ShardLeaseManager
) -> None:
    await alice.heartbeat()
    await bob.heartbeat()

    await alice.deregister()

    assert await bob.heartbeat() == 1


async def test_a_lone_replica_is_offered_every_shard(alice: ShardLeaseManager) -> None:
    plan = await alice.rebalance(owned=set())

    assert (plan.live, plan.target) == (1, 8)
    assert set(plan.candidates) == set(range(8))


async def test_a_second_replica_makes_the_first_hand_shards_back(
    alice: ShardLeaseManager, bob: ShardLeaseManager
) -> None:
    for shard in range(8):
        assert await alice.acquire(shard) is True
    await bob.heartbeat()

    plan = await alice.rebalance(owned=set(range(8)))

    assert (plan.live, plan.target) == (2, 4)
    assert len(plan.release) == 4

    for shard in plan.release:
        await alice.release(shard)
    for shard in plan.release:
        assert await bob.acquire(shard) is True


async def test_membership_survives_only_as_long_as_the_heartbeats(
    redis_client: Redis,
) -> None:
    """The membership key carries its own expiry, so a dead fleet leaves no litter."""
    manager = ShardLeaseManager(redis_client, shards=4, ttl_ms=SHORT_TTL_MS, instance_id="solo")

    await manager.heartbeat()

    ttl = await redis_client.pttl(PROCESSORS_ZSET)
    assert 0 < ttl <= SHORT_TTL_MS * 10


async def test_a_short_lease_does_not_evict_replicas_that_are_still_alive(
    redis_client: Redis,
) -> None:
    """Membership is judged by heartbeats, which are slower than a lease renewal.

    Using the lease TTL as the membership window would drop every peer between two
    control rounds, and each replica would then conclude it was alone and claim the
    whole ring.
    """
    manager = ShardLeaseManager(
        redis_client, shards=8, ttl_ms=SHORT_TTL_MS, instance_id="alice", membership_ttl_ms=8_000
    )
    await redis_client.zadd(PROCESSORS_ZSET, {"bob": now_ms() - 2_000})

    plan = await manager.rebalance(owned=set())

    assert (plan.live, plan.target) == (2, 4)
