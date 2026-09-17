"""Shard ownership.

A shard must have exactly one live consumer, or two processors would interleave the
reports of the same device. Ownership is a Redis lease: a short-lived key holding the
owner's instance id, renewed while the owner is healthy and expiring on its own when
the owner dies. Replicas agree on how many shards each may hold by counting the live
members of a heartbeat set, so no coordinator is needed.
"""

import math
import zlib
from dataclasses import dataclass

import structlog
from redis.asyncio import Redis
from redis.commands.core import AsyncScript

from geotrack.clock import now_ms
from geotrack.messaging.keys import PROCESSORS_ZSET, shard_lease_key

logger = structlog.get_logger(__name__)

# Compare-and-act, so a lease that has already been taken over by somebody else is never
# extended or deleted by its previous owner.
RENEW_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""

RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""


@dataclass(frozen=True, slots=True)
class RebalancePlan:
    """What this replica should do about its ownership, and why.

    A plain ``(to_acquire, to_release)`` pair cannot say any of it: ``candidates`` is an
    ordered list rather than a set, because the rotation is what stops two replicas from
    racing for the same shard, and the caller needs ``target`` to know when to stop
    taking from it. ``live`` is here because a wrong membership count is the one input
    that can make every replica claim the whole ring, and it belongs in the log line
    next to the decision it produced.
    """

    live: int
    target: int
    # Shards this replica does not hold, in the order it should try them. Every free
    # shard is offered, not just `target - owned`: the first few are often leased by
    # another replica, and a short list would leave the rest unclaimed forever.
    candidates: tuple[int, ...]
    release: tuple[int, ...]


def fair_share_plan(*, shards: int, owned: set[int], live: int, offset: int) -> RebalancePlan:
    """Decide what this replica should hold when ``live`` replicas share ``shards``.

    ``ceil`` rather than floor, so an uneven split still covers every shard: with 8
    shards and 3 replicas the target is 3 and the last replica simply finds nothing
    left to take.
    """
    live = max(live, 1)
    target = math.ceil(shards / live)
    surplus = len(owned) - target
    if surplus > 0:
        # Give back the highest shards first: every replica applies the same rule, so
        # the shards that change hands are predictable instead of arbitrary.
        return RebalancePlan(
            live=live,
            target=target,
            candidates=(),
            release=tuple(sorted(owned, reverse=True)[:surplus]),
        )

    rotation = offset % shards if shards else 0
    order = [(rotation + step) % shards for step in range(shards)]
    candidates = tuple(shard for shard in order if shard not in owned) if surplus < 0 else ()
    return RebalancePlan(live=live, target=target, candidates=candidates, release=())


class ShardLeaseManager:
    def __init__(
        self,
        redis: Redis,
        *,
        shards: int,
        ttl_ms: int,
        instance_id: str,
        membership_ttl_ms: int | None = None,
    ) -> None:
        self._redis = redis
        self._shards = shards
        self._ttl_ms = ttl_ms
        # Membership is judged by heartbeats, which arrive on the control loop's
        # interval rather than the lease's. Reusing the lease TTL here would evict
        # perfectly healthy replicas whenever a lease is shorter than that interval,
        # and every replica would then believe it is alone and claim every shard.
        self._membership_ttl_ms = membership_ttl_ms or ttl_ms
        self._instance_id = instance_id
        # A stable per-replica rotation keeps two replicas that start together from
        # racing for the same shard on every round.
        self._offset = zlib.crc32(instance_id.encode()) % shards
        self._renew: AsyncScript = redis.register_script(RENEW_SCRIPT)
        self._release: AsyncScript = redis.register_script(RELEASE_SCRIPT)

    @property
    def instance_id(self) -> str:
        return self._instance_id

    async def heartbeat(self) -> int:
        """Announce this replica and return how many are currently live."""
        now = now_ms()
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.zadd(PROCESSORS_ZSET, {self._instance_id: now})
            pipe.zremrangebyscore(PROCESSORS_ZSET, "-inf", now - self._membership_ttl_ms)
            # The set would otherwise outlive the last replica and keep a dead name.
            pipe.pexpire(PROCESSORS_ZSET, self._membership_ttl_ms * 10)
            pipe.zcard(PROCESSORS_ZSET)
            results = await pipe.execute()
        return max(int(results[-1]), 1)

    async def acquire(self, shard: int) -> bool:
        taken = await self._redis.set(
            shard_lease_key(shard), self._instance_id, nx=True, px=self._ttl_ms
        )
        return bool(taken)

    async def renew(self, shard: int) -> bool:
        """Extend a lease we still hold. ``False`` means it is somebody else's now."""
        result = await self._renew(
            keys=[shard_lease_key(shard)], args=[self._instance_id, self._ttl_ms]
        )
        return bool(result)

    async def release(self, shard: int) -> None:
        await self._release(keys=[shard_lease_key(shard)], args=[self._instance_id])

    async def rebalance(self, owned: set[int]) -> RebalancePlan:
        """Heartbeat, then plan ownership against the membership that heartbeat saw."""
        live = await self.heartbeat()
        return fair_share_plan(shards=self._shards, owned=owned, live=live, offset=self._offset)

    async def deregister(self) -> None:
        await self._redis.zrem(PROCESSORS_ZSET, self._instance_id)
