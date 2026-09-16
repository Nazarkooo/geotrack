"""The shard count is part of the queue's layout, not a per-replica preference.

Reports are placed on ``geo:ingest:{crc32(device_id) % shards}`` and the processors read
exactly the streams they were told to read. Change the number on one side and the two
sides address different streams: reports are accepted with a 202, land where nothing is
consuming and are never stored — and the fleet's own ordering guarantee goes with them,
because a device moves to a different shard. So the count is recorded the first time the
system runs and every later start has to agree with what is recorded.
"""

import structlog
from redis.asyncio import Redis

from geotrack.messaging.keys import SHARDS_META_KEY
from geotrack.messaging.redis import close_redis, create_redis

logger = structlog.get_logger(__name__)


class ShardCountMismatchError(RuntimeError):
    """This process is configured for a different partitioning than the queue holds."""

    def __init__(self, configured: int, recorded: str) -> None:
        super().__init__(
            f"configured for {configured} ingest shards, but the queue is partitioned into "
            f"{recorded}: drain the ingest streams and clear {SHARDS_META_KEY} before "
            f"changing INGEST_SHARDS, or set it back to {recorded}"
        )
        self.configured = configured
        self.recorded = recorded


async def verify_shard_count(redis_url: str, *, shards: int) -> None:
    """Record the shard count, or refuse to run against a queue built for another one.

    Done on a connection of its own before the process opens anything else, so a replica
    that disagrees fails at startup rather than quietly writing where nobody reads.
    """
    redis: Redis = create_redis(redis_url, purpose="commands", max_connections=1)
    try:
        # SET NX GET claims the key and reports what was already there in one round trip,
        # so replicas starting together cannot disagree about who wrote first.
        recorded = await redis.set(SHARDS_META_KEY, shards, nx=True, get=True)
    finally:
        await close_redis(redis)

    if recorded is None:
        logger.info("ingest shard count recorded", shards=shards)
        return

    # Compared as text: that is how Redis stores it, and a value nobody can parse is a
    # mismatch to report rather than an exception to raise from a startup path.
    previous = recorded.decode() if isinstance(recorded, bytes) else str(recorded)
    if previous != str(shards):
        raise ShardCountMismatchError(shards, previous)
