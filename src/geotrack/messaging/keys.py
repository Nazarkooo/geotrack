"""Every Redis key and channel name used by the system.

Keeping them in one module means the API, the processor and the gateway cannot
drift apart on naming, and makes the data layout reviewable at a glance.
"""

from uuid import UUID

PREFIX = "geo:"

# Ingest streams: one per shard, consumed by exactly one processor at a time.
INGEST_GROUP = "processors"
DLQ_STREAM = f"{PREFIX}ingest:dlq"
SHARDS_META_KEY = f"{PREFIX}meta:shards"
STREAM_FIELD = b"r"

# Shard ownership and processor membership.
MAINTENANCE_LEASE_KEY = f"{PREFIX}lease:maintenance"
PROCESSORS_ZSET = f"{PREFIX}processors"

# Fan-out channels.
POSITIONS_CHANNEL = f"{PREFIX}ch:positions"
USER_CHANNEL_PATTERN = f"{PREFIX}ch:user:*"


def ingest_stream(shard: int) -> str:
    return f"{PREFIX}ingest:{shard}"


def consumer_name(shard: int) -> str:
    """Consumer names are per shard, not per replica.

    When a lease moves to another replica, the new owner reads the same consumer's
    pending entries with id ``0`` and finishes the work the old owner had claimed.
    """
    return f"shard-{shard}"


def shard_lease_key(shard: int) -> str:
    return f"{PREFIX}lease:shard:{shard}"


def user_channel(user_id: UUID) -> str:
    return f"{PREFIX}ch:user:{user_id}"


def user_id_from_channel(channel: str) -> UUID:
    return UUID(channel.rsplit(":", 1)[1])


def sessions_zset_key(user_id: UUID) -> str:
    return f"{PREFIX}sessions:{user_id}"


def sessions_meta_key(user_id: UUID) -> str:
    return f"{PREFIX}sessions:{user_id}:meta"
