"""Device-to-shard assignment for the ingest streams.

All reports of one device land on the same shard, and each shard has exactly one
active consumer, so a device's reports are always applied in arrival order.
"""

import zlib


def shard_for(device_id: str, shards: int) -> int:
    if shards < 1:
        raise ValueError("shards must be a positive integer")
    return zlib.crc32(device_id.encode()) % shards
