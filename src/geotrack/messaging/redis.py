from typing import Literal

from redis.asyncio import Redis
from redis.asyncio.connection import BlockingConnectionPool

type RedisPurpose = Literal["commands", "streams", "pubsub"]


def create_redis(url: str, *, purpose: RedisPurpose, max_connections: int = 64) -> Redis:
    """Create a client dedicated to one kind of traffic.

    Separate pools keep a burst of pipeline commands from starving the blocking stream
    reads, and keep the long-lived pub/sub connection out of both. redis-py 8 applies a
    5 s ``socket_timeout`` by default, which would abort ``XREADGROUP BLOCK`` and idle
    pub/sub reads, so those clients disable it. The blocking pool makes callers wait
    for a free connection instead of failing with "Too many connections".
    """
    socket_timeout = 5.0 if purpose == "commands" else None
    pool = BlockingConnectionPool.from_url(
        url,
        max_connections=max_connections,
        timeout=10,
        socket_timeout=socket_timeout,
        socket_connect_timeout=5.0,
        socket_keepalive=True,
        health_check_interval=30 if purpose == "pubsub" else 0,
        client_name=f"geotrack-{purpose}",
    )
    return Redis(connection_pool=pool)


async def close_redis(client: Redis) -> None:
    """Close the client and the pool this module created for it."""
    await client.aclose(close_connection_pool=True)
