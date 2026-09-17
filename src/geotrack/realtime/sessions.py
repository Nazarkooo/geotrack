"""The list of a user's live sessions, shared across replicas through Redis.

A user may be connected from several browsers at once, on any replica. Each replica
publishes the sessions it holds into one sorted set per user, with a short TTL that
its heartbeat renews, so the list self-heals when a replica dies without cleaning up.
"""

from collections.abc import Sequence
from typing import Any, cast
from uuid import UUID

import orjson
import structlog
from redis.asyncio import Redis

from geotrack.clock import from_epoch_ms, now_ms, to_epoch_ms
from geotrack.messaging.keys import sessions_meta_key, sessions_zset_key
from geotrack.realtime.protocol import SessionInfo

logger = structlog.get_logger(__name__)

UNKNOWN_CLIENT = "Unknown device"
_MAX_LABEL_LENGTH = 40
_LABEL_EXTRA_CHARS = frozenset(" .-_")

# Checked in order: an Edge user agent also claims Chrome and Safari, and an iPhone
# claims "like Mac OS X", so the most specific token has to win.
_BROWSERS = (
    ("Edg/", "Edge"),
    ("OPR/", "Opera"),
    ("Firefox/", "Firefox"),
    ("Chrome/", "Chrome"),
    ("Safari/", "Safari"),
)
_PLATFORMS = (
    ("Windows NT", "Windows"),
    ("Android", "Android"),
    ("iPhone", "iOS"),
    ("iPad", "iPadOS"),
    ("CrOS", "ChromeOS"),
    ("Macintosh", "macOS"),
    ("Mac OS X", "macOS"),
    ("Linux", "Linux"),
)


def describe_client(user_agent: str | None) -> str:
    """A short label for the session list; a raw user agent is noise in a UI."""
    if not user_agent:
        return UNKNOWN_CLIENT
    browser = next((name for token, name in _BROWSERS if token in user_agent), None)
    platform = next((name for token, name in _PLATFORMS if token in user_agent), None)
    if browser and platform:
        return f"{browser} on {platform}"
    if browser or platform:
        return cast(str, browser or platform)
    # Not a browser: show the product token of the client that connected.
    return _sanitised(user_agent.split("/", 1)[0]) or UNKNOWN_CLIENT


def _sanitised(value: str) -> str:
    kept = "".join(c for c in value if c.isalnum() or c in _LABEL_EXTRA_CHARS)
    return kept.strip()[:_MAX_LABEL_LENGTH]


class SessionDirectory:
    def __init__(self, redis: Redis, *, instance: str, ttl_s: float = 30.0) -> None:
        self._redis = redis
        self._instance = instance
        self._ttl_ms = max(int(ttl_s * 1_000), 1)

    async def register(self, user_id: UUID, session: SessionInfo) -> None:
        await self.refresh(user_id, (session,))

    async def refresh(self, user_id: UUID, sessions: Sequence[SessionInfo]) -> None:
        """Extend the lease of every session this replica still holds for a user.

        The label and connect time are written again rather than only the score, so a
        replica that fell behind long enough to be pruned restores a complete entry.
        """
        if not sessions:
            return
        expires_at = now_ms() + self._ttl_ms
        zset, meta = sessions_zset_key(user_id), sessions_meta_key(user_id)
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.zadd(zset, {str(session.id): expires_at for session in sessions})
            pipe.hset(
                meta,
                mapping={
                    str(session.id): orjson.dumps(
                        {
                            "label": session.label,
                            "connected_at": to_epoch_ms(session.connected_at),
                            "instance": self._instance,
                        }
                    )
                    for session in sessions
                },
            )
            # Nothing renews these keys once the last replica is gone.
            pipe.pexpire(zset, self._ttl_ms * 3)
            pipe.pexpire(meta, self._ttl_ms * 3)
            await pipe.execute()

    async def remove(self, user_id: UUID, session_id: UUID) -> None:
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.zrem(sessions_zset_key(user_id), str(session_id))
            pipe.hdel(sessions_meta_key(user_id), str(session_id))
            await pipe.execute()

    async def list_for(self, user_id: UUID) -> list[SessionInfo]:
        """Every live session of a user, oldest first, dropping expired entries."""
        now = now_ms()
        zset, meta = sessions_zset_key(user_id), sessions_meta_key(user_id)
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.zrangebyscore(zset, "-inf", now)
            pipe.zrangebyscore(zset, f"({now}", "+inf")
            expired, live = cast(tuple[list[bytes], list[bytes]], await pipe.execute())

        if expired:
            async with self._redis.pipeline(transaction=True) as pipe:
                pipe.zrem(zset, *expired)
                pipe.hdel(meta, *expired)
                await pipe.execute()
        if not live:
            return []

        payloads = cast(list[bytes | None], await self._redis.hmget(meta, live))
        sessions = [
            session
            for member, payload in zip(live, payloads, strict=True)
            if (session := _parse(member, payload)) is not None
        ]
        sessions.sort(key=lambda session: session.connected_at)
        return sessions


def _parse(member: bytes, payload: bytes | None) -> SessionInfo | None:
    if payload is None:
        return None
    try:
        meta: dict[str, Any] = orjson.loads(payload)
        return SessionInfo(
            id=UUID(member.decode()),
            label=str(meta["label"]),
            connected_at=from_epoch_ms(int(meta["connected_at"])),
        )
    except (orjson.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        logger.warning(
            "discarding unreadable session entry",
            member=member.decode(errors="replace"),
            error=str(exc),
        )
        return None
