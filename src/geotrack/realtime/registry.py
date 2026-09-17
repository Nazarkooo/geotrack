"""Which connections this replica holds, and for whom.

A user's alerts and zone events travel on their own pub/sub channel. The registry
reports when a user gains their first local session and loses their last, so this
replica subscribes to exactly the channels it can deliver — not to every user in the
system.

Admission and the subscription that follows it are deliberately two steps. Membership
is decided under one short lock that is never held across an await; the hook that
talks to Redis then runs outside it, serialised per user. One slow round trip for one
user therefore costs that user, not every other socket connecting or leaving on the
replica at the same time.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from uuid import UUID

from geotrack.observability.metrics import ws_connections
from geotrack.realtime.connection import ClientConnection

type UserHook = Callable[[UUID], Awaitable[None]]


@dataclass(slots=True)
class _Sync:
    """Serialises the subscription hooks of one user, and outlives none of them."""

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    callers: int = 0


class ConnectionRegistry:
    def __init__(
        self, *, on_first_user_connection: UserHook, on_last_user_disconnect: UserHook
    ) -> None:
        self._by_user: dict[UUID, set[ClientConnection]] = {}
        self._subscribed: set[UUID] = set()
        self._on_first = on_first_user_connection
        self._on_last = on_last_user_disconnect
        self._admission = asyncio.Lock()
        self._syncs: dict[UUID, _Sync] = {}

    async def add(self, connection: ClientConnection, *, limit: int) -> bool:
        """Admit a connection unless the user is already at their session limit.

        Counting and admitting happen under the same lock: sockets that arrive
        together must not each see a count taken before any of them was added. The
        subscription is in place before this returns, so a client that gets its hello
        is one whose alerts are already routed here.
        """
        async with self._admission:
            connections = self._by_user.get(connection.user_id, set())
            if len(connections) >= limit:
                return False
            connections.add(connection)
            self._by_user[connection.user_id] = connections
            ws_connections.labels("client").inc()
        await self._sync_user(connection.user_id)
        return True

    async def remove(self, connection: ClientConnection) -> None:
        async with self._admission:
            connections = self._by_user.get(connection.user_id)
            if connections is None or connection not in connections:
                return
            connections.remove(connection)
            ws_connections.labels("client").dec()
            if not connections:
                del self._by_user[connection.user_id]
        await self._sync_user(connection.user_id)

    def for_user(self, user_id: UUID) -> tuple[ClientConnection, ...]:
        """A snapshot, so callers can iterate while connections come and go."""
        return tuple(self._by_user.get(user_id, ()))

    def all(self) -> tuple[ClientConnection, ...]:
        return tuple(
            connection for connections in self._by_user.values() for connection in connections
        )

    def count(self) -> int:
        return sum(len(connections) for connections in self._by_user.values())

    async def _sync_user(self, user_id: UUID) -> None:
        """Make the subscription match the sessions this replica holds for a user.

        The membership is re-read after every await rather than trusted from before
        it, so a connect racing the disconnect of the same user always settles on
        whichever of them the registry ended up holding.
        """
        sync = self._syncs.get(user_id)
        if sync is None:
            sync = self._syncs[user_id] = _Sync()
        sync.callers += 1
        try:
            async with sync.lock:
                while (wanted := user_id in self._by_user) != (user_id in self._subscribed):
                    if wanted:
                        await self._on_first(user_id)
                        self._subscribed.add(user_id)
                    else:
                        await self._on_last(user_id)
                        self._subscribed.discard(user_id)
        finally:
            sync.callers -= 1
            if not sync.callers:
                del self._syncs[user_id]
