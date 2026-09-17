"""Cross-replica fan-out.

A user's sessions can land on any replica, and the processor that produced an alert
knows nothing about websockets. Everything therefore travels over Redis pub/sub: one
channel for positions (every gateway wants them) and one channel per user for alerts,
zone changes and session lists. This bridge is the single reader of that traffic.

Two rules keep a replica delivering through a bad day. The reader never dies: one
message it cannot handle is dropped and counted, because a single malformed payload
must not cost every client on the replica every later update. And the subscription
set is a desired state the reader keeps converging on, not a command whose failure is
only logged — a subscribe that fails would otherwise leave a user connected, healthy
and silently without alerts for the whole session.

The handlers it calls are plain functions that only enqueue work, never await a
socket: one unresponsive client must not be able to stall the fan-out for everyone.
"""

import asyncio
from collections.abc import Callable, Sequence
from contextlib import suppress
from uuid import UUID

import structlog
from redis.asyncio import Redis
from redis.exceptions import RedisError

from geotrack.messaging.codec import PositionItem, decode_positions
from geotrack.messaging.keys import POSITIONS_CHANNEL, user_channel, user_id_from_channel

logger = structlog.get_logger(__name__)

# Short enough that redis-py keeps running its connection health checks between
# messages, long enough that an idle gateway is not spinning.
_POLL_TIMEOUT_S = 1.0
_RETRY_DELAY_S = 0.5
# The pub/sub client deliberately has no socket timeout, so an idle read is never
# aborted. A subscribe must not inherit that: against a blackholed connection it
# would wait for the OS to give up, holding back the client that asked for it.
_COMMAND_TIMEOUT_S = 5.0

_POSITIONS_CHANNEL_BYTES = POSITIONS_CHANNEL.encode()

type PositionsHandler = Callable[[Sequence[PositionItem]], None]
type UserFrameHandler = Callable[[UUID, bytes], None]


class RedisBridge:
    def __init__(
        self,
        redis: Redis,
        *,
        on_positions: PositionsHandler,
        on_user_frame: UserFrameHandler,
    ) -> None:
        self._pubsub = redis.pubsub()
        self._on_positions = on_positions
        self._on_user_frame = on_user_frame
        # What this replica should be subscribed to, and what Redis accepted. The
        # two only differ while a subscription change is failing.
        self._wanted: set[str] = {POSITIONS_CHANNEL}
        self._held: set[str] = set()
        # Subscription changes are serialised so a reconnect cannot interleave with a
        # client connecting and leave the server subscribed to the wrong set.
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._running = False
        self._dropped = 0

    @property
    def dropped_messages(self) -> int:
        """Messages that could not be delivered; a growing count means a bad producer."""
        return self._dropped

    @property
    def in_sync(self) -> bool:
        """Whether Redis holds exactly the channels this replica needs."""
        return self._held == self._wanted

    @property
    def delivering(self) -> bool:
        """Whether this replica can still receive fan-out at all.

        A reader that stopped means every client here is deaf, which readiness has to
        report: a replica in that state must not keep taking new sessions.
        """
        return self._running and self._task is not None and not self._task.done()

    async def start(self) -> None:
        self._running = True
        # A failed first subscribe is not fatal either: the reader repairs it.
        await self._sync()
        self._task = asyncio.create_task(self._read(), name="realtime-bridge")

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            # A reader that already died takes its exception to the grave here: stopping
            # is not the moment to fail, and the cause was logged when it happened.
            with suppress(asyncio.CancelledError, RedisError, OSError, RuntimeError):
                await self._task
            self._task = None
        # redis-py leaves PubSub.aclose unannotated.
        with suppress(RedisError, OSError, RuntimeError):
            await self._pubsub.aclose()  # type: ignore[no-untyped-call]

    async def subscribe_user(self, user_id: UUID) -> None:
        """Start delivering a user's frames; retried by the reader if Redis refuses."""
        async with self._lock:
            self._wanted.add(user_channel(user_id))
            await self._apply()

    async def unsubscribe_user(self, user_id: UUID) -> None:
        async with self._lock:
            self._wanted.discard(user_channel(user_id))
            await self._apply()

    async def _read(self) -> None:
        loop = asyncio.get_running_loop()
        retry_at = 0.0
        while self._running:
            try:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=_POLL_TIMEOUT_S
                )
            except Exception as exc:
                # RedisError and OSError are the expected shapes; redis-py also raises a
                # bare RuntimeError when the first subscribe never landed, and that one
                # used to end the reader and leave the replica silently without updates.
                logger.warning("pub/sub read failed", error=f"{type(exc).__name__}: {exc}")
                # The connection that held the subscriptions is gone with them.
                self._held.clear()
                await asyncio.sleep(_RETRY_DELAY_S)
                await self._sync()
                continue
            if message is not None:
                self._deliver(message["channel"], message["data"])
            # Whatever a subscription change could not apply is repaired here, so a
            # user whose SUBSCRIBE failed starts receiving their alerts by themselves.
            if not self.in_sync and (now := loop.time()) >= retry_at:
                retry_at = now + _RETRY_DELAY_S
                if await self._sync():
                    logger.info("pub/sub subscriptions repaired", channels=len(self._held))

    async def _sync(self) -> bool:
        async with self._lock:
            return await self._apply()

    async def _apply(self) -> bool:
        """Make Redis hold exactly ``_wanted``; ``True`` once it does.

        The caller holds the lock. Whatever does not land stays in the gap between
        ``_wanted`` and ``_held``, which is the reader's cue to try again.
        """
        stale, missing = self._held - self._wanted, self._wanted - self._held
        if not stale and not missing:
            return True
        try:
            async with asyncio.timeout(_COMMAND_TIMEOUT_S):
                if stale:
                    await self._pubsub.unsubscribe(*sorted(stale))
                    self._held -= stale
                if missing:
                    await self._pubsub.subscribe(*sorted(missing))
                    self._held |= missing
        except (RedisError, OSError) as exc:
            # A timeout arrives here too: TimeoutError is an OSError.
            logger.warning(
                "pub/sub subscription not applied",
                error=str(exc),
                pending=len(self._wanted ^ self._held),
            )
        else:
            logger.debug("pub/sub subscriptions applied", channels=len(self._held))
        return self.in_sync

    def _deliver(self, channel: bytes, data: bytes) -> None:
        """Hand one message to its handler, containing whatever it raises.

        Dropping a message the fan-out cannot use costs one client one update; letting
        it end the reader costs every client on this replica every update after it.
        """
        try:
            self._dispatch(channel, data)
        except Exception as exc:
            self._dropped += 1
            logger.warning(
                "dropping an undeliverable pub/sub message",
                channel=channel.decode(errors="replace"),
                error=f"{type(exc).__name__}: {exc}",
            )

    def _dispatch(self, channel: bytes, data: bytes) -> None:
        if channel == _POSITIONS_CHANNEL_BYTES:
            self._on_positions(decode_positions(data))
            return

        user_id = user_id_from_channel(channel.decode())
        # User frames are relayed verbatim, so the one thing worth checking is that
        # they can be written to a socket at all: a frame that is not UTF-8 would
        # otherwise take down the sender of every session of that user. The
        # UnicodeDecodeError is the caller's to count.
        data.decode()
        self._on_user_frame(user_id, data)
