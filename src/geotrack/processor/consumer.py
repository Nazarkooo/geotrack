"""One shard's consumer loop.

The loop is deliberately conservative about acknowledgement: an entry is removed from
the stream only after its batch has been committed and broadcast. Anything that dies
in between leaves the entries pending, and the next owner of the shard picks them up
under the same consumer name.

It is equally deliberate about time. Every call in the loop has a ceiling, because the
failure that matters here is not a Redis that refuses the connection — that one raises
— but a Redis that accepts it and then says nothing. Without a ceiling the shard is
parked forever with its lease still held, and a stop request is never seen, because the
loop can only notice one between two awaits.
"""

import asyncio
import contextlib
from collections.abc import Awaitable, Sequence
from dataclasses import dataclass
from typing import cast

import structlog
from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from geotrack.messaging.codec import CodecError, LocationRecord, decode_record, encode_record
from geotrack.messaging.keys import (
    DLQ_STREAM,
    INGEST_GROUP,
    STREAM_FIELD,
    consumer_name,
    ingest_stream,
)
from geotrack.observability.metrics import processor_dead_letters_total, processor_shard_backlog
from geotrack.processor.batch import BatchProcessor, BatchResult
from geotrack.processor.leases import ShardLeaseManager
from geotrack.processor.publisher import ResultPublisher
from geotrack.settings import Settings

logger = structlog.get_logger(__name__)

DB_BACKOFF_MIN_S = 0.25
DB_BACKOFF_MAX_S = 2.0
# Entries nobody can process must not be able to fill the instance; the dead-letter
# stream keeps the most recent ones as evidence and drops the rest.
DLQ_MAX_LEN = 10_000

# The ceiling on one Redis round trip, matching the command client's socket_timeout. The
# stream client cannot use that timeout itself, because it would abort XREADGROUP BLOCK.
REDIS_OP_TIMEOUT_S = 5.0

# The one reply that means the command will never work on this server. Anything else —
# a group being recreated, a stream just trimmed away — is transient and must not turn
# every later acknowledgement into two round trips for the life of the process.
UNKNOWN_COMMAND = "unknown command"

# The pending entries of a consumer name are read with id "0"; new entries with ">".
PENDING = "0"
NEW = ">"

type StreamEntry = tuple[bytes, dict[bytes, bytes]]


@dataclass(frozen=True, slots=True)
class DeadLetter:
    entry_id: str
    raw: bytes
    error: str


async def bounded[T](awaitable: Awaitable[T], *, seconds: float, what: str) -> T:
    """Wait for one Redis call, and give up as a ``RedisError`` if it never answers.

    redis-py drops a connection whose command was cancelled mid-flight, so the next
    command cannot read this one's reply. Reporting the give-up as a Redis timeout puts
    it through the same handling as any other transport failure.
    """
    try:
        async with asyncio.timeout(seconds):
            return await awaitable
    except TimeoutError as exc:
        raise RedisTimeoutError(f"{what} did not answer within {seconds:g}s") from exc


class ShardConsumer:
    def __init__(
        self,
        shard: int,
        *,
        redis: Redis,
        batch_processor: BatchProcessor,
        publisher: ResultPublisher,
        settings: Settings,
        lease: ShardLeaseManager,
    ) -> None:
        self._shard = shard
        self._redis = redis
        self._batch_processor = batch_processor
        self._publisher = publisher
        self._settings = settings
        self._lease = lease
        self._stream = ingest_stream(shard)
        self._consumer = consumer_name(shard)
        self._stopping = asyncio.Event()
        self._backoff_s = DB_BACKOFF_MIN_S
        self._supports_xackdel = True
        self._log = logger.bind(shard=shard)

    def request_stop(self) -> None:
        """Ask the loop to finish its current batch and return."""
        self._stopping.set()

    async def run(self) -> None:
        await self._create_group_when_ready()
        # Start on the pending list: a lease that has just changed hands usually leaves
        # entries claimed by the previous owner under this same consumer name.
        cursor = PENDING
        while not self._stopping.is_set():
            try:
                entries = await self._read(cursor)
                backlog = await bounded(
                    self._redis.xlen(self._stream), seconds=REDIS_OP_TIMEOUT_S, what="XLEN"
                )
            except ResponseError as exc:
                # The group is gone: the stream was trimmed away or never created.
                self._log.warning("stream read rejected", error=str(exc))
                await self._create_group_when_ready()
                cursor = PENDING
                continue
            except RedisError as exc:
                # Redis is unreachable or has stopped answering. Backing off here rather
                # than letting the task die keeps the shard owned and resumes on its own
                # once Redis returns.
                self._log.warning("stream read failed", error=str(exc))
                cursor = PENDING
                await self._sleep_backoff()
                continue

            processor_shard_backlog.labels(str(self._shard)).set(backlog)
            if not entries:
                cursor = NEW
                continue
            try:
                if not await self._still_leased():
                    break
                await self._handle(entries)
            except Exception:
                # Entries stay pending, so the retry re-reads exactly these ones.
                self._log.exception("batch failed, entries left pending", entries=len(entries))
                cursor = PENDING
                await self._sleep_backoff()
            else:
                self._backoff_s = DB_BACKOFF_MIN_S
        self._log.info("consumer stopped")

    async def _still_leased(self) -> bool:
        """Renew the lease immediately before committing under it.

        The service renews on its own clock, but only the consumer knows when it is
        about to write. Checking here is what makes "stop when the lease is lost" mean
        the next batch rather than up to a third of a lease TTL later, which is how long
        two replicas would otherwise be applying the same shard.
        """
        held = await bounded(
            self._lease.renew(self._shard), seconds=REDIS_OP_TIMEOUT_S, what="lease renewal"
        )
        if not held:
            self._log.warning("shard lease lost, leaving the entries for the new owner")
            self._stopping.set()
        return held

    async def _create_group_when_ready(self) -> None:
        """Keep trying until Redis answers.

        The shard is leased whether or not Redis is reachable, so giving up here would
        leave it owned by a replica that never reads it.
        """
        while not self._stopping.is_set():
            try:
                await self._ensure_group()
            except RedisError as exc:
                self._log.warning("consumer group not ready", error=str(exc))
                await self._sleep_backoff()
            else:
                self._backoff_s = DB_BACKOFF_MIN_S
                return

    async def _ensure_group(self) -> None:
        try:
            await bounded(
                self._redis.xgroup_create(self._stream, INGEST_GROUP, id="0", mkstream=True),
                seconds=REDIS_OP_TIMEOUT_S,
                what="XGROUP CREATE",
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _read(self, cursor: str) -> list[StreamEntry]:
        # Reading the pending list must not block: it is a local lookup that returns
        # immediately, and blocking on it would stall a handover behind idle time.
        block = None if cursor == PENDING else self._settings.processor_block_ms
        # This is the one call that is meant to wait, so its ceiling is what it was told
        # to block for plus the slack every other call gets.
        budget = REDIS_OP_TIMEOUT_S + (block / 1_000 if block else 0.0)
        response = await bounded(
            self._redis.xreadgroup(
                INGEST_GROUP,
                self._consumer,
                {self._stream: cursor},
                count=self._settings.processor_batch_size,
                block=block,
            ),
            seconds=budget,
            what="XREADGROUP",
        )
        if not response:
            return []
        # redis-py returns the untyped protocol shape: one (stream name, entries) pair
        # per stream read, and this consumer only ever reads one stream.
        streams = cast(list[tuple[bytes, list[StreamEntry]]], response)
        entries: list[StreamEntry] = []
        for _, stream_entries in streams:
            entries.extend(stream_entries)
        return entries

    async def _handle(self, entries: Sequence[StreamEntry]) -> None:
        records, sources, dead = self._decode(entries)
        result = (
            await self._batch_processor.apply(self._shard, records)
            if records
            else BatchResult(accepted=[], alerts=[], stale=0, rejected=[])
        )
        await self._publisher.publish(result)

        # Records the schema cannot hold keep their place in the input, so each one can
        # still be traced back to the stream entry it came from.
        dead.extend(
            DeadLetter(
                entry_id=sources[rejected.index],
                raw=encode_record(rejected.record),
                error=rejected.reason,
            )
            for rejected in result.rejected
        )
        if dead:
            await self._dead_letter(dead)
        await self._acknowledge([entry_id for entry_id, _ in entries])

    def _decode(
        self, entries: Sequence[StreamEntry]
    ) -> tuple[list[LocationRecord], list[str], list[DeadLetter]]:
        records: list[LocationRecord] = []
        sources: list[str] = []
        dead: list[DeadLetter] = []
        for entry_id, fields in entries:
            raw = fields.get(STREAM_FIELD, b"")
            try:
                record = decode_record(raw)
            except CodecError as exc:
                dead.append(DeadLetter(entry_id=entry_id.decode(), raw=raw, error=str(exc)))
            else:
                records.append(record)
                sources.append(entry_id.decode())
        return records, sources, dead

    async def _dead_letter(self, dead: Sequence[DeadLetter]) -> None:
        async with self._redis.pipeline(transaction=False) as pipe:
            for entry in dead:
                pipe.xadd(
                    DLQ_STREAM,
                    {
                        "shard": self._shard,
                        "id": entry.entry_id,
                        "raw": entry.raw,
                        "error": entry.error,
                    },
                    maxlen=DLQ_MAX_LEN,
                    approximate=True,
                )
            await bounded(pipe.execute(), seconds=REDIS_OP_TIMEOUT_S, what="dead-lettering")
        processor_dead_letters_total.inc(len(dead))
        self._log.warning("entries dead-lettered", count=len(dead), error=dead[0].error)

    async def _acknowledge(self, entry_ids: Sequence[bytes]) -> None:
        """Acknowledge and drop the entries, so stream length stays the real backlog."""
        if self._supports_xackdel:
            try:
                await bounded(
                    self._redis.xackdel(
                        self._stream, INGEST_GROUP, *entry_ids, ref_policy="DELREF"
                    ),
                    seconds=REDIS_OP_TIMEOUT_S,
                    what="XACKDEL",
                )
                return
            except ResponseError as exc:
                if UNKNOWN_COMMAND not in str(exc).lower():
                    raise
                self._supports_xackdel = False
                self._log.warning(
                    "xackdel unavailable, falling back to xack and xdel", error=str(exc)
                )
        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.xack(self._stream, INGEST_GROUP, *entry_ids)
            pipe.xdel(self._stream, *entry_ids)
            await bounded(pipe.execute(), seconds=REDIS_OP_TIMEOUT_S, what="XACK and XDEL")

    async def _sleep_backoff(self) -> None:
        delay = self._backoff_s
        self._backoff_s = min(self._backoff_s * 2, DB_BACKOFF_MAX_S)
        # Waiting on the stop event rather than sleeping keeps shutdown responsive while
        # a dependency is down.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), timeout=delay)
