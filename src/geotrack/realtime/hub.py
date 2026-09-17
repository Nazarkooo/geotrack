"""The gateway's in-memory picture of where every device is right now.

Every replica keeps the full picture: positions are public to all users, so there is
nothing to filter per user, and a shared picture means a client that pans or
reconnects is answered from memory instead of from PostgreSQL.

The hub serialises each changed cell exactly once per tick. With 10,000 devices and
hundreds of clients that is the difference between one encode per device and one
encode per device per client.
"""

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from geotrack.clock import from_epoch_ms, now_ms, to_epoch_ms
from geotrack.messaging.codec import PositionItem
from geotrack.observability.metrics import hub_devices, hub_updates_total
from geotrack.realtime.grid import Cell, CellRect, cell_of, covers
from geotrack.realtime.protocol import encode_position_item

logger = structlog.get_logger(__name__)

# Rows are applied in batches so a cold start never builds one huge list.
_WARM_START_CHUNK = 2_000

_WARM_START_SQL = text(
    """
    SELECT device_id,
           ST_Y(position::geometry) AS latitude,
           ST_X(position::geometry) AS longitude,
           reported_at
    FROM device_positions
    WHERE reported_at >= :cutoff
    """
)


@dataclass(frozen=True, slots=True)
class Departure:
    """A device that left a cell, and where it went (``None`` means it is gone)."""

    device_id: str
    moved_to: Cell | None


@dataclass(frozen=True, slots=True)
class TickDelta:
    t_ms: int
    chunks: Mapping[Cell, bytes]
    departures: Mapping[Cell, tuple[Departure, ...]]

    @property
    def is_empty(self) -> bool:
        return not self.chunks and not self.departures

    @property
    def removed(self) -> tuple[str, ...]:
        """Devices that disappeared entirely, regardless of which cell held them."""
        return tuple(
            departure.device_id
            for departures in self.departures.values()
            for departure in departures
            if departure.moved_to is None
        )


@dataclass(slots=True)
class _Entry:
    item: PositionItem
    cell: Cell

    @property
    def reported_ms(self) -> int:
        return self.item[3]


@dataclass(slots=True)
class _Pending:
    """What changed since the last drain, grouped the way clients consume it."""

    changed: dict[Cell, set[str]] = field(default_factory=dict)
    departed: dict[Cell, dict[str, Cell | None]] = field(default_factory=dict)

    def clear(self) -> None:
        self.changed.clear()
        self.departed.clear()


class PositionHub:
    def __init__(self, *, cell_size_deg: float, stale_after_s: int) -> None:
        self._cell_size_deg = cell_size_deg
        self._stale_after_ms = stale_after_s * 1_000
        self._latest: dict[str, _Entry] = {}
        self._cells: dict[Cell, set[str]] = {}
        self._pending = _Pending()
        self._snapshots: dict[Cell, bytes] = {}
        self._updates = 0

    @property
    def device_count(self) -> int:
        return len(self._latest)

    @property
    def updates(self) -> int:
        """Total position updates accepted; the stats frame reports its rate."""
        return self._updates

    def apply(self, items: Sequence[PositionItem]) -> None:
        applied = 0
        for item in items:
            device_id, lat, lon, reported_ms = item
            entry = self._latest.get(device_id)
            # Batches from different shards can interleave; the newest report wins.
            if entry is not None and reported_ms <= entry.reported_ms:
                continue
            cell = cell_of(lat, lon, self._cell_size_deg)
            if entry is None:
                self._latest[device_id] = _Entry(item=item, cell=cell)
            else:
                if entry.cell != cell:
                    self._leave(entry.cell, device_id, moved_to=cell)
                    entry.cell = cell
                entry.item = item
            self._cells.setdefault(cell, set()).add(device_id)
            self._touch(cell, device_id)
            applied += 1

        if applied:
            self._updates += applied
            hub_updates_total.inc(applied)
            hub_devices.set(len(self._latest))

    def drain(self, *, t_ms: int) -> TickDelta:
        chunks = {
            cell: b",".join(encode_position_item(self._latest[device].item) for device in devices)
            for cell, devices in self._pending.changed.items()
            if devices
        }
        departures = {
            cell: tuple(Departure(device, moved_to) for device, moved_to in moves.items())
            for cell, moves in self._pending.departed.items()
            if moves
        }
        self._pending.clear()
        return TickDelta(t_ms=t_ms, chunks=chunks, departures=departures)

    def discard_pending(self) -> None:
        """Forget this tick's changes instead of serialising them.

        Only the delta is dropped. ``apply`` keeps the picture itself current, so the
        next client to send a viewport is answered from live state either way.
        """
        self._pending.clear()

    def snapshot_chunks(self, rects: Sequence[CellRect]) -> Iterator[bytes]:
        """Current contents of every visible cell, cached so resyncing clients share it.

        Consume this eagerly: it reads live state and must not be held across an await.
        """
        for cell, devices in self._cells.items():
            if not covers(rects, cell):
                continue
            chunk = self._snapshots.get(cell)
            if chunk is None:
                chunk = b",".join(encode_position_item(self._latest[d].item) for d in devices)
                self._snapshots[cell] = chunk
            yield chunk

    def sweep(self, now: int) -> None:
        """Forget devices that stopped reporting, so the map does not keep ghosts."""
        cutoff = now - self._stale_after_ms
        stale = [device for device, entry in self._latest.items() if entry.reported_ms < cutoff]
        for device in stale:
            entry = self._latest.pop(device)
            self._leave(entry.cell, device, moved_to=None)
        if stale:
            hub_devices.set(len(self._latest))
            logger.debug("swept stale devices", count=len(stale))

    async def warm_start(self, session: AsyncSession) -> int:
        """Load recent positions so a fresh replica has state before the first client.

        The session is released as soon as this returns; the hub never holds a
        connection while clients are attached.
        """
        cutoff = from_epoch_ms(now_ms() - self._stale_after_ms)
        result = await session.stream(_WARM_START_SQL, {"cutoff": cutoff})
        loaded = 0
        async for rows in result.partitions(_WARM_START_CHUNK):
            self.apply(
                [
                    (row.device_id, row.latitude, row.longitude, to_epoch_ms(row.reported_at))
                    for row in rows
                ]
            )
            loaded += len(rows)
        # No client is attached yet, so this is initial state rather than a delta.
        self._pending.clear()
        return loaded

    def _touch(self, cell: Cell, device_id: str) -> None:
        self._pending.changed.setdefault(cell, set()).add(device_id)
        # A device that came back into a cell within the same tick is not a departure.
        moves = self._pending.departed.get(cell)
        if moves is not None:
            moves.pop(device_id, None)
        self._snapshots.pop(cell, None)

    def _leave(self, cell: Cell, device_id: str, *, moved_to: Cell | None) -> None:
        devices = self._cells.get(cell)
        if devices is not None:
            devices.discard(device_id)
            if not devices:
                del self._cells[cell]
        changed = self._pending.changed.get(cell)
        if changed is not None:
            changed.discard(device_id)
        self._pending.departed.setdefault(cell, {})[device_id] = moved_to
        self._snapshots.pop(cell, None)
