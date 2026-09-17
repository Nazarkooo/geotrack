"""The single statement that turns a batch of reports into committed state.

Everything a batch has to decide — which reports are new enough to store, which zones
they fall into, which of those are transitions, which transitions deserve an alert —
happens inside one set-based statement, in one transaction, on one connection. That is
what keeps a 10,000-device fleet inside a handful of database connections: the cost of
a batch is one round trip, not one per report.
"""

import asyncio
import random
import re
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import structlog
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from geotrack.clock import now_ms
from geotrack.db.errors import RETRYABLE_SQLSTATES, sqlstate_of
from geotrack.db.models import AlertKind
from geotrack.messaging.codec import LocationRecord, PositionItem
from geotrack.observability.metrics import (
    processor_alerts_total,
    processor_batch_seconds,
    processor_batch_size,
    processor_batches_total,
    processor_end_to_end_seconds,
    processor_reports_total,
    processor_retries_total,
    processor_stale_reports_total,
)
from geotrack.settings import Settings

logger = structlog.get_logger(__name__)

MAX_ATTEMPTS = 5
RETRY_BASE_DELAY_S = 0.025

# Mirrors ck_device_positions_device_id_length in migration 0001 and the DeviceId shape
# the REST models and the client frames share. A row the database would take but no alert
# frame could carry is worse than a rejected one: by the time the frame is built the row
# is committed and can no longer be refused. A unit test pins this against the model.
MIN_DEVICE_ID_LENGTH = 1
MAX_DEVICE_ID_LENGTH = 64
DEVICE_ID_RE = re.compile(rf"[A-Za-z0-9._:-]{{{MIN_DEVICE_ID_LENGTH},{MAX_DEVICE_ID_LENGTH}}}")

# Waiting for the shard lock is the one thing in a batch that is meant to take time, so
# losing that race is a reason to come back rather than to fail. 55P03 can only reach this
# table through the lock below; db/errors.py stays the shared data layer's own map.
LOCK_NOT_AVAILABLE = "55P03"
BATCH_RETRYABLE_SQLSTATES = RETRYABLE_SQLSTATES | {LOCK_NOT_AVAILABLE}

# How long a batch may wait for the owner ahead of it. That owner is itself bounded by the
# engine's statement_timeout on each of its three statements, so a multiple of that is the
# longest the lock can legitimately be held; past it the other side is wedged and backing
# off beats waiting.
LOCK_WAIT_STATEMENTS = 3

# Two processors can briefly own the same shard while a lease changes hands. Taking a
# per-shard transaction lock makes their batches run one after the other, so the second
# one sees the first one's positions and correctly treats its own reports as stale
# instead of replaying transitions that already happened.
#
# The wait needs a budget of its own. Under the engine's statement_timeout the lock — the
# one statement built to wait — would be the first thing to break under exactly the load
# it exists for. statement_timeout and lock_timeout are both armed when a statement
# begins, so the budget has to be installed by a statement of its own; both are set
# transaction-locally, so the connection goes back to the pool untouched.
BEGIN_LOCK_WAIT_SQL = text(
    """
    SELECT set_config('lock_timeout', :wait, true),
           set_config('statement_timeout', '0', true)
    """
)
LOCK_SHARD_SQL = text(
    "SELECT pg_advisory_xact_lock(hashtext('geotrack.shard'), CAST(:shard AS int))"
)
# Only statement_timeout is handed back: lock_timeout stays armed for the rest of the
# transaction on purpose, so that a row-lock conflict in the statements below also comes
# back as a retry instead of an unbounded wait.
END_LOCK_WAIT_SQL = text("SELECT set_config('statement_timeout', :statement, true)")

# Every report is archived, including the ones the main statement discards as stale:
# the track of a device is the raw stream, not the accepted subset. The primary key
# makes a replayed batch a no-op.
INSERT_HISTORY_SQL = text(
    """
    INSERT INTO location_history (device_id, position, reported_at, received_at)
    SELECT device_id,
           ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography,
           to_timestamp(reported_ms / 1000.0),
           to_timestamp(received_ms / 1000.0)
    FROM unnest(
             CAST(:device_ids AS text[]),
             CAST(:lats AS float8[]),
             CAST(:lons AS float8[]),
             CAST(:reported_ms AS bigint[]),
             CAST(:received_ms AS bigint[])
         ) AS t(device_id, lat, lon, reported_ms, received_ms)
    ON CONFLICT DO NOTHING
    """
)

APPLY_BATCH_SQL = text(
    """
    WITH input AS (
        SELECT device_id,
               ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography AS position,
               to_timestamp(reported_ms / 1000.0) AS reported_at,
               to_timestamp(received_ms / 1000.0) AS received_at
        FROM unnest(
                 CAST(:device_ids AS text[]),
                 CAST(:lats AS float8[]),
                 CAST(:lons AS float8[]),
                 CAST(:reported_ms AS bigint[]),
                 CAST(:received_ms AS bigint[])
             ) AS t(device_id, lat, lon, reported_ms, received_ms)
    ),

    -- The conditional upsert is the stale-report guard. A report that is not newer than
    -- the stored one updates no row, so it never reaches RETURNING and can take no part
    -- in the transitions below. Devices are unique in `input` (see latest_per_device),
    -- which ON CONFLICT DO UPDATE requires.
    accepted AS (
        INSERT INTO device_positions AS dp
            (device_id, position, reported_at, received_at, updated_at)
        SELECT device_id, position, reported_at, received_at, now() FROM input
        ON CONFLICT (device_id) DO UPDATE
            SET position = EXCLUDED.position,
                reported_at = EXCLUDED.reported_at,
                received_at = EXCLUDED.received_at,
                updated_at = now()
            WHERE dp.reported_at < EXCLUDED.reported_at
        RETURNING dp.device_id, dp.position, dp.reported_at
    ),

    -- `&&` on the stored buffer polygon is the index-assisted candidate filter (a radius
    -- that lives in the row cannot drive a GiST scan on its own); ST_DWithin on the
    -- geography centre is the exact geodesic test. See migration 0001.
    hits AS (
        SELECT a.device_id, a.position, a.reported_at,
               z.id AS zone_id, z.user_id, z.name AS zone_name,
               z.alert_on_enter, z.alert_on_exit, z.dwell_alert_interval_s
        FROM accepted a
        JOIN geozones z
          ON z.search_area && a.position
         AND ST_DWithin(z.center, a.position, z.radius_m)
    ),

    -- One row per (zone, device) pair this batch has something to say about: inside and
    -- new (`enter`), inside and known (`stay`), or known but no longer inside (`exit`).
    -- Exits are found from the presence table, because a device that left a zone by
    -- definition produces no hit for it.
    transitions AS (
        SELECT h.zone_id, h.device_id, h.user_id, h.zone_name, h.position, h.reported_at,
               h.alert_on_enter, h.alert_on_exit, h.dwell_alert_interval_s,
               p.last_alert_at,
               CASE WHEN p.device_id IS NULL THEN 'enter' ELSE 'stay' END AS state
        FROM hits h
        LEFT JOIN zone_presence p
               ON p.zone_id = h.zone_id AND p.device_id = h.device_id
        UNION ALL
        SELECT p.zone_id, p.device_id, z.user_id, z.name, a.position, a.reported_at,
               z.alert_on_enter, z.alert_on_exit, z.dwell_alert_interval_s,
               p.last_alert_at, 'exit'
        FROM accepted a
        JOIN zone_presence p ON p.device_id = a.device_id
        JOIN geozones z ON z.id = p.zone_id
        WHERE NOT EXISTS (
            SELECT 1 FROM hits h
            WHERE h.zone_id = p.zone_id AND h.device_id = p.device_id
        )
    ),

    -- Which transitions the zone's owner asked to hear about. `dwell` fires at most once
    -- per interval while a device keeps reporting from inside.
    events AS (
        SELECT t.*,
               CASE
                   WHEN t.state = 'enter' AND t.alert_on_enter THEN 'enter'
                   WHEN t.state = 'exit' AND t.alert_on_exit THEN 'exit'
                   WHEN t.state = 'stay'
                        AND t.dwell_alert_interval_s IS NOT NULL
                        AND t.reported_at >= t.last_alert_at
                                             + make_interval(secs => t.dwell_alert_interval_s)
                       THEN 'dwell'
               END::alert_kind AS kind
        FROM transitions t
    ),

    presence_upsert AS (
        INSERT INTO zone_presence AS zp
            (zone_id, device_id, entered_at, last_seen_at, last_alert_at)
        SELECT zone_id, device_id, reported_at, reported_at,
               -- Entering starts the dwell clock and a dwell alert restarts it, so that
               -- a silent zone (both toggles off) still paces its dwell reminders.
               COALESCE(
                   CASE WHEN kind = 'dwell' THEN reported_at ELSE last_alert_at END,
                   reported_at
               )
        FROM events
        WHERE state <> 'exit'
        ON CONFLICT (zone_id, device_id) DO UPDATE
            SET last_seen_at = EXCLUDED.last_seen_at,
                last_alert_at = EXCLUDED.last_alert_at
    ),

    presence_delete AS (
        DELETE FROM zone_presence zp
        USING events e
        WHERE e.state = 'exit' AND zp.zone_id = e.zone_id AND zp.device_id = e.device_id
    ),

    -- ORDER BY makes the generated identities follow event time, so the keyset pagination
    -- of GET /alerts and the live feed agree on the order of one batch.
    new_alerts AS (
        INSERT INTO alerts (user_id, zone_id, zone_name, device_id, kind, position, occurred_at)
        SELECT user_id, zone_id, zone_name, device_id, kind, position, reported_at
        FROM events
        WHERE kind IS NOT NULL
        ORDER BY reported_at, zone_id, device_id
        RETURNING id, user_id, zone_id, zone_name, device_id, kind, position,
                  occurred_at, created_at
    )

    -- One result set carries both halves of the outcome: the positions to broadcast and
    -- the alerts to route. Splitting them into two statements would need a second round
    -- trip and could not see the data-modifying CTEs above.
    SELECT 'position' AS row_kind,
           a.device_id,
           ST_Y(a.position::geometry) AS latitude,
           ST_X(a.position::geometry) AS longitude,
           a.reported_at AS occurred_at,
           NULL::bigint AS alert_id,
           NULL::uuid AS user_id,
           NULL::uuid AS zone_id,
           NULL::text AS zone_name,
           NULL::alert_kind AS kind,
           NULL::timestamptz AS created_at
    FROM accepted a
    UNION ALL
    SELECT 'alert',
           n.device_id,
           ST_Y(n.position::geometry),
           ST_X(n.position::geometry),
           n.occurred_at,
           n.id, n.user_id, n.zone_id, n.zone_name, n.kind, n.created_at
    FROM new_alerts n
    """
)


@dataclass(frozen=True, slots=True)
class AlertRow:
    """A committed alert, carrying everything the publisher needs to build a frame."""

    id: int
    user_id: UUID
    zone_id: UUID | None
    zone_name: str
    device_id: str
    kind: AlertKind
    latitude: float
    longitude: float
    occurred_at: datetime
    created_at: datetime


@dataclass(frozen=True, slots=True)
class RejectedRecord:
    """A report the schema cannot hold, with its position in the input batch."""

    index: int
    record: LocationRecord
    reason: str


@dataclass(frozen=True, slots=True)
class BatchResult:
    accepted: list[PositionItem]
    alerts: list[AlertRow]
    stale: int
    # Reports that would raise a constraint or partition-routing error no retry can fix.
    # They are handed back rather than applied, so that one bad entry dead-letters
    # instead of wedging its shard forever.
    rejected: list[RejectedRecord]

    @property
    def is_empty(self) -> bool:
        return not self.accepted and not self.alerts


def latest_per_device(records: Iterable[LocationRecord]) -> list[LocationRecord]:
    """Keep the newest report per device, in first-appearance order.

    ``ON CONFLICT DO UPDATE`` refuses to touch the same row twice in one statement, and
    a device reporting twice inside one 1,000-entry batch is ordinary under backlog.
    """
    newest: dict[str, LocationRecord] = {}
    for record in records:
        current = newest.get(record.device_id)
        if current is None or record.reported_ms > current.reported_ms:
            newest[record.device_id] = record
    return list(newest.values())


async def take_shard_lock(
    session: AsyncSession, shard: int, *, wait_ms: int, statement_ms: int
) -> None:
    """Serialise this shard against its other owner, then hand the budget back.

    The budget is handed back only on success: a lock that ran out of time aborts the
    transaction, and the rollback takes the transaction-local settings with it, so
    trying to restore them there would replace the real error with ``25P02``.
    """
    await session.execute(BEGIN_LOCK_WAIT_SQL, {"wait": f"{wait_ms}ms"})
    await session.execute(LOCK_SHARD_SQL, {"shard": shard})
    await session.execute(END_LOCK_WAIT_SQL, {"statement": f"{statement_ms}ms"})


def should_retry(sqlstate: str | None, attempt: int, *, max_attempts: int = MAX_ATTEMPTS) -> bool:
    """Only conflicts that a fresh read, or another turn, can resolve are worth repeating."""
    return sqlstate in BATCH_RETRYABLE_SQLSTATES and attempt < max_attempts


def retry_delay(attempt: int) -> float:
    """Exponential backoff with equal jitter: half the window is fixed, half is random."""
    ceiling = RETRY_BASE_DELAY_S * 2 ** (attempt - 1)
    return random.uniform(ceiling / 2, ceiling)  # noqa: S311 - backoff spread, not a secret


def _unstorable_reason(record: LocationRecord, *, oldest: int, newest: int) -> str | None:
    """Why this report cannot be kept, or ``None`` when it can."""
    if not DEVICE_ID_RE.fullmatch(record.device_id):
        return (
            f"device_id must be between {MIN_DEVICE_ID_LENGTH} and {MAX_DEVICE_ID_LENGTH} "
            "characters long and made of A-Z a-z 0-9 . _ : -"
        )
    if record.reported_ms < oldest:
        return "report timestamp is older than the history retention window"
    if record.reported_ms > newest:
        return "report timestamp is too far in the future"
    return None


def statement_params(records: Sequence[LocationRecord]) -> dict[str, Any]:
    """Bind one array per column rather than one parameter set per report.

    A thousand reports then travel as five arrays, which is also what lets the
    whole batch be one statement.
    """
    return {
        "device_ids": [r.device_id for r in records],
        "lats": [r.lat for r in records],
        "lons": [r.lon for r in records],
        "reported_ms": [r.reported_ms for r in records],
        "received_ms": [r.received_ms for r in records],
    }


class BatchProcessor:
    """Applies one shard's batch of reports in a single transaction."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        settings: Settings,
        max_attempts: int = MAX_ATTEMPTS,
        lock_wait_ms: int | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._settings = settings
        self._max_attempts = max_attempts
        self._statement_ms = settings.db_statement_timeout_ms
        self._lock_wait_ms = lock_wait_ms or LOCK_WAIT_STATEMENTS * self._statement_ms

    async def apply(self, shard: int, records: Sequence[LocationRecord]) -> BatchResult:
        storable, rejected = self._split_storable(records)
        if not storable:
            return BatchResult(accepted=[], alerts=[], stale=0, rejected=rejected)

        deduped = latest_per_device(storable)
        started = time.perf_counter()
        attempt = 0
        while True:
            try:
                accepted, alerts = await self._apply_once(shard, storable, deduped)
            except DBAPIError as exc:
                attempt += 1
                sqlstate = sqlstate_of(exc)
                if not should_retry(sqlstate, attempt, max_attempts=self._max_attempts):
                    raise
                processor_retries_total.labels(sqlstate).inc()
                logger.warning("retrying batch", shard=shard, sqlstate=sqlstate, attempt=attempt)
                await asyncio.sleep(retry_delay(attempt))
            else:
                break

        # Everything the batch could have stored but did not: a report the database
        # refused as older than the stored one, and a report this same batch superseded
        # with a newer one for the same device. Both are "ignored because a newer one was
        # stored", which is what processor_stale_reports_total counts, and in-batch
        # duplicates only appear under the backlog where the metric earns its keep.
        stale = len(storable) - len(accepted)
        self._observe(storable, accepted=accepted, alerts=alerts, stale=stale, started=started)
        return BatchResult(accepted=accepted, alerts=alerts, stale=stale, rejected=rejected)

    async def _apply_once(
        self, shard: int, storable: Sequence[LocationRecord], deduped: Sequence[LocationRecord]
    ) -> tuple[list[PositionItem], list[AlertRow]]:
        async with self._session_factory() as session, session.begin():
            await take_shard_lock(
                session, shard, wait_ms=self._lock_wait_ms, statement_ms=self._statement_ms
            )
            await session.execute(INSERT_HISTORY_SQL, statement_params(storable))
            rows = (await session.execute(APPLY_BATCH_SQL, statement_params(deduped))).all()

        accepted: list[PositionItem] = []
        alerts: list[AlertRow] = []
        for row in rows:
            if row.row_kind == "position":
                accepted.append(
                    (
                        row.device_id,
                        row.latitude,
                        row.longitude,
                        int(row.occurred_at.timestamp() * 1000),
                    )
                )
            else:
                alerts.append(
                    AlertRow(
                        id=row.alert_id,
                        user_id=row.user_id,
                        zone_id=row.zone_id,
                        zone_name=row.zone_name,
                        device_id=row.device_id,
                        kind=AlertKind(row.kind),
                        latitude=row.latitude,
                        longitude=row.longitude,
                        occurred_at=row.occurred_at,
                        created_at=row.created_at,
                    )
                )
        return accepted, alerts

    def _split_storable(
        self, records: Sequence[LocationRecord]
    ) -> tuple[list[LocationRecord], list[RejectedRecord]]:
        """Separate reports the schema can hold from ones it cannot.

        The ingest endpoints already enforce both rules, so anything caught here is a
        contract violation upstream. Catching it a second time matters because these
        failures are check violations that no retry can fix: the batch would be retried
        for as long as the entry stays in the stream, and the shard would never move on.
        """
        now = now_ms()
        oldest = now - int(
            timedelta(days=self._settings.history_retention_days).total_seconds() * 1000
        )
        newest = now + self._settings.ingest_max_future_skew_s * 1000
        storable: list[LocationRecord] = []
        rejected: list[RejectedRecord] = []
        for index, record in enumerate(records):
            reason = _unstorable_reason(record, oldest=oldest, newest=newest)
            if reason is None:
                storable.append(record)
            else:
                rejected.append(RejectedRecord(index=index, record=record, reason=reason))
        return storable, rejected

    def _observe(
        self,
        storable: Sequence[LocationRecord],
        *,
        accepted: Sequence[PositionItem],
        alerts: Sequence[AlertRow],
        stale: int,
        started: float,
    ) -> None:
        processor_batches_total.inc()
        processor_batch_size.observe(len(storable))
        processor_batch_seconds.observe(time.perf_counter() - started)
        processor_reports_total.inc(len(accepted))
        if stale:
            processor_stale_reports_total.inc(stale)
        for alert in alerts:
            processor_alerts_total.labels(alert.kind.value).inc()
        committed_ms = now_ms()
        for record in storable:
            processor_end_to_end_seconds.observe((committed_ms - record.received_ms) / 1000.0)
