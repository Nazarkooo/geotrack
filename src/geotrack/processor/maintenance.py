"""Daily partitions for the location history.

Partitions are created ahead of time rather than on demand: a report that arrives for
a day with no partition fails with a check violation that no retry can fix. One
replica does the work per round — the lease is held for the length of the interval,
so the others simply find it taken and skip.

Taking the lead is a promise to do the round. A replica that takes it and then fails —
the database is not up yet, the process is about to exit — gives it straight back, so
the next one in the fleet does the work instead of finding the round already claimed by
somebody who never did it.
"""

from dataclasses import dataclass

import structlog
from redis.asyncio import Redis
from redis.commands.core import AsyncScript
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from geotrack.messaging.keys import MAINTENANCE_LEASE_KEY
from geotrack.processor.leases import RELEASE_SCRIPT

logger = structlog.get_logger(__name__)

INTERVAL_S = 600.0
# One round minus a second, so that exactly one replica works per round even when the
# replicas' clocks and tick offsets differ.
LEASE_TTL_MS = int(INTERVAL_S * 1_000) - 1_000
# Two days of headroom: a replica that misses a round (or a clock that rolls over
# between rounds) still finds tomorrow's partition waiting.
DAYS_AHEAD = 2

ENSURE_SQL = text(
    """
    SELECT geotrack_ensure_history_partitions(
        (now() AT TIME ZONE 'UTC')::date - CAST(:retention_days AS int),
        (now() AT TIME ZONE 'UTC')::date + CAST(:days_ahead AS int)
    )
    """
)

DROP_SQL = text(
    """
    SELECT geotrack_drop_history_partitions(
        (now() AT TIME ZONE 'UTC')::date - CAST(:retention_days AS int)
    )
    """
)


@dataclass(frozen=True, slots=True)
class MaintenanceOutcome:
    led: bool
    created: int = 0
    dropped: int = 0


class PartitionMaintenance:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        redis: Redis,
        *,
        retention_days: int,
        instance_id: str,
        lease_key: str = MAINTENANCE_LEASE_KEY,
        ttl_ms: int = LEASE_TTL_MS,
    ) -> None:
        self._session_factory = session_factory
        self._redis = redis
        self._retention_days = retention_days
        self._instance_id = instance_id
        self._lease_key = lease_key
        self._ttl_ms = ttl_ms
        self._release: AsyncScript = redis.register_script(RELEASE_SCRIPT)

    async def run_once(self) -> MaintenanceOutcome:
        if not await self._take_lead():
            return MaintenanceOutcome(led=False)

        params = {"retention_days": self._retention_days, "days_ahead": DAYS_AHEAD}
        try:
            async with self._session_factory() as session, session.begin():
                created = await session.scalar(ENSURE_SQL, params)
                dropped = await session.scalar(DROP_SQL, params)
        except BaseException:
            await self._give_up_the_lead()
            raise

        outcome = MaintenanceOutcome(led=True, created=int(created or 0), dropped=int(dropped or 0))
        if outcome.created or outcome.dropped:
            logger.info(
                "history partitions maintained",
                created=outcome.created,
                dropped=outcome.dropped,
                retention_days=self._retention_days,
            )
        return outcome

    async def _take_lead(self) -> bool:
        """Hold the lease for one interval, so exactly one replica works per round."""
        taken = await self._redis.set(self._lease_key, self._instance_id, nx=True, px=self._ttl_ms)
        return bool(taken)

    async def _give_up_the_lead(self) -> None:
        """Compare-and-delete, so a round that has since moved on is left alone."""
        try:
            await self._release(keys=[self._lease_key], args=[self._instance_id])
        except Exception as exc:
            logger.warning("could not hand back the maintenance lease", error=str(exc))
