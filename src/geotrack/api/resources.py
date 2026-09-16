"""Process-wide resources shared by every request, websocket and background task.

Kept in its own module so routes and dependencies can refer to it without importing
the application factory (which imports them).
"""

from dataclasses import dataclass

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from geotrack.ingest.backlog import BacklogMonitor
from geotrack.ingest.service import IngestService
from geotrack.observability.loop_lag import EventLoopLagMonitor
from geotrack.realtime.gateway import Gateway
from geotrack.settings import Settings


@dataclass(slots=True)
class AppResources:
    settings: Settings
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    redis: Redis
    redis_pubsub: Redis
    gateway: Gateway
    loop_lag: EventLoopLagMonitor
    backlog: BacklogMonitor
    ingest: IngestService
