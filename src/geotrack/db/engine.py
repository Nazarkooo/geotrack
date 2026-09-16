from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from geotrack.settings import Settings


def create_engine(settings: Settings, *, pool_size: int, application_name: str) -> AsyncEngine:
    """Create a bounded async engine.

    ``max_overflow=0`` makes ``pool_size`` a hard ceiling, and a short ``pool_timeout``
    turns pool exhaustion into a fast, explicit error (mapped to HTTP 503) instead of
    requests silently queueing behind each other.
    """
    return create_async_engine(
        settings.database_url.get_secret_value(),
        pool_size=pool_size,
        max_overflow=0,
        pool_timeout=settings.db_pool_timeout_s,
        pool_pre_ping=True,
        pool_recycle=1_800,
        connect_args={
            "server_settings": {
                "application_name": application_name,
                "statement_timeout": str(settings.db_statement_timeout_ms),
                "idle_in_transaction_session_timeout": "15000",
                # JIT compilation costs more than it saves on short OLTP statements.
                "jit": "off",
            },
            "command_timeout": 30,
        },
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)
