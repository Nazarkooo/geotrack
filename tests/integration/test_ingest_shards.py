"""The shard count is recorded once and every later start has to agree with it."""

from typing import Any

import pytest
from redis.asyncio import Redis

from geotrack.ingest.shards import ShardCountMismatchError, verify_shard_count
from geotrack.messaging.keys import SHARDS_META_KEY
from geotrack.settings import Settings
from tests.conftest import make_settings
from tests.integration.conftest import running_app


def _settings(settings: Settings, **overrides: Any) -> Settings:
    return make_settings(
        database_url=settings.database_url.get_secret_value(),
        redis_url=settings.redis_url.get_secret_value(),
        **overrides,
    )


async def test_the_first_start_records_the_layout_and_later_ones_accept_it(
    settings: Settings, redis_client: Redis
) -> None:
    async with running_app(_settings(settings, ingest_shards=8)):
        pass
    async with running_app(_settings(settings, ingest_shards=8)):
        pass

    assert await redis_client.get(SHARDS_META_KEY) == b"8"


async def test_a_replica_configured_for_another_shard_count_refuses_to_start(
    settings: Settings, redis_client: Redis
) -> None:
    # Reports are placed by ``crc32(device_id) % shards``, so a replica that disagrees
    # writes to streams the processors do not read: accepted with a 202 and never stored,
    # with every device's ordering broken on the way. Refusing to start is the only
    # answer that cannot lose data quietly.
    async with running_app(_settings(settings, ingest_shards=8)):
        pass

    with pytest.raises(ShardCountMismatchError, match="partitioned into 8"):
        async with running_app(_settings(settings, ingest_shards=4)):
            pass

    # The recorded layout belongs to the queue, not to whichever replica started last.
    assert await redis_client.get(SHARDS_META_KEY) == b"8"


async def test_the_guard_reports_what_is_recorded_even_when_it_is_nonsense(
    settings: Settings, redis_client: Redis
) -> None:
    await redis_client.set(SHARDS_META_KEY, "not-a-number")

    with pytest.raises(ShardCountMismatchError, match="not-a-number"):
        await verify_shard_count(settings.redis_url.get_secret_value(), shards=8)
