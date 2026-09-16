from collections import Counter

import pytest

from geotrack.sharding import shard_for


def test_shard_is_stable_across_processes() -> None:
    # Golden values: the API and every processor replica must agree on placement, and
    # crc32 is deterministic across processes (unlike hash(), which is salted).
    assert [shard_for(f"dev-{i:05d}", 8) for i in range(5)] == [0, 6, 4, 2, 1]


def test_shards_are_evenly_used() -> None:
    counts = Counter(shard_for(f"dev-{i:05d}", 8) for i in range(10_000))

    assert set(counts) == set(range(8))
    for count in counts.values():
        assert 1_250 * 0.85 <= count <= 1_250 * 1.15


@pytest.mark.parametrize("shards", [0, -1])
def test_rejects_non_positive_shard_count(shards: int) -> None:
    with pytest.raises(ValueError, match="shards"):
        shard_for("dev", shards)
