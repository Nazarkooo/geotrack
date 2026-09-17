"""Fair-share shard ownership: the maths that decides who processes what."""

import math

import pytest

from geotrack.processor.leases import fair_share_plan


@pytest.mark.parametrize(
    ("shards", "live", "expected"),
    [(8, 1, 8), (8, 2, 4), (8, 3, 3), (8, 8, 1), (8, 16, 1), (7, 2, 4), (1, 4, 1)],
)
def test_target_is_the_ceiling_of_an_even_split(shards: int, live: int, expected: int) -> None:
    plan = fair_share_plan(shards=shards, owned=set(), live=live, offset=0)

    assert plan.target == expected == math.ceil(shards / live)


def test_a_lone_replica_takes_every_shard() -> None:
    plan = fair_share_plan(shards=8, owned=set(), live=1, offset=0)

    assert plan.target == 8
    assert set(plan.candidates) == set(range(8))
    assert plan.release == ()


def test_candidates_exclude_shards_already_owned() -> None:
    plan = fair_share_plan(shards=8, owned={0, 1}, live=1, offset=0)

    assert set(plan.candidates) == {2, 3, 4, 5, 6, 7}


def test_every_free_shard_stays_a_candidate_even_beyond_the_target() -> None:
    """The caller stops at ``target``; offering only ``target`` candidates would strand
    the high shards when the low ones are already leased by another replica."""
    plan = fair_share_plan(shards=8, owned=set(), live=2, offset=0)

    assert plan.target == 4
    assert set(plan.candidates) == set(range(8))


def test_candidates_are_rotated_so_replicas_do_not_all_start_at_shard_zero() -> None:
    first = fair_share_plan(shards=8, owned=set(), live=2, offset=0).candidates
    second = fair_share_plan(shards=8, owned=set(), live=2, offset=5).candidates

    assert first == (0, 1, 2, 3, 4, 5, 6, 7)
    assert second == (5, 6, 7, 0, 1, 2, 3, 4)
    assert set(first) == set(second)


def test_a_replica_over_its_share_hands_shards_back() -> None:
    """A second replica joined: the one holding all eight shards releases four."""
    plan = fair_share_plan(shards=8, owned=set(range(8)), live=2, offset=0)

    assert plan.target == 4
    assert len(plan.release) == 4
    assert set(plan.release) <= set(range(8))
    assert plan.candidates == ()


def test_released_shards_are_the_highest_numbered_ones() -> None:
    plan = fair_share_plan(shards=8, owned={1, 2, 5, 7}, live=4, offset=0)

    assert plan.target == 2
    assert plan.release == (7, 5)


def test_nothing_moves_when_ownership_already_matches_the_share() -> None:
    plan = fair_share_plan(shards=8, owned={0, 1, 2, 3}, live=2, offset=0)

    assert (plan.candidates, plan.release) == ((), ())


def test_an_uneven_split_lets_one_replica_hold_the_remainder() -> None:
    """Seven shards over two replicas: 4 + 3, and neither is asked to release.

    The replica one below the target still offers the taken shards as candidates; the
    lease's ``SET NX`` is what stops it from stealing them.
    """
    first = fair_share_plan(shards=7, owned={0, 1, 2, 3}, live=2, offset=0)
    second = fair_share_plan(shards=7, owned={4, 5, 6}, live=2, offset=0)

    assert first.target == second.target == 4
    assert (first.release, second.release) == ((), ())
    assert first.candidates == ()
    assert set(second.candidates) == {0, 1, 2, 3}


def test_live_count_below_one_is_treated_as_one() -> None:
    """A stale membership read must never widen the share to every shard at once."""
    assert fair_share_plan(shards=4, owned=set(), live=0, offset=0).target == 4
