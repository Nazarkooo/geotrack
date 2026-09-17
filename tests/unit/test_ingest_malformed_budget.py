"""The allowance a device gets for sending nonsense before it is hung up on."""

import pytest

from geotrack.api.routes.ws_ingest import MALFORMED_LIMIT, MALFORMED_WINDOW_S, MalformedBudget


def test_a_handful_of_bad_frames_is_tolerated() -> None:
    budget = MalformedBudget(limit=3, window_s=60)

    assert [budget.record() for _ in range(3)] == [True, True, True]


def test_the_budget_runs_out_once_the_limit_is_passed() -> None:
    budget = MalformedBudget(limit=3, window_s=60)
    for _ in range(3):
        budget.record()

    assert budget.record() is False


def test_the_allowance_is_per_window_not_per_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A device that sends one bad frame an hour is buggy, not abusive, and must survive
    # an arbitrarily long session.
    clock = [1_000.0]
    monkeypatch.setattr("geotrack.api.routes.ws_ingest.time.monotonic", lambda: clock[0])
    budget = MalformedBudget(limit=2, window_s=60)

    for _ in range(50):
        assert budget.record() is True
        clock[0] += 61


def test_the_defaults_match_the_documented_protocol() -> None:
    assert (MALFORMED_LIMIT, MALFORMED_WINDOW_S) == (20, 60.0)
