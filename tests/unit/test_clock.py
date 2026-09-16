from datetime import UTC, datetime, timedelta, timezone

from geotrack.clock import from_epoch_ms, now_ms, to_epoch_ms, utc_now


def test_epoch_ms_round_trip() -> None:
    moment = datetime(2026, 9, 16, 12, 30, 15, 123_000, tzinfo=UTC)

    assert from_epoch_ms(to_epoch_ms(moment)) == moment


def test_to_epoch_ms_respects_timezone() -> None:
    kyiv = timezone(timedelta(hours=3))
    local = datetime(2026, 9, 16, 15, 0, tzinfo=kyiv)

    assert to_epoch_ms(local) == to_epoch_ms(datetime(2026, 9, 16, 12, 0, tzinfo=UTC))


def test_utc_now_is_aware_and_close_to_now_ms() -> None:
    current = utc_now()

    assert current.tzinfo is UTC
    assert abs(to_epoch_ms(current) - now_ms()) < 1_000
