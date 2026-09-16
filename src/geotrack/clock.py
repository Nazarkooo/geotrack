import time
from datetime import UTC, datetime


def utc_now() -> datetime:
    return datetime.now(UTC)


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def to_epoch_ms(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


def from_epoch_ms(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1000, tz=UTC)
