"""Mapping of driver exceptions to PostgreSQL SQLSTATE codes.

SQLAlchemy wraps asyncpg errors (``DBAPIError.orig`` -> adapter error -> asyncpg
error via ``__cause__``), so the code is looked up along the whole chain.
"""

SERIALIZATION_FAILURE = "40001"
DEADLOCK_DETECTED = "40P01"
FOREIGN_KEY_VIOLATION = "23503"
UNIQUE_VIOLATION = "23505"

# A zone deleted while a batch referencing it is in flight surfaces as a foreign key
# violation; retrying the batch re-reads zones and simply skips the deleted one.
RETRYABLE_SQLSTATES = frozenset({SERIALIZATION_FAILURE, DEADLOCK_DETECTED, FOREIGN_KEY_VIOLATION})


def sqlstate_of(exc: BaseException) -> str | None:
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        state = getattr(current, "sqlstate", None)
        if isinstance(state, str) and state:
            return state
        orig = getattr(current, "orig", None)
        current = orig if isinstance(orig, BaseException) else current.__cause__
    return None
