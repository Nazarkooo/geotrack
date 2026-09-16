from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from geotrack.ids import new_uuid
from geotrack.schemas.auth import UserOut

_INSERT = text(
    "INSERT INTO users (id, username) VALUES (:id, :username) "
    "ON CONFLICT (username) DO NOTHING "
    "RETURNING id, username"
)
_SELECT = text("SELECT id, username FROM users WHERE username = :username")


async def get_or_create(session: AsyncSession, username: str) -> UserOut:
    """Return the account for this name, creating it on first sight.

    Two first logins for the same name race against each other; ``ON CONFLICT DO
    NOTHING`` plus a follow-up read means the loser gets the existing row instead of
    a unique-violation error. The second statement only runs on the losing path.
    """
    row = (await session.execute(_INSERT, {"id": new_uuid(), "username": username})).first()
    if row is None:
        row = (await session.execute(_SELECT, {"username": username})).first()
    if row is None:  # pragma: no cover - the insert above cannot leave the row missing
        raise RuntimeError(f"user {username!r} vanished between insert and read")
    return UserOut(id=row.id, username=row.username)
