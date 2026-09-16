"""Mock authentication.

The brief allows a simplified scheme, so there are no passwords: a username is
exchanged for a signed, expiring token. Everything downstream (REST and websocket)
authorises against that token, so swapping in a real identity provider later means
replacing this module only.
"""

from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID

import jwt

from geotrack.clock import utc_now
from geotrack.ids import new_uuid
from geotrack.settings import Settings

ALGORITHM = "HS256"


class InvalidTokenError(Exception):
    """The token is missing, malformed, expired or signed with the wrong key."""


@dataclass(frozen=True, slots=True)
class TokenClaims:
    user_id: UUID
    username: str


def create_access_token(*, user_id: UUID, username: str, settings: Settings) -> tuple[str, int]:
    """Return the encoded token and its lifetime in seconds."""
    issued_at = utc_now()
    expires_in = settings.jwt_ttl_seconds
    payload = {
        "sub": str(user_id),
        "name": username,
        "iat": issued_at,
        "exp": issued_at + timedelta(seconds=expires_in),
        "jti": str(new_uuid()),
    }
    token = jwt.encode(payload, settings.jwt_secret.get_secret_value(), algorithm=ALGORITHM)
    return token, expires_in


def decode_access_token(token: str, settings: Settings) -> TokenClaims:
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret.get_secret_value(),
            algorithms=[ALGORITHM],
            options={"require": ["exp", "iat", "sub"]},
            leeway=5,
        )
        return TokenClaims(user_id=UUID(payload["sub"]), username=str(payload["name"]))
    except (jwt.InvalidTokenError, KeyError, ValueError) as exc:
        raise InvalidTokenError(str(exc)) from exc
