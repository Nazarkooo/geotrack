from typing import Literal
from uuid import UUID

from pydantic import field_validator

from geotrack.schemas.common import Payload, Schema, Username


class LoginRequest(Payload):
    username: Username

    @field_validator("username")
    @classmethod
    def _normalise(cls, value: str) -> str:
        return value.lower()


class UserOut(Schema):
    id: UUID
    username: str


class TokenResponse(Schema):
    access_token: str
    token_type: Literal["bearer"] = "bearer"  # noqa: S105 - OAuth2 token type, not a secret
    expires_in: int
    user: UserOut
