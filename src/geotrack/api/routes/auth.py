from typing import Any

from fastapi import APIRouter, Depends
from fastapi.security import HTTPBearer

from geotrack.api.deps import CurrentUser, Resources, Session
from geotrack.api.security import create_access_token
from geotrack.db.repositories import users
from geotrack.schemas.auth import LoginRequest, TokenResponse, UserOut

# Declared purely so the generated documentation carries an Authorize control and every
# protected operation advertises how to reach it; the token is still verified by
# ``get_current_user``, which is the single place that trusts it.
bearer_scheme = HTTPBearer(
    auto_error=False,
    scheme_name="Bearer token",
    description="Access token issued by POST /api/v1/auth/login",
)
DOCUMENTED_AUTH = [Depends(bearer_scheme)]
UNAUTHORIZED: dict[int | str, dict[str, Any]] = {
    401: {"description": "Missing, malformed or expired bearer token"}
}

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


@router.post("/login", response_model=TokenResponse, summary="Exchange a username for a token")
async def login(payload: LoginRequest, session: Session, resources: Resources) -> TokenResponse:
    """Sign in with a username alone.

    Authentication is deliberately simplified: there is no password and no registration
    step, so the first login for a name creates the account. The result is a normal
    signed bearer token with an expiry, and every other endpoint — REST and websocket —
    checks only that token. Putting a real identity provider in front of the service
    therefore means replacing this one endpoint, nothing else.
    """
    user = await users.get_or_create(session, payload.username)
    await session.commit()
    token, expires_in = create_access_token(
        user_id=user.id, username=user.username, settings=resources.settings
    )
    return TokenResponse(access_token=token, expires_in=expires_in, user=user)


@router.get(
    "/me",
    response_model=UserOut,
    dependencies=DOCUMENTED_AUTH,
    responses=UNAUTHORIZED,
    summary="Identity behind the bearer token",
)
async def me(current_user: CurrentUser) -> UserOut:
    """Read the caller's identity.

    Answered from the token's own claims: it is signed and short-lived, so a database
    round trip here would add load to every dashboard load without adding trust.
    """
    return UserOut(id=current_user.user_id, username=current_user.username)
