import hmac
from collections.abc import AsyncIterator
from contextlib import suppress
from typing import Annotated

from fastapi import Depends, Header, Request, WebSocket
from sqlalchemy.ext.asyncio import AsyncSession

from geotrack.api.problems import ProblemError
from geotrack.api.resources import AppResources
from geotrack.api.security import InvalidTokenError, TokenClaims, decode_access_token


def get_resources(request: Request) -> AppResources:
    resources: AppResources = request.app.state.resources
    return resources


def get_websocket_resources(websocket: WebSocket) -> AppResources:
    resources: AppResources = websocket.app.state.resources
    return resources


Resources = Annotated[AppResources, Depends(get_resources)]


async def get_session(resources: Resources) -> AsyncIterator[AsyncSession]:
    """A session per request, returned to the pool as soon as the request ends.

    Websocket handlers deliberately do not use this: holding a pooled connection for
    the lifetime of a socket would exhaust the pool at a few hundred clients.
    """
    async with resources.session_factory() as session:
        yield session


Session = Annotated[AsyncSession, Depends(get_session)]


def _unauthorized(detail: str) -> ProblemError:
    return ProblemError(
        401,
        "Authentication required",
        code="unauthorized",
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


async def get_current_user(
    resources: Resources,
    authorization: Annotated[str | None, Header()] = None,
) -> TokenClaims:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise _unauthorized("Provide a bearer token from POST /api/v1/auth/login.")
    try:
        return decode_access_token(authorization.split(" ", 1)[1].strip(), resources.settings)
    except InvalidTokenError as exc:
        raise _unauthorized(f"The access token is not valid: {exc}") from exc


CurrentUser = Annotated[TokenClaims, Depends(get_current_user)]


def _key_candidates(provided: str) -> tuple[bytes, ...]:
    """The byte strings a device could have meant by this value.

    A key is configured as text but travels as bytes, and the two transports hand it
    over differently: a header arrives latin-1 decoded, because that is how raw header
    bytes are surfaced, while a query parameter arrives as real text decoded from UTF-8.
    Encoding both readings back keeps the comparison a comparison of bytes — the only
    kind that is safe, since ``hmac.compare_digest`` refuses text outside ASCII and
    would turn a wrong key into a 500 on the busiest endpoint in the system. For an
    ASCII key the two readings are identical and exactly one comparison happens.
    """
    candidates = {provided.encode()}
    with suppress(UnicodeEncodeError):
        candidates.add(provided.encode("latin-1"))
    return tuple(candidates)


def verify_ingest_key(resources: AppResources, provided: str | None) -> None:
    expected = resources.settings.ingest_api_key.get_secret_value().encode()
    if not provided or not any(
        hmac.compare_digest(candidate, expected) for candidate in _key_candidates(provided)
    ):
        raise ProblemError(
            401,
            "Authentication required",
            code="unauthorized",
            detail="A valid X-Ingest-Key header is required to submit device reports.",
        )


async def require_ingest_key(
    resources: Resources,
    x_ingest_key: Annotated[str | None, Header()] = None,
) -> None:
    verify_ingest_key(resources, x_ingest_key)


IngestKey = Annotated[None, Depends(require_ingest_key)]
