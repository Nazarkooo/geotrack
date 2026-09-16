"""The dashboard websocket endpoint.

A browser cannot set an Authorization header on a websocket handshake, so the token
arrives as a second subprotocol (``bearer.<jwt>``) and the query string is only a
fallback for command-line clients. Authentication happens after ``accept`` because a
rejected handshake reaches JavaScript as an opaque error, while a close code does not.
"""

from typing import Annotated

from fastapi import APIRouter, Query, WebSocket

from geotrack.api.deps import get_websocket_resources
from geotrack.api.security import InvalidTokenError, decode_access_token
from geotrack.realtime.gateway import WS_SUBPROTOCOL, token_from_handshake
from geotrack.realtime.protocol import CLOSE_UNAUTHORIZED
from geotrack.schemas.auth import UserOut

router = APIRouter(tags=["realtime"])


@router.websocket("/ws")
async def client_stream(
    websocket: WebSocket, access_token: Annotated[str | None, Query()] = None
) -> None:
    resources = get_websocket_resources(websocket)
    offered: list[str] = websocket.scope.get("subprotocols", [])
    await websocket.accept(subprotocol=WS_SUBPROTOCOL if WS_SUBPROTOCOL in offered else None)

    try:
        claims = decode_access_token(
            token_from_handshake(offered, access_token) or "", resources.settings
        )
    except InvalidTokenError:
        await websocket.close(CLOSE_UNAUTHORIZED, "invalid access token")
        return

    await resources.gateway.serve(
        websocket,
        user=UserOut(id=claims.user_id, username=claims.username),
        user_agent=websocket.headers.get("user-agent"),
    )
