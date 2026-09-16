"""RFC 9457 problem responses.

One error shape for the whole API: a client can always read ``code`` to branch on,
and ``detail`` to show.
"""

from collections.abc import Mapping
from typing import Any

import orjson
import structlog
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import Response
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from starlette.exceptions import HTTPException as StarletteHTTPException

MEDIA_TYPE = "application/problem+json"
logger = structlog.get_logger(__name__)


class ProblemError(Exception):
    def __init__(
        self,
        status: int,
        title: str,
        *,
        code: str,
        detail: str | None = None,
        headers: Mapping[str, str] | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(detail or title)
        self.status = status
        self.title = title
        self.code = code
        self.detail = detail
        self.headers = dict(headers or {})
        self.extra = dict(extra or {})


def problem_response(
    *,
    status: int,
    title: str,
    code: str,
    detail: str | None = None,
    headers: Mapping[str, str] | None = None,
    extra: Mapping[str, Any] | None = None,
) -> Response:
    body: dict[str, Any] = {
        "type": f"/problems/{code}",
        "title": title,
        "status": status,
        "code": code,
    }
    if detail:
        body["detail"] = detail
    body.update(extra or {})
    return Response(
        # ``default=str`` because validation errors carry the offending input and the
        # original exception in their context: an error response must never itself fail
        # to render just because a client sent an unusual value.
        orjson.dumps(body, default=str),
        status_code=status,
        media_type=MEDIA_TYPE,
        headers=dict(headers or {}),
    )


def install_problem_handlers(app: FastAPI) -> None:
    @app.exception_handler(ProblemError)
    async def _problem(_: Request, exc: ProblemError) -> Response:
        return problem_response(
            status=exc.status,
            title=exc.title,
            code=exc.code,
            detail=exc.detail,
            headers=exc.headers,
            extra=exc.extra,
        )

    @app.exception_handler(RequestValidationError)
    async def _validation(_: Request, exc: RequestValidationError) -> Response:
        return problem_response(
            status=422,
            title="Request validation failed",
            code="validation_error",
            detail="The request body or parameters did not pass validation.",
            extra={"errors": exc.errors()},
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, exc: StarletteHTTPException) -> Response:
        detail = exc.detail or None
        return problem_response(
            status=exc.status_code,
            title=_TITLES.get(exc.status_code, "Request failed"),
            code=_CODES.get(exc.status_code, "http_error"),
            detail=detail,
            headers=exc.headers,
        )

    @app.exception_handler(PoolTimeoutError)
    async def _pool_timeout(_: Request, exc: PoolTimeoutError) -> Response:
        # Shedding load beats queueing behind an exhausted pool until clients time out.
        logger.warning("database pool exhausted", error=str(exc))
        return problem_response(
            status=503,
            title="Database is busy",
            code="database_busy",
            detail="No database connection was available in time. Retry shortly.",
            headers={"Retry-After": "1"},
        )


_TITLES = {
    400: "Bad request",
    401: "Authentication required",
    403: "Forbidden",
    404: "Not found",
    409: "Conflict",
    412: "Precondition failed",
    413: "Payload too large",
    422: "Request validation failed",
    429: "Too many requests",
    503: "Service unavailable",
}

_CODES = {
    400: "bad_request",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    409: "conflict",
    412: "precondition_failed",
    413: "payload_too_large",
    422: "validation_error",
    429: "too_many_requests",
    503: "service_unavailable",
}


__all__ = [
    "MEDIA_TYPE",
    "HTTPException",
    "ProblemError",
    "install_problem_handlers",
    "problem_response",
]
