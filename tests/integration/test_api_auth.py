"""Login, token issuance and the shape of an authentication failure."""

import asyncio
from datetime import timedelta

import jwt
import pytest
from httpx import AsyncClient

from geotrack.api.problems import MEDIA_TYPE
from geotrack.api.security import ALGORITHM
from geotrack.clock import utc_now
from geotrack.ids import new_uuid
from tests.conftest import TEST_JWT_SECRET


async def test_login_creates_the_account_and_returns_a_usable_token(api: AsyncClient) -> None:
    response = await api.post("/api/v1/auth/login", json={"username": "Dispatcher"})

    assert response.status_code == 200
    body = response.json()
    assert body["token_type"] == "bearer"
    assert body["expires_in"] > 0
    # Usernames are normalised, so the account is the same however it was typed.
    assert body["user"]["username"] == "dispatcher"

    me = await api.get(
        "/api/v1/auth/me", headers={"Authorization": f"Bearer {body['access_token']}"}
    )
    assert me.status_code == 200
    assert me.json() == body["user"]


async def test_logging_in_twice_returns_the_same_account(api: AsyncClient) -> None:
    first = await api.post("/api/v1/auth/login", json={"username": "repeat"})
    second = await api.post("/api/v1/auth/login", json={"username": "repeat"})

    assert first.json()["user"]["id"] == second.json()["user"]["id"]
    assert first.json()["access_token"] != second.json()["access_token"]


async def test_simultaneous_first_logins_do_not_collide(api: AsyncClient) -> None:
    # Two devices signing in with a brand new name at the same moment both race to
    # insert it; neither may see a unique violation.
    responses = await asyncio.gather(
        *(api.post("/api/v1/auth/login", json={"username": "racer"}) for _ in range(5))
    )

    assert {response.status_code for response in responses} == {200}
    assert len({response.json()["user"]["id"] for response in responses}) == 1


@pytest.mark.parametrize("username", ["ab", "x" * 33, "has space", "tab\tchar", "emoji-\N{ROCKET}"])
async def test_rejects_unusable_usernames(api: AsyncClient, username: str) -> None:
    response = await api.post("/api/v1/auth/login", json={"username": username})

    assert response.status_code == 422
    assert response.headers["content-type"].startswith(MEDIA_TYPE)
    assert response.json()["code"] == "validation_error"


def _expired_token() -> str:
    issued = utc_now() - timedelta(days=2)
    return jwt.encode(
        {
            "sub": str(new_uuid()),
            "name": "ghost",
            "iat": issued,
            "exp": issued + timedelta(minutes=1),
            "jti": str(new_uuid()),
        },
        TEST_JWT_SECRET,
        algorithm=ALGORITHM,
    )


def _foreign_token() -> str:
    issued = utc_now()
    return jwt.encode(
        {
            "sub": str(new_uuid()),
            "name": "intruder",
            "iat": issued,
            "exp": issued + timedelta(hours=1),
            "jti": str(new_uuid()),
        },
        "a-different-signing-key-entirely-0123456789",
        algorithm=ALGORITHM,
    )


async def test_missing_token_is_a_problem_response(api: AsyncClient) -> None:
    response = await api.get("/api/v1/auth/me")

    assert response.status_code == 401
    assert response.headers["content-type"].startswith(MEDIA_TYPE)
    assert response.headers["www-authenticate"] == "Bearer"
    assert response.json()["code"] == "unauthorized"


async def test_garbage_expired_and_foreign_tokens_are_all_rejected(api: AsyncClient) -> None:
    candidates = ["not-a-token", _expired_token(), _foreign_token()]

    for token in candidates:
        response = await api.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert response.status_code == 401, token
        assert response.json()["code"] == "unauthorized"


async def test_a_token_without_the_bearer_scheme_is_rejected(api: AsyncClient) -> None:
    headers = await _raw_token(api)
    response = await api.get("/api/v1/auth/me", headers={"Authorization": headers})

    assert response.status_code == 401


async def _raw_token(api: AsyncClient) -> str:
    response = await api.post("/api/v1/auth/login", json={"username": "schemeless"})
    token: str = response.json()["access_token"]
    return token
