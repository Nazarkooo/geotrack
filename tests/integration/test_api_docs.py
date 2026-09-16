"""The published contract.

The generated document is what a reviewer or a client author reads first, so it is
checked like any other behaviour: if an endpoint needs a credential, the document has
to say so, and the ingest body shapes have to match what the parser really accepts.
"""

from typing import Any

import pytest
from httpx import AsyncClient

PUBLIC = {"/health/live", "/health/ready", "/api/v1/auth/login"}
BEARER = "Bearer token"
INGEST_KEY = "Device ingestion key"


@pytest.fixture
async def spec(api: AsyncClient) -> dict[str, Any]:
    response = await api.get("/openapi.json")
    assert response.status_code == 200
    document: dict[str, Any] = response.json()
    return document


def _operations(spec: dict[str, Any]) -> list[tuple[str, str, dict[str, Any]]]:
    return [
        (path, method, operation)
        for path, methods in spec["paths"].items()
        for method, operation in methods.items()
    ]


def test_both_credentials_are_described(spec: dict[str, Any]) -> None:
    schemes = spec["components"]["securitySchemes"]

    assert schemes[BEARER]["type"] == "http"
    assert schemes[BEARER]["scheme"] == "bearer"
    assert schemes[INGEST_KEY] == {
        "type": "apiKey",
        "in": "header",
        "name": "X-Ingest-Key",
        "description": "Shared key configured as INGEST_API_KEY",
    }


def test_every_non_public_operation_advertises_its_credential(spec: dict[str, Any]) -> None:
    for path, method, operation in _operations(spec):
        if path in PUBLIC:
            assert "security" not in operation, f"{method} {path}"
            continue
        expected = INGEST_KEY if path.startswith("/api/v1/ingest") else BEARER
        assert operation.get("security") == [{expected: []}], f"{method} {path}"
        assert "401" in operation["responses"], f"{method} {path}"


def test_conditional_endpoints_document_their_failure_codes(spec: dict[str, Any]) -> None:
    for method in ("put", "patch", "delete"):
        responses = spec["paths"]["/api/v1/geozones/{zone_id}"][method]["responses"]
        assert {"400", "404", "412"} <= set(responses)


def test_the_ingest_body_documents_all_three_accepted_shapes(spec: dict[str, Any]) -> None:
    schema = spec["paths"]["/api/v1/ingest/locations"]["post"]["requestBody"]["content"][
        "application/json"
    ]["schema"]
    single, array, envelope = schema["oneOf"]

    assert set(single["required"]) == {"device_id", "latitude", "longitude", "timestamp"}
    assert array["items"] == single
    assert envelope["properties"]["items"]["items"] == single
    # The parser takes epoch numbers too, and the document must not claim otherwise.
    assert {"type": "number"} in single["properties"]["timestamp"]["anyOf"]


async def test_the_interactive_documentation_renders(api: AsyncClient) -> None:
    response = await api.get("/docs")

    assert response.status_code == 200
    assert "swagger" in response.text.lower()


def test_every_operation_carries_a_summary(spec: dict[str, Any]) -> None:
    missing = [
        f"{method} {path}"
        for path, method, operation in _operations(spec)
        if not operation.get("summary")
    ]

    assert missing == []
