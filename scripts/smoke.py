"""End-to-end check against a running stack.

Logs in, opens the dashboard websocket, creates a zone, reports a position inside it
and fails unless the enter alert arrives over the websocket. That covers the whole
path: proxy, REST, Redis stream, processor, PostGIS and the fan-out back to clients.

Run it through ``scripts/smoke.sh``, which loads .env first.
"""

import asyncio
import json
import os
import sys
import time
import uuid
from typing import Any

import httpx
from websockets.asyncio.client import connect

BASE_URL = os.environ.get("SMOKE_BASE_URL", "http://127.0.0.1:8080").rstrip("/")
INGEST_KEY = os.environ.get("INGEST_API_KEY", "")
READY_TIMEOUT_S = float(os.environ.get("SMOKE_READY_TIMEOUT_S", "60"))
ALERT_TIMEOUT_S = float(os.environ.get("SMOKE_ALERT_TIMEOUT_S", "30"))

# Somewhere unremarkable in the Dnipro river bend, far from any other test data.
LATITUDE = 48.4647
LONGITUDE = 35.0462


def log(message: str) -> None:
    print(f"[smoke] {message}", flush=True)


async def wait_until_ready(client: httpx.AsyncClient) -> None:
    deadline = time.monotonic() + READY_TIMEOUT_S
    last = "no attempt made"
    while time.monotonic() < deadline:
        try:
            response = await client.get("/health/ready", timeout=5)
        except httpx.HTTPError as exc:
            last = repr(exc)
        else:
            if response.status_code == 200:
                log("stack is ready")
                return
            last = f"HTTP {response.status_code}"
        await asyncio.sleep(1)
    raise SystemExit(f"stack never became ready: {last}")


async def expect_alert(websocket: Any, device_id: str, zone_id: str) -> dict[str, Any]:
    """Read frames until the enter alert for this device shows up."""
    deadline = time.monotonic() + ALERT_TIMEOUT_S
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SystemExit(f"no alert for {device_id} within {ALERT_TIMEOUT_S:.0f}s")
        raw = await asyncio.wait_for(websocket.recv(), timeout=remaining)
        frame = json.loads(raw)
        if frame.get("type") != "alert":
            continue
        alert = frame["alert"]
        if alert["device_id"] == device_id and alert["zone"]["id"] == zone_id:
            return dict(alert)


async def main() -> None:
    if not INGEST_KEY:
        raise SystemExit("INGEST_API_KEY is not set; run this through scripts/smoke.sh")

    suffix = uuid.uuid4().hex[:8]
    username = f"smoke-{suffix}"
    device_id = f"smoke-device-{suffix}"

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10) as client:
        await wait_until_ready(client)

        response = await client.post("/api/v1/auth/login", json={"username": username})
        response.raise_for_status()
        token = response.json()["access_token"]
        client.headers["Authorization"] = f"Bearer {token}"
        log(f"logged in as {username}")

        ws_url = BASE_URL.replace("http://", "ws://").replace("https://", "wss://") + "/ws"
        async with connect(ws_url, subprotocols=["geotrack.v1", f"bearer.{token}"]) as websocket:
            hello = json.loads(await asyncio.wait_for(websocket.recv(), timeout=10))
            assert hello["type"] == "hello", hello
            log(f"websocket session {hello['session_id']}")

            await websocket.send(
                json.dumps(
                    {
                        "type": "viewport",
                        "bbox": [LONGITUDE - 0.1, LATITUDE - 0.1, LONGITUDE + 0.1, LATITUDE + 0.1],
                    }
                )
            )

            response = await client.post(
                "/api/v1/geozones",
                json={
                    "name": f"smoke zone {suffix}",
                    "latitude": LATITUDE,
                    "longitude": LONGITUDE,
                    "radius_m": 500,
                },
            )
            response.raise_for_status()
            zone_id = response.json()["id"]
            log(f"created zone {zone_id}")

            response = await client.post(
                "/api/v1/ingest/locations",
                headers={"X-Ingest-Key": INGEST_KEY},
                json={
                    "items": [
                        {
                            "device_id": device_id,
                            "latitude": LATITUDE,
                            "longitude": LONGITUDE,
                            "timestamp": time.time(),
                        }
                    ]
                },
            )
            response.raise_for_status()
            log(f"reported {device_id} inside the zone")

            started = time.monotonic()
            alert = await expect_alert(websocket, device_id, zone_id)
            elapsed = time.monotonic() - started
            assert alert["kind"] == "enter", alert
            log(f"enter alert received after {elapsed * 1000:.0f} ms")

        response = await client.delete(f"/api/v1/geozones/{zone_id}")
        response.raise_for_status()
        log("cleaned up")

    log("OK")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except SystemExit:
        raise
    except Exception as exc:
        # A smoke check reports and exits; it has nothing to recover to.
        print(f"[smoke] FAILED: {exc!r}", file=sys.stderr)
        raise SystemExit(1) from exc
