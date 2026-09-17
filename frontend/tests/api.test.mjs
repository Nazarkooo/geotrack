import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { ApiClient, ApiError } from "../js/api.js";

function stubFetch(responder) {
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options });
    return responder(url, options, calls.length - 1);
  };
  return { calls, fetchImpl };
}

const json = (body, init = {}) =>
  new Response(JSON.stringify(body), {
    status: init.status ?? 200,
    headers: { "content-type": "application/json", ...(init.headers ?? {}) },
  });

const problem = (body, status) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/problem+json" },
  });

const clientWith = (responder, options = {}) => {
  const { calls, fetchImpl } = stubFetch(responder);
  return { calls, api: new ApiClient({ fetchImpl, ...options }) };
};

describe("ApiClient requests", () => {
  it("targets the versioned API on the page's own origin", async () => {
    const { api, calls } = clientWith(() => json({ items: [], total: 0 }));
    await api.listZones();

    // One page covers the per-user quota, so the panel never has to paginate.
    assert.equal(calls[0].url, "/api/v1/geozones?limit=500&offset=0");
    assert.equal(calls[0].options.method, "GET");
  });

  it("omits the authorization header until a token is set", async () => {
    const { api, calls } = clientWith(() => json({ items: [], total: 0 }));

    await api.listZones();
    api.setToken("jwt-value");
    await api.listZones();

    assert.equal(calls[0].options.headers.Authorization, undefined);
    assert.equal(calls[1].options.headers.Authorization, "Bearer jwt-value");
  });

  it("drops empty query parameters instead of sending them", async () => {
    const { api, calls } = clientWith(() => json({ items: [], next_before_id: null }));

    await api.listAlerts({ afterId: 12, zoneId: null, limit: 50, deviceId: undefined });

    assert.equal(calls[0].url, "/api/v1/alerts?after_id=12&limit=50");
  });

  it("encodes a bounding box and a device id safely", async () => {
    const { api, calls } = clientWith(() => json({ items: [] }));

    await api.listDevices({ bbox: [30.1, 50.2, 30.9, 50.8], limit: 10 });
    await api.deviceTrack("dev/1", { since: "2026-09-17T10:00:00Z" });

    assert.equal(calls[0].url, "/api/v1/devices?bbox=30.1%2C50.2%2C30.9%2C50.8&limit=10");
    assert.equal(calls[1].url, "/api/v1/devices/dev%2F1/track?since=2026-09-17T10%3A00%3A00Z");
  });

  it("sends a json body and the optimistic concurrency header", async () => {
    const { api, calls } = clientWith(() => json({ id: "zone-a", version: 3 }));

    await api.updateZone("zone-a", { name: "Depot" }, { version: 2 });

    const { url, options } = calls[0];
    assert.equal(url, "/api/v1/geozones/zone-a");
    assert.equal(options.method, "PATCH");
    assert.equal(options.headers["Content-Type"], "application/json");
    assert.equal(options.headers["If-Match"], '"2"');
    assert.deepEqual(JSON.parse(options.body), { name: "Depot" });
  });

  it("returns nothing for a 204", async () => {
    const { api } = clientWith(() => new Response(null, { status: 204 }));
    assert.equal(await api.deleteZone("zone-a"), null);
  });
});

describe("ApiClient failures", () => {
  it("turns problem+json into a readable error", async () => {
    const { api } = clientWith(() =>
      problem(
        {
          type: "/problems/conflict",
          title: "Conflict",
          status: 409,
          code: "zone_quota_exceeded",
          detail: "You already have 500 zones.",
        },
        409,
      ),
    );

    const error = await api.createZone({}).then(
      () => null,
      (caught) => caught,
    );

    assert.ok(error instanceof ApiError);
    assert.equal(error.status, 409);
    assert.equal(error.code, "zone_quota_exceeded");
    assert.equal(error.message, "You already have 500 zones.");
  });

  it("falls back to the title when there is no detail", async () => {
    const { api } = clientWith(() =>
      problem({ title: "Not found", status: 404, code: "not_found" }, 404),
    );

    const error = await api.getZone("zone-a").catch((caught) => caught);
    assert.equal(error.message, "Not found");
  });

  it("still explains itself when the error body is not json", async () => {
    const { api } = clientWith(() => new Response("<html>502</html>", { status: 502 }));

    const error = await api.listZones().catch((caught) => caught);

    assert.equal(error.status, 502);
    assert.equal(error.code, "http_error");
    assert.match(error.message, /502/);
  });

  it("reports a dead network as an error the user can read", async () => {
    const { api } = clientWith(() => {
      throw new TypeError("Failed to fetch");
    });

    const error = await api.listZones().catch((caught) => caught);

    assert.ok(error instanceof ApiError);
    assert.equal(error.code, "network_unreachable");
    assert.equal(error.status, 0);
    assert.match(error.message, /server/i);
  });

  it("surfaces validation failures with the field that was rejected", async () => {
    const { api } = clientWith(() =>
      problem(
        {
          title: "Request validation failed",
          status: 422,
          code: "validation_error",
          errors: [{ loc: ["body", "radius_m"], msg: "Input should be less than 50000" }],
        },
        422,
      ),
    );

    const error = await api.createZone({}).catch((caught) => caught);

    assert.equal(error.code, "validation_error");
    assert.match(error.message, /radius_m/);
    assert.match(error.message, /less than 50000/);
  });

  it("marks an authentication failure so the caller can log out", async () => {
    const { api } = clientWith(() =>
      problem({ title: "Authentication required", status: 401, code: "unauthorized" }, 401),
    );

    const error = await api.me().catch((caught) => caught);
    assert.ok(error.isUnauthorized);
  });
});

describe("ApiClient login", () => {
  it("lowercases the username and keeps the token for later calls", async () => {
    const { api, calls } = clientWith(() =>
      json({
        access_token: "jwt-value",
        token_type: "bearer",
        expires_in: 86_400,
        user: { id: "u1", username: "nazar" },
      }),
    );

    const session = await api.login("  Nazar  ");

    assert.deepEqual(JSON.parse(calls[0].options.body), { username: "nazar" });
    assert.equal(session.user.username, "nazar");
    assert.equal(api.token, "jwt-value");
  });
});
