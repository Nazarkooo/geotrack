import assert from "node:assert/strict";
import { beforeEach, describe, it } from "node:test";

import { RealtimeClient, websocketUrl } from "../js/ws.js";

class FakeSocket {
  static instances = [];

  constructor(url, protocols) {
    this.url = url;
    this.protocols = protocols;
    this.readyState = 0;
    this.sent = [];
    this.closedWith = null;
    FakeSocket.instances.push(this);
  }

  send(data) {
    if (this.readyState !== 1) throw new Error("send on a socket that is not open");
    this.sent.push(data);
  }

  close(code, reason) {
    this.readyState = 3;
    this.closedWith = { code, reason };
  }

  // --- driven by the tests ------------------------------------------------

  accept() {
    this.readyState = 1;
    this.onopen?.({});
  }

  deliver(payload) {
    const data = typeof payload === "string" ? payload : JSON.stringify(payload);
    this.onmessage?.({ data });
  }

  deliverBinary(payload) {
    this.onmessage?.({ data: new TextEncoder().encode(JSON.stringify(payload)).buffer });
  }

  drop(code = 1006, reason = "") {
    this.readyState = 3;
    this.onclose?.({ code, reason, wasClean: false });
  }

  get parsedSent() {
    return this.sent.map((frame) => JSON.parse(frame));
  }
}

class ManualTimers {
  #pending = new Map();
  #next = 1;
  now = 0;

  setTimeout = (fn, delay) => {
    const handle = this.#next++;
    this.#pending.set(handle, { fn, at: this.now + delay });
    return handle;
  };

  clearTimeout = (handle) => {
    this.#pending.delete(handle);
  };

  setInterval = (fn, delay) => {
    const handle = this.#next++;
    this.#pending.set(handle, { fn, at: this.now + delay, every: delay });
    return handle;
  };

  clearInterval = (handle) => {
    this.#pending.delete(handle);
  };

  advance(ms) {
    const target = this.now + ms;
    for (;;) {
      const due = [...this.#pending.entries()]
        .filter(([, timer]) => timer.at <= target)
        .sort((a, b) => a[1].at - b[1].at);
      if (due.length === 0) break;
      const [handle, timer] = due[0];
      this.now = timer.at;
      if (timer.every) timer.at += timer.every;
      else this.#pending.delete(handle);
      timer.fn();
    }
    this.now = target;
  }

  get pendingCount() {
    return this.#pending.size;
  }
}

const HELLO = {
  type: "hello",
  protocol: 1,
  session_id: "11111111-1111-1111-1111-111111111111",
  user: { id: "u1", username: "nazar" },
  tick_ms: 250,
  server_t: 1_000,
};

function build(overrides = {}) {
  const timers = new ManualTimers();
  const events = [];
  const client = new RealtimeClient({
    url: "ws://localhost/ws",
    token: "jwt-value",
    WebSocketImpl: FakeSocket,
    timers,
    now: () => timers.now,
    random: () => 0.5,
    ...overrides,
  });
  for (const type of [
    "status",
    "hello",
    "positions",
    "alert",
    "zone",
    "sessions",
    "stats",
    "rtt",
    "frame-error",
    "unauthorized",
    "session-limit",
    "slow-consumer",
  ]) {
    client.on(type, (payload) => events.push({ type, payload }));
  }
  return { client, timers, events, typesOf: () => events.map((event) => event.type) };
}

const latest = () => FakeSocket.instances[FakeSocket.instances.length - 1];

beforeEach(() => {
  FakeSocket.instances.length = 0;
});

describe("websocketUrl", () => {
  it("keeps the page's origin and upgrades the scheme", () => {
    assert.equal(websocketUrl({ protocol: "http:", host: "localhost:8080" }), "ws://localhost:8080/ws");
    assert.equal(websocketUrl({ protocol: "https:", host: "geo.example" }), "wss://geo.example/ws");
  });
});

describe("RealtimeClient handshake", () => {
  it("offers the protocol and the bearer token as subprotocols", () => {
    const { client } = build();
    client.connect();

    assert.deepEqual(latest().protocols, ["geotrack.v1", "bearer.jwt-value"]);
    assert.equal(latest().url, "ws://localhost/ws");
  });

  it("goes live only once the hello frame arrives", () => {
    const { client, typesOf } = build();
    client.connect();
    assert.equal(client.state, "connecting");

    latest().accept();
    assert.equal(client.state, "connecting");

    latest().deliver(HELLO);
    assert.equal(client.state, "live");
    assert.equal(client.sessionId, HELLO.session_id);
    assert.ok(typesOf().includes("hello"));
  });

  it("sends the viewport chosen before the socket existed", () => {
    const { client } = build();
    client.setViewport([30, 50, 31, 51]);
    client.connect();
    latest().accept();
    latest().deliver(HELLO);

    assert.deepEqual(latest().parsedSent[0], { type: "viewport", bbox: [30, 50, 31, 51] });
  });

  it("does not repeat a viewport that did not move", () => {
    const { client } = build();
    client.connect();
    latest().accept();
    latest().deliver(HELLO);

    client.setViewport([30, 50, 31, 51]);
    client.setViewport([30, 50, 31, 51]);
    client.setViewport([30, 50, 31, 51.5]);

    assert.equal(latest().parsedSent.filter((frame) => frame.type === "viewport").length, 2);
  });

  it("never writes to a socket that is not open", () => {
    const { client } = build();
    client.connect();
    client.setViewport([30, 50, 31, 51]);

    assert.deepEqual(latest().sent, []);
  });
});

describe("RealtimeClient frames", () => {
  const live = () => {
    const context = build();
    context.client.connect();
    latest().accept();
    latest().deliver(HELLO);
    return context;
  };

  it("decodes a binary positions frame", () => {
    const { events } = live();
    latest().deliverBinary({
      type: "positions",
      full: true,
      t: 2_000,
      items: [["dev-1", 50.45, 30.52, 1_900]],
      removed: [],
    });

    const frame = events.find((event) => event.type === "positions").payload;
    assert.equal(frame.full, true);
    assert.deepEqual(frame.items, [["dev-1", 50.45, 30.52, 1_900]]);
  });

  it("routes alert, zone, sessions and stats frames", () => {
    const { events } = live();
    latest().deliver({ type: "alert", alert: { id: 1 } });
    latest().deliver({ type: "zone", op: "created", zone: { id: "z1" } });
    latest().deliver({ type: "sessions", count: 2, sessions: [] });
    latest().deliver({ type: "stats", t: 1, devices: 10, updates_per_s: 4, connections: 2, backlog: 0 });

    const routed = events.filter((event) => ["alert", "zone", "sessions", "stats"].includes(event.type));
    assert.deepEqual(routed.map((event) => event.type), ["alert", "zone", "sessions", "stats"]);
  });

  it("measures the round trip from its own ping", () => {
    const { timers, events } = live();
    timers.advance(5_000);

    const ping = latest().parsedSent.find((frame) => frame.type === "ping");
    assert.ok(ping, "no ping was sent");

    timers.advance(40);
    latest().deliver({ type: "pong", t: ping.t, server_t: 1 });

    const rtt = events.find((event) => event.type === "rtt").payload;
    assert.equal(rtt.rttMs, 40);
  });

  it("reports a malformed frame without tearing the socket down", () => {
    const { client, events } = live();
    latest().deliver("{not json");

    assert.equal(client.state, "live");
    assert.ok(events.some((event) => event.type === "frame-error"));
  });

  it("ignores a frame type it does not know", () => {
    const { client, events } = live();
    latest().deliver({ type: "from-the-future" });

    assert.equal(client.state, "live");
    assert.ok(!events.some((event) => event.type === "frame-error"));
  });
});

describe("RealtimeClient reconnection", () => {
  it("starts from the shortest delay again after a connection that worked", () => {
    const { client, timers } = build({ backoff: { baseMs: 500, factor: 2, maxMs: 4_000, jitter: 0 } });
    client.connect();

    const delays = [];
    for (let attempt = 0; attempt < 6; attempt += 1) {
      latest().accept();
      latest().deliver(HELLO);
      const before = FakeSocket.instances.length;
      latest().drop();
      const waited = client.retryInMs;
      delays.push(waited);
      timers.advance(waited);
      assert.equal(FakeSocket.instances.length, before + 1, "a new socket should have been opened");
    }

    assert.deepEqual(delays, [500, 500, 500, 500, 500, 500]);
  });

  it("backs off further while connections keep failing", () => {
    const { client, timers } = build({ backoff: { baseMs: 500, factor: 2, maxMs: 4_000, jitter: 0 } });
    client.connect();

    const delays = [];
    for (let attempt = 0; attempt < 5; attempt += 1) {
      latest().drop();
      delays.push(client.retryInMs);
      timers.advance(client.retryInMs);
    }

    assert.deepEqual(delays, [500, 1_000, 2_000, 4_000, 4_000]);
  });

  it("spreads retries with jitter so replicas are not stampeded together", () => {
    const rolls = [0, 1];
    const { client } = build({
      backoff: { baseMs: 1_000, factor: 2, maxMs: 10_000, jitter: 0.5 },
      random: () => rolls.shift() ?? 0.5,
    });
    client.connect();

    latest().drop();
    const low = client.retryInMs;
    assert.equal(low, 500);
  });

  it("announces the wait so the header can count down", () => {
    const { client, events } = build({ backoff: { baseMs: 800, factor: 2, maxMs: 4_000, jitter: 0 } });
    client.connect();
    latest().drop();

    const status = events.filter((event) => event.type === "status").pop().payload;
    assert.equal(status.state, "reconnecting");
    assert.equal(status.retryInMs, 800);
    assert.equal(status.attempt, 1);
  });

  it("replays the viewport on the new connection", () => {
    const { client, timers } = build({ backoff: { baseMs: 100, factor: 2, maxMs: 100, jitter: 0 } });
    client.connect();
    latest().accept();
    latest().deliver(HELLO);
    client.setViewport([30, 50, 31, 51]);

    latest().drop();
    timers.advance(100);
    latest().accept();
    latest().deliver(HELLO);

    assert.deepEqual(latest().parsedSent[0], { type: "viewport", bbox: [30, 50, 31, 51] });
  });

  it("tells the app a session was resumed so it can backfill", () => {
    const { client, timers, events } = build({ backoff: { baseMs: 100, factor: 2, maxMs: 100, jitter: 0 } });
    client.connect();
    latest().accept();
    latest().deliver(HELLO);
    latest().drop();
    timers.advance(100);
    latest().accept();
    latest().deliver(HELLO);

    const hellos = events.filter((event) => event.type === "hello");
    assert.deepEqual(hellos.map((event) => event.payload.resumed), [false, true]);
  });

  it("stops retrying when the token was rejected", () => {
    const { client, timers, typesOf } = build();
    client.connect();
    latest().drop(4401, "invalid token");

    assert.ok(typesOf().includes("unauthorized"));
    assert.equal(client.state, "closed");
    timers.advance(60_000);
    assert.equal(FakeSocket.instances.length, 1);
  });

  it("stops retrying when the per-user session cap is reached", () => {
    const { client, timers, typesOf } = build();
    client.connect();
    latest().drop(4429, "too many sessions");

    assert.ok(typesOf().includes("session-limit"));
    timers.advance(60_000);
    assert.equal(FakeSocket.instances.length, 1);
  });

  it("reconnects after being dropped as a slow consumer", () => {
    const { client, timers, typesOf } = build({ backoff: { baseMs: 100, factor: 2, maxMs: 100, jitter: 0 } });
    client.connect();
    latest().accept();
    latest().deliver(HELLO);
    latest().drop(1013, "slow consumer");

    assert.ok(typesOf().includes("slow-consumer"));
    timers.advance(100);
    assert.equal(FakeSocket.instances.length, 2);
  });

  it("stays down after an explicit close and leaves no timers behind", () => {
    const { client, timers } = build();
    client.connect();
    latest().accept();
    latest().deliver(HELLO);

    client.close();

    assert.equal(client.state, "closed");
    assert.equal(latest().closedWith.code, 1000);
    timers.advance(60_000);
    assert.equal(FakeSocket.instances.length, 1);
    assert.equal(timers.pendingCount, 0);
  });

  it("can be restarted after the user logs back in", () => {
    const { client } = build();
    client.connect();
    client.close();
    client.setToken("another-jwt");
    client.connect();

    assert.equal(FakeSocket.instances.length, 2);
    assert.deepEqual(latest().protocols, ["geotrack.v1", "bearer.another-jwt"]);
    assert.equal(client.state, "connecting");
  });

  it("ignores a second connect while one is already in flight", () => {
    const { client } = build();
    client.connect();
    client.connect();

    assert.equal(FakeSocket.instances.length, 1);
  });
});
