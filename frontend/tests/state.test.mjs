import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { Emitter, PresenceTracker, TokenStore } from "../js/state.js";

const alert = (kind, zoneId, deviceId) => ({
  id: 1,
  kind,
  zone: { id: zoneId, name: "Depot" },
  device_id: deviceId,
});

describe("PresenceTracker", () => {
  it("loads the presence snapshot the API returns", () => {
    const presence = new PresenceTracker();
    presence.setSnapshot({ "zone-a": ["dev-1", "dev-2"], "zone-b": ["dev-2"] });

    assert.deepEqual([...presence.deviceIds].sort(), ["dev-1", "dev-2"]);
    assert.deepEqual([...presence.devicesIn("zone-a")].sort(), ["dev-1", "dev-2"]);
    assert.equal(presence.devicesIn("missing").size, 0);
  });

  it("adds a device on an enter alert", () => {
    const presence = new PresenceTracker();
    assert.equal(presence.applyAlert(alert("enter", "zone-a", "dev-1")), true);
    assert.ok(presence.deviceIds.has("dev-1"));
  });

  it("reports no change when an enter alert repeats", () => {
    const presence = new PresenceTracker();
    presence.applyAlert(alert("enter", "zone-a", "dev-1"));
    assert.equal(presence.applyAlert(alert("dwell", "zone-a", "dev-1")), false);
  });

  it("removes a device on an exit alert", () => {
    const presence = new PresenceTracker();
    presence.applyAlert(alert("enter", "zone-a", "dev-1"));
    assert.equal(presence.applyAlert(alert("exit", "zone-a", "dev-1")), true);
    assert.ok(!presence.deviceIds.has("dev-1"));
  });

  it("keeps a device highlighted while it is still inside another zone", () => {
    const presence = new PresenceTracker();
    presence.setSnapshot({ "zone-a": ["dev-1"], "zone-b": ["dev-1"] });

    assert.equal(presence.applyAlert(alert("exit", "zone-a", "dev-1")), false);
    assert.ok(presence.deviceIds.has("dev-1"));
    assert.deepEqual([...presence.zonesOf("dev-1")], ["zone-b"]);
  });

  it("ignores an alert whose zone has already been deleted", () => {
    const presence = new PresenceTracker();
    assert.equal(presence.applyAlert(alert("enter", null, "dev-1")), false);
    assert.equal(presence.deviceIds.size, 0);
  });

  it("drops a zone's devices when the zone goes away", () => {
    const presence = new PresenceTracker();
    presence.setSnapshot({ "zone-a": ["dev-1"], "zone-b": ["dev-2"] });

    assert.equal(presence.removeZone("zone-a"), true);
    assert.deepEqual([...presence.deviceIds], ["dev-2"]);
    assert.equal(presence.removeZone("zone-a"), false);
  });

  it("forgets everything on logout", () => {
    const presence = new PresenceTracker();
    presence.setSnapshot({ "zone-a": ["dev-1"] });
    presence.clear();
    assert.equal(presence.deviceIds.size, 0);
  });
});

describe("Emitter", () => {
  it("delivers an event to every listener", () => {
    const emitter = new Emitter();
    const seen = [];
    emitter.on("tick", (value) => seen.push(`a:${value}`));
    emitter.on("tick", (value) => seen.push(`b:${value}`));

    emitter.emit("tick", 1);

    assert.deepEqual(seen, ["a:1", "b:1"]);
  });

  it("stops delivering once a listener unsubscribes", () => {
    const emitter = new Emitter();
    const seen = [];
    const off = emitter.on("tick", (value) => seen.push(value));
    emitter.emit("tick", 1);
    off();
    emitter.emit("tick", 2);

    assert.deepEqual(seen, [1]);
  });

  it("lets a listener unsubscribe from inside its own callback", () => {
    const emitter = new Emitter();
    const seen = [];
    const off = emitter.on("tick", (value) => {
      seen.push(value);
      off();
    });
    emitter.on("tick", (value) => seen.push(`other:${value}`));

    emitter.emit("tick", 1);
    emitter.emit("tick", 2);

    assert.deepEqual(seen, [1, "other:1", "other:2"]);
  });

  it("does not let one broken listener stop the others", () => {
    const emitter = new Emitter();
    const seen = [];
    const errors = [];
    emitter.on("tick", () => {
      throw new Error("boom");
    });
    emitter.on("tick", (value) => seen.push(value));

    emitter.emit("tick", 1, (error) => errors.push(error.message));

    assert.deepEqual(seen, [1]);
    assert.deepEqual(errors, ["boom"]);
  });
});

class FakeStorage {
  #data = new Map();
  getItem(key) {
    return this.#data.has(key) ? this.#data.get(key) : null;
  }
  setItem(key, value) {
    this.#data.set(key, String(value));
  }
  removeItem(key) {
    this.#data.delete(key);
  }
}

describe("TokenStore", () => {
  it("round-trips a token", () => {
    const tokens = new TokenStore(new FakeStorage());
    assert.equal(tokens.get(), null);
    tokens.set("jwt-value");
    assert.equal(tokens.get(), "jwt-value");
    tokens.clear();
    assert.equal(tokens.get(), null);
  });

  it("survives a browser that refuses storage", () => {
    const denied = {
      getItem() {
        throw new Error("denied");
      },
      setItem() {
        throw new Error("denied");
      },
      removeItem() {
        throw new Error("denied");
      },
    };
    const tokens = new TokenStore(denied);

    tokens.set("jwt-value");

    // Without persistence the session still works, it just does not survive a reload.
    assert.equal(tokens.get(), "jwt-value");
    tokens.clear();
    assert.equal(tokens.get(), null);
  });
});
