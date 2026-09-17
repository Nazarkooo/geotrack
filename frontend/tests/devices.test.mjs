import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { DeviceStore } from "../js/devices.js";

const item = (id, lat, lon, ms) => [id, lat, lon, ms];

const positionOf = (store, id) => {
  const slot = store.slotOf(id);
  const { target } = store.buffers;
  return [target[slot * 2], target[slot * 2 + 1]];
};

const drawnAt = (store, id) => {
  const slot = store.slotOf(id);
  const { drawn } = store.buffers;
  return [drawn[slot * 2], drawn[slot * 2 + 1]];
};

const colourOf = (store, id) => {
  const slot = store.slotOf(id);
  const { colors } = store.buffers;
  return Array.from(colors.subarray(slot * 4, slot * 4 + 4));
};

const radiusOf = (store, id) => store.buffers.radii[store.slotOf(id)];

describe("DeviceStore.apply", () => {
  it("adds devices and reports them through the layer payload", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50.45, 30.52, 1_000), item("dev-2", 50.46, 30.53, 1_000)]);

    assert.equal(store.size, 2);
    const data = store.layerData();
    assert.equal(data.length, 2);
    assert.equal(data.attributes.getPosition.value.length, 4);
    assert.equal(data.attributes.getFillColor.value.length, 8);
    assert.equal(data.attributes.getRadius.value.length, 2);
    assert.deepEqual(positionOf(store, "dev-1"), [30.52, 50.45]);
    assert.ok(radiusOf(store, "dev-1") > 0);
  });

  it("keeps a device in the same slot across updates so markers can glide", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50.45, 30.52, 1_000)]);
    const slot = store.slotOf("dev-1");

    store.apply([item("dev-1", 50.46, 30.53, 2_000)]);

    assert.equal(store.slotOf("dev-1"), slot);
    assert.deepEqual(positionOf(store, "dev-1"), [30.53, 50.46]);
  });

  it("ignores a report older than the one already held", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50.45, 30.52, 5_000)]);
    store.apply([item("dev-1", 10, 10, 4_999)]);

    assert.deepEqual(positionOf(store, "dev-1"), [30.52, 50.45]);
    assert.equal(store.get("dev-1").reportedMs, 5_000);
  });

  it("drops devices missing from a full snapshot", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50.45, 30.52, 1_000), item("dev-2", 50.46, 30.53, 1_000)]);

    store.apply([item("dev-2", 50.47, 30.54, 2_000)], { full: true });

    assert.equal(store.size, 1);
    assert.equal(store.get("dev-1"), null);
    assert.deepEqual(positionOf(store, "dev-2"), [30.54, 50.47]);
  });

  it("keeps devices missing from a delta", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50.45, 30.52, 1_000), item("dev-2", 50.46, 30.53, 1_000)]);
    store.apply([item("dev-2", 50.47, 30.54, 2_000)]);

    assert.equal(store.size, 2);
    assert.deepEqual(positionOf(store, "dev-1"), [30.52, 50.45]);
  });

  it("grows past its initial capacity without losing anything", () => {
    const store = new DeviceStore({ capacity: 2 });
    const items = [];
    for (let i = 0; i < 50; i += 1) items.push(item(`dev-${i}`, 50 + i / 1000, 30 + i / 1000, 1_000));
    store.apply(items);

    assert.equal(store.size, 50);
    assert.equal(store.layerData().length, 50);
    assert.deepEqual(positionOf(store, "dev-49"), [30.049, 50.049]);
    assert.ok(radiusOf(store, "dev-0") > 0);
  });
});

describe("DeviceStore.remove", () => {
  it("hides the device in place instead of renumbering its neighbours", () => {
    const store = new DeviceStore();
    store.apply([
      item("dev-1", 50.45, 30.52, 1_000),
      item("dev-2", 50.46, 30.53, 1_000),
      item("dev-3", 50.47, 30.54, 1_000),
    ]);
    const slots = ["dev-1", "dev-2", "dev-3"].map((id) => store.slotOf(id));

    store.remove(["dev-1"]);

    assert.equal(store.size, 2);
    assert.equal(store.get("dev-1"), null);
    assert.deepEqual(["dev-1", "dev-2", "dev-3"].map((id) => store.slotOf(id)), slots);
    // Invisible, and with no radius it cannot be picked either.
    assert.equal(radiusOf(store, "dev-1"), 0);
    assert.equal(colourOf(store, "dev-1")[3], 0);
  });

  it("ignores devices it never knew", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50.45, 30.52, 1_000)]);
    store.remove(["ghost"]);
    assert.equal(store.size, 1);
  });

  it("gives a returning device its own slot back", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50.45, 30.52, 1_000), item("dev-2", 50.46, 30.53, 1_000)]);
    const slot = store.slotOf("dev-1");
    store.remove(["dev-1"]);

    store.apply([item("dev-1", 51, 31, 2_000)]);

    assert.equal(store.size, 2);
    assert.equal(store.slotOf("dev-1"), slot);
    assert.deepEqual(positionOf(store, "dev-1"), [31, 51]);
    assert.ok(radiusOf(store, "dev-1") > 0);
    assert.equal(colourOf(store, "dev-1")[3] > 0, true);
  });

  it("does not shrink the drawn slot count", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50.45, 30.52, 1_000), item("dev-2", 50.46, 30.53, 1_000)]);
    store.remove(["dev-1", "dev-2"]);

    assert.equal(store.size, 0);
    assert.equal(store.layerData().length, 2);
  });
});

describe("DeviceStore.sweep", () => {
  it("drops devices that stopped reporting and names them", () => {
    const store = new DeviceStore({ staleMs: 1_000 });
    store.apply([item("fresh", 50.45, 30.52, 10_000), item("stale", 50.46, 30.53, 5_000)]);

    const dropped = store.sweep(10_500);

    assert.deepEqual(dropped, ["stale"]);
    assert.equal(store.size, 1);
    assert.ok(store.get("fresh"));
  });

  it("returns nothing when every device is current", () => {
    const store = new DeviceStore({ staleMs: 1_000 });
    store.apply([item("dev-1", 50.45, 30.52, 10_000)]);
    assert.deepEqual(store.sweep(10_500), []);
  });
});

describe("DeviceStore highlighting", () => {
  it("recolours the devices inside a zone and only those", () => {
    const store = new DeviceStore();
    store.apply([item("inside", 50.45, 30.52, 1_000), item("outside", 50.46, 30.53, 1_000)]);

    store.setHighlighted(new Set(["inside"]));

    assert.notDeepEqual(colourOf(store, "inside"), colourOf(store, "outside"));
    assert.deepEqual(positionOf(store, "inside"), [30.52, 50.45]);
  });

  it("colours a device that arrives while already highlighted", () => {
    const store = new DeviceStore();
    store.setHighlighted(new Set(["late"]));
    store.apply([item("late", 50.45, 30.52, 1_000), item("plain", 50.46, 30.53, 1_000)]);

    assert.notDeepEqual(colourOf(store, "late"), colourOf(store, "plain"));
  });

  it("returns to the plain colour when the device leaves every zone", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50.45, 30.52, 1_000)]);
    const plain = colourOf(store, "dev-1");

    store.setHighlighted(new Set(["dev-1"]));
    store.setHighlighted(new Set());

    assert.deepEqual(colourOf(store, "dev-1"), plain);
  });

  it("does not make a hidden device visible again", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50.45, 30.52, 1_000)]);
    store.remove(["dev-1"]);

    store.setHighlighted(new Set(["dev-1"]));

    assert.equal(colourOf(store, "dev-1")[3], 0);
    assert.equal(radiusOf(store, "dev-1"), 0);
  });
});

describe("DeviceStore.interpolate", () => {
  it("draws a device at its reported position the moment it appears", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50.45, 30.52, 1_000)], { nowMs: 0 });

    // No sliding in from wherever the slot last pointed.
    assert.deepEqual(drawnAt(store, "dev-1"), [30.52, 50.45]);
  });

  it("moves a marker part of the way and says it is still moving", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50, 30, 1_000)], { nowMs: 0 });
    store.apply([item("dev-1", 51, 32, 2_000)], { nowMs: 1_000 });

    assert.equal(store.interpolate(1_125, 250), true);

    const [lon, lat] = drawnAt(store, "dev-1");
    assert.ok(Math.abs(lon - 31) < 1e-9, `lon ${lon}`);
    assert.ok(Math.abs(lat - 50.5) < 1e-9, `lat ${lat}`);
  });

  it("arrives exactly and then reports nothing left to do", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50, 30, 1_000)], { nowMs: 0 });
    store.apply([item("dev-1", 51, 32, 2_000)], { nowMs: 1_000 });

    assert.equal(store.interpolate(1_250, 250), false);
    assert.deepEqual(drawnAt(store, "dev-1"), [32, 51]);
  });

  it("restarts from where the marker is when a report lands mid-glide", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50, 30, 1_000)], { nowMs: 0 });
    store.apply([item("dev-1", 52, 30, 2_000)], { nowMs: 1_000 });
    store.interpolate(1_125, 250);
    const halfway = drawnAt(store, "dev-1");

    store.apply([item("dev-1", 60, 30, 3_000)], { nowMs: 1_125 });
    store.interpolate(1_125, 250);

    // Continues from the halfway point rather than jumping back to the old report.
    assert.deepEqual(drawnAt(store, "dev-1"), halfway);
  });

  it("catches up after a tab was in the background", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50, 30, 1_000)], { nowMs: 0 });
    store.apply([item("dev-1", 51, 32, 2_000)], { nowMs: 1_000 });

    assert.equal(store.interpolate(600_000, 250), false);
    assert.deepEqual(drawnAt(store, "dev-1"), [32, 51]);
  });

  it("snaps everything into place when motion is switched off", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50, 30, 1_000)], { nowMs: 0 });
    store.apply([item("dev-1", 51, 32, 2_000)], { nowMs: 1_000 });

    store.snap();

    assert.deepEqual(drawnAt(store, "dev-1"), [32, 51]);
  });
});

describe("DeviceStore.deviceAt", () => {
  it("maps a picked slot back to the device it belongs to", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50.45, 30.52, 1_000), item("dev-2", 50.46, 30.53, 1_000)]);

    const picked = store.deviceAt(store.slotOf("dev-2"));

    assert.equal(picked.id, "dev-2");
    assert.equal(picked.latitude, 50.46);
    assert.equal(picked.longitude, 30.53);
    assert.equal(store.deviceAt(99), null);
    assert.equal(store.deviceAt(-1), null);
  });

  it("does not resolve a slot whose device went away", () => {
    const store = new DeviceStore();
    store.apply([item("dev-1", 50.45, 30.52, 1_000)]);
    const slot = store.slotOf("dev-1");
    store.remove(["dev-1"]);

    assert.equal(store.deviceAt(slot), null);
  });
});
