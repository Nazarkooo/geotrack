import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { AlertLog, ToastAggregator, matchesFilter } from "../js/alerts.js";

const alert = (id, overrides = {}) => ({
  id,
  kind: "enter",
  zone: { id: "zone-a", name: "Depot" },
  device_id: "dev-1",
  latitude: 50.45,
  longitude: 30.52,
  occurred_at: "2026-09-17T10:00:00Z",
  created_at: "2026-09-17T10:00:00Z",
  ...overrides,
});

describe("AlertLog", () => {
  it("keeps the newest alert first", () => {
    const log = new AlertLog();
    log.add(alert(1));
    log.add(alert(2));

    assert.deepEqual(log.items.map((a) => a.id), [2, 1]);
  });

  it("orders a backfill page correctly however it arrives", () => {
    const log = new AlertLog();
    log.addMany([alert(3), alert(1), alert(2)]);

    assert.deepEqual(log.items.map((a) => a.id), [3, 2, 1]);
  });

  it("refuses an id it already holds, so a reconnect backfill cannot duplicate", () => {
    const log = new AlertLog();
    assert.equal(log.add(alert(7)), true);
    assert.equal(log.add(alert(7)), false);
    assert.equal(log.items.length, 1);
  });

  it("tracks the highest id for after_id backfill", () => {
    const log = new AlertLog();
    assert.equal(log.lastId, null);
    log.addMany([alert(4), alert(9), alert(6)]);
    assert.equal(log.lastId, 9);
  });

  it("caps its length and drops the oldest rows", () => {
    const log = new AlertLog({ capacity: 3 });
    log.addMany([alert(1), alert(2), alert(3), alert(4)]);

    assert.deepEqual(log.items.map((a) => a.id), [4, 3, 2]);
    // The evicted id must be forgotten too, or the log could never take it back.
    assert.equal(log.add(alert(1)), true);
  });

  it("forgets everything on logout", () => {
    const log = new AlertLog();
    log.add(alert(1));
    log.clear();
    assert.equal(log.items.length, 0);
    assert.equal(log.lastId, null);
  });
});

describe("matchesFilter", () => {
  it("passes everything when nothing is selected", () => {
    assert.ok(matchesFilter(alert(1), {}));
  });

  it("filters by zone", () => {
    assert.ok(matchesFilter(alert(1), { zoneId: "zone-a" }));
    assert.ok(!matchesFilter(alert(1), { zoneId: "zone-b" }));
  });

  it("filters by kind", () => {
    assert.ok(matchesFilter(alert(1, { kind: "exit" }), { kind: "exit" }));
    assert.ok(!matchesFilter(alert(1, { kind: "exit" }), { kind: "enter" }));
  });

  it("matches an alert whose zone was deleted only when no zone is selected", () => {
    const orphan = alert(1, { zone: { id: null, name: "Depot" } });
    assert.ok(matchesFilter(orphan, {}));
    assert.ok(!matchesFilter(orphan, { zoneId: "zone-a" }));
  });
});

describe("ToastAggregator", () => {
  it("shows the first few alerts one by one", () => {
    const toasts = new ToastAggregator({ windowMs: 1_000, threshold: 3 });

    assert.deepEqual(toasts.offer(alert(1), 0), { kind: "single", alert: alert(1) });
    assert.deepEqual(toasts.offer(alert(2), 100), { kind: "single", alert: alert(2) });
  });

  it("stops showing individual toasts once the rate is exceeded", () => {
    const toasts = new ToastAggregator({ windowMs: 1_000, threshold: 3 });
    for (const [id, at] of [[1, 0], [2, 100], [3, 200]]) toasts.offer(alert(id), at);

    assert.equal(toasts.offer(alert(4), 300), null);
    assert.equal(toasts.offer(alert(5), 400), null);
  });

  it("reports what it swallowed when asked to flush", () => {
    const toasts = new ToastAggregator({ windowMs: 1_000, threshold: 2 });
    toasts.offer(alert(1), 0);
    toasts.offer(alert(2), 10);
    toasts.offer(alert(3), 20);
    toasts.offer(alert(4), 30);

    assert.deepEqual(toasts.flush(40), { kind: "aggregate", count: 2 });
    assert.equal(toasts.flush(50), null);
  });

  it("goes back to single toasts once the burst is over", () => {
    const toasts = new ToastAggregator({ windowMs: 1_000, threshold: 2 });
    toasts.offer(alert(1), 0);
    toasts.offer(alert(2), 10);
    assert.equal(toasts.offer(alert(3), 20), null);

    assert.deepEqual(toasts.offer(alert(4), 2_000), { kind: "single", alert: alert(4) });
  });

  it("has nothing pending before anything happened", () => {
    assert.equal(new ToastAggregator().flush(0), null);
  });
});
