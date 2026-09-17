import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  MAX_RADIUS_M,
  MIN_RADIUS_M,
  bboxEquals,
  boundsToBBox,
  circleMoved,
  circleRing,
  clampRadius,
  destination,
  haversineMeters,
  ringToCircle,
  wrapLongitude,
} from "../js/geo.js";

describe("haversineMeters", () => {
  it("is zero for the same point", () => {
    assert.equal(haversineMeters([30.5, 50.45], [30.5, 50.45]), 0);
  });

  it("matches a known great-circle distance", () => {
    // Kyiv (Maidan) to Lviv (Rynok), ~468 km per geodesic references.
    const metres = haversineMeters([30.5238, 50.4501], [24.0316, 49.8419]);
    assert.ok(Math.abs(metres - 468_000) < 6_000, `got ${metres}`);
  });

  it("measures one degree of latitude as about 111 km", () => {
    const metres = haversineMeters([0, 0], [0, 1]);
    assert.ok(Math.abs(metres - 111_195) < 50, `got ${metres}`);
  });

  it("takes the short way across the antimeridian", () => {
    const metres = haversineMeters([179.99, 0], [-179.99, 0]);
    assert.ok(metres < 2_500, `got ${metres}`);
  });
});

describe("destination", () => {
  it("lands exactly the requested distance away", () => {
    const origin = [30.5, 50.45];
    for (const bearing of [0, 45, 90, 180, 271, 359]) {
      const target = destination(origin, bearing, 1_500);
      const error = Math.abs(haversineMeters(origin, target) - 1_500);
      assert.ok(error < 0.5, `bearing ${bearing}: error ${error}`);
    }
  });

  it("moves north for bearing 0", () => {
    const [lon, lat] = destination([30.5, 50.45], 0, 1_000);
    assert.ok(lat > 50.45);
    assert.ok(Math.abs(lon - 30.5) < 1e-9);
  });
});

describe("circleRing / ringToCircle", () => {
  it("produces a closed ring whose first vertex is due north", () => {
    const ring = circleRing([30.5, 50.45], 800, 80);
    assert.equal(ring.length, 81);
    assert.deepEqual(ring[0], ring[ring.length - 1]);
    assert.ok(ring[0][1] > 50.45);
  });

  it("keeps every vertex on the circle", () => {
    const centre = [30.5, 50.45];
    for (const vertex of circleRing(centre, 2_500, 64)) {
      assert.ok(Math.abs(haversineMeters(centre, vertex) - 2_500) < 0.5);
    }
  });

  it("round-trips a ring back to its centre and radius", () => {
    const centre = [30.5238, 50.4501];
    const circle = ringToCircle(circleRing(centre, 1_234, 80), centre);
    assert.ok(Math.abs(circle.radiusM - 1_234) < 0.5, `radius ${circle.radiusM}`);
    assert.deepEqual(circle.center, centre);
  });

  it("recovers the centre from the ring alone when none is supplied", () => {
    const centre = [30.5238, 50.4501];
    const circle = ringToCircle(circleRing(centre, 900, 80), null);
    assert.ok(haversineMeters(circle.center, centre) < 2, "centre drifted");
    assert.ok(Math.abs(circle.radiusM - 900) < 5, `radius ${circle.radiusM}`);
  });

  it("survives a ring that crosses the antimeridian", () => {
    const centre = [179.98, 10];
    const circle = ringToCircle(circleRing(centre, 3_000, 80), null);
    assert.ok(haversineMeters(circle.center, centre) < 10, `centre ${circle.center}`);
    assert.ok(Math.abs(circle.radiusM - 3_000) < 20, `radius ${circle.radiusM}`);
  });
});

describe("clampRadius", () => {
  it("keeps the radius inside the range the API accepts", () => {
    assert.equal(clampRadius(1), MIN_RADIUS_M);
    assert.equal(clampRadius(1e9), MAX_RADIUS_M);
    assert.equal(clampRadius(1_234.56789), 1_234.57);
    assert.equal(clampRadius(Number.NaN), MIN_RADIUS_M);
  });
});

describe("wrapLongitude", () => {
  it("brings any longitude into [-180, 180]", () => {
    assert.equal(wrapLongitude(0), 0);
    assert.equal(wrapLongitude(190), -170);
    assert.equal(wrapLongitude(-190), 170);
    assert.equal(wrapLongitude(540), 180);
    assert.equal(wrapLongitude(180), 180);
    assert.equal(wrapLongitude(-180), -180);
  });
});

describe("boundsToBBox", () => {
  const bounds = (west, south, east, north) => ({
    getWest: () => west,
    getSouth: () => south,
    getEast: () => east,
    getNorth: () => north,
  });

  it("returns west, south, east, north", () => {
    assert.deepEqual(boundsToBBox(bounds(30.3, 50.3, 30.7, 50.6)), [30.3, 50.3, 30.7, 50.6]);
  });

  it("clamps latitudes to the valid range", () => {
    const [, south, , north] = boundsToBBox(bounds(-10, -95, 10, 95));
    assert.equal(south, -90);
    assert.equal(north, 90);
  });

  it("collapses a viewport wider than the world to the whole world", () => {
    assert.deepEqual(boundsToBBox(bounds(-400, -60, 400, 60)), [-180, -60, 180, 60]);
  });

  it("keeps an antimeridian-crossing viewport as west > east", () => {
    const [west, , east] = boundsToBBox(bounds(170, 0, 190, 10));
    assert.equal(west, 170);
    assert.equal(east, -170);
  });

  it("normalises a viewport panned past the world edge", () => {
    assert.deepEqual(boundsToBBox(bounds(370, 0, 380, 10)), [10, 0, 20, 10]);
  });

  it("does not turn an eastern edge of exactly 180 into -180", () => {
    assert.deepEqual(boundsToBBox(bounds(170, 0, 180, 10)), [170, 0, 180, 10]);
  });
});

describe("bboxEquals", () => {
  it("treats sub-metre differences as equal", () => {
    assert.ok(bboxEquals([30, 50, 31, 51], [30.0000001, 50, 31, 51]));
  });

  it("detects a real pan", () => {
    assert.ok(!bboxEquals([30, 50, 31, 51], [30.01, 50, 31, 51]));
  });

  it("handles a missing box", () => {
    assert.ok(!bboxEquals(null, [30, 50, 31, 51]));
    assert.ok(!bboxEquals([30, 50, 31, 51], null));
  });
});

describe("circleMoved", () => {
  const at = (lon, lat, radiusM) => ({ center: [lon, lat], radiusM });

  it("ignores a gesture that ended where it started", () => {
    const zone = at(30.5, 50.45, 800);
    assert.equal(circleMoved(zone, at(30.5, 50.45, 800)), false);
  });

  it("ignores the rounding a re-read ring introduces", () => {
    // Reading a circle back out of an 80-point ring recovers the radius to a few
    // centimetres; that must not read as an edit.
    const zone = at(30.5, 50.45, 800);
    const reread = ringToCircle(circleRing(zone.center, zone.radiusM, 80), zone.center);
    assert.equal(circleMoved(zone, reread), false);
  });

  it("sees a centre that moved further than the tolerance", () => {
    const zone = at(30.5, 50.45, 800);
    assert.equal(circleMoved(zone, at(...destination(zone.center, 90, 3), 800)), true);
  });

  it("sees a radius that changed further than the tolerance", () => {
    const zone = at(30.5, 50.45, 800);
    assert.equal(circleMoved(zone, at(30.5, 50.45, 812)), true);
    assert.equal(circleMoved(zone, at(30.5, 50.45, 788)), true);
  });

  it("holds a sub-tolerance nudge on both axes at once", () => {
    const zone = at(30.5, 50.45, 800);
    const nudged = at(...destination(zone.center, 45, 0.2), 800.2);
    assert.equal(circleMoved(zone, nudged), false);
  });

  it("takes an explicit tolerance", () => {
    const zone = at(30.5, 50.45, 800);
    assert.equal(circleMoved(zone, at(30.5, 50.45, 802), 5), false);
    assert.equal(circleMoved(zone, at(30.5, 50.45, 802), 1), true);
  });
});
