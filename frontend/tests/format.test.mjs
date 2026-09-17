import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  formatClock,
  formatCoordinate,
  formatCount,
  formatDistance,
  formatRate,
  relativeAge,
} from "../js/format.js";

describe("relativeAge", () => {
  it("reads as a short age, not a sentence", () => {
    assert.equal(relativeAge(0), "now");
    assert.equal(relativeAge(900), "now");
    assert.equal(relativeAge(5_400), "5s");
    assert.equal(relativeAge(95_000), "1m");
    assert.equal(relativeAge(3_600_000), "1h");
    assert.equal(relativeAge(90_000_000), "1d");
  });

  it("treats a clock that runs slightly ahead as the present", () => {
    assert.equal(relativeAge(-2_000), "now");
  });

  it("says so when there is no timestamp at all", () => {
    assert.equal(relativeAge(null), "—");
    assert.equal(relativeAge(Number.NaN), "—");
  });
});

describe("formatCoordinate", () => {
  it("keeps five decimals, which is about a metre", () => {
    assert.equal(formatCoordinate(50.4501234), "50.45012");
    assert.equal(formatCoordinate(-0.1), "-0.10000");
  });

  it("does not invent a value it was not given", () => {
    assert.equal(formatCoordinate(undefined), "—");
  });
});

describe("formatDistance", () => {
  it("switches to kilometres where metres stop being readable", () => {
    assert.equal(formatDistance(85), "85 m");
    assert.equal(formatDistance(999), "999 m");
    assert.equal(formatDistance(1_000), "1.0 km");
    assert.equal(formatDistance(12_345), "12.3 km");
  });
});

describe("formatCount", () => {
  it("groups thousands", () => {
    assert.equal(formatCount(0), "0");
    assert.equal(formatCount(10_000), "10,000");
  });

  it("shows a dash rather than NaN", () => {
    assert.equal(formatCount(undefined), "—");
  });
});

describe("formatRate", () => {
  it("keeps one decimal below ten and none above", () => {
    assert.equal(formatRate(0), "0");
    assert.equal(formatRate(4.25), "4.3");
    assert.equal(formatRate(1_234.5), "1,235");
  });
});

describe("formatClock", () => {
  it("renders a 24-hour wall clock", () => {
    assert.equal(formatClock("2026-09-17T14:03:22Z", { timeZone: "UTC" }), "14:03:22");
  });

  it("tolerates a value that is not a date", () => {
    assert.equal(formatClock("not a date", { timeZone: "UTC" }), "—");
    assert.equal(formatClock(null), "—");
  });
});
