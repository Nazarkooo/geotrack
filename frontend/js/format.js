/** Display helpers. Every one of them answers "—" rather than showing NaN to a user. */

const EMPTY = "—";

const counts = new Intl.NumberFormat("en-US");
const rates = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 });
const wholeRates = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

export function relativeAge(ageMs) {
  if (ageMs === null || ageMs === undefined || Number.isNaN(ageMs)) return EMPTY;
  // Device clocks drift; a report from "the future" is still the latest one we have.
  const age = Math.max(0, ageMs);
  if (age < 1_000) return "now";
  if (age < 60_000) return `${Math.floor(age / 1_000)}s`;
  if (age < 3_600_000) return `${Math.floor(age / 60_000)}m`;
  if (age < 86_400_000) return `${Math.floor(age / 3_600_000)}h`;
  return `${Math.floor(age / 86_400_000)}d`;
}

export function formatCoordinate(value, digits = 5) {
  return Number.isFinite(value) ? value.toFixed(digits) : EMPTY;
}

export function formatDistance(metres) {
  if (!Number.isFinite(metres)) return EMPTY;
  if (metres < 1_000) return `${Math.round(metres)} m`;
  return `${(metres / 1_000).toFixed(1)} km`;
}

export function formatCount(value) {
  return Number.isFinite(value) ? counts.format(value) : EMPTY;
}

export function formatRate(value) {
  if (!Number.isFinite(value)) return EMPTY;
  return value < 10 ? rates.format(value) : wholeRates.format(value);
}

export function formatClock(value, { timeZone } = {}) {
  if (value === null || value === undefined) return EMPTY;
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return EMPTY;
  return new Intl.DateTimeFormat("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    ...(timeZone ? { timeZone } : {}),
  }).format(date);
}
