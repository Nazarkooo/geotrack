/**
 * Alert history and the policy that decides what is worth interrupting the user for.
 *
 * The log is the client's copy of `GET /api/v1/alerts` plus everything the socket has
 * pushed since. It is keyed by the server's identity column, which is what makes the
 * reconnect backfill (`after_id`) safe to replay: an id already held is refused rather
 * than shown twice.
 */

import { formatClock } from "./format.js";

const DEFAULT_CAPACITY = 300;
const MAX_ROWS = 200;

export class AlertLog {
  #capacity;
  #items = [];
  #ids = new Set();
  #lastId = null;

  constructor({ capacity = DEFAULT_CAPACITY } = {}) {
    this.#capacity = capacity;
  }

  /** Newest first — the order the feed renders and the order the API returns. */
  get items() {
    return this.#items;
  }

  /** Highest id ever seen, including rows already evicted; drives `?after_id=`. */
  get lastId() {
    return this.#lastId;
  }

  add(alert) {
    if (this.#ids.has(alert.id)) return false;
    this.#ids.add(alert.id);
    this.#items.splice(this.#insertionIndex(alert.id), 0, alert);
    if (this.#lastId === null || alert.id > this.#lastId) this.#lastId = alert.id;
    while (this.#items.length > this.#capacity) {
      this.#ids.delete(this.#items.pop().id);
    }
    return true;
  }

  addMany(alerts) {
    return alerts.filter((alert) => this.add(alert));
  }

  clear() {
    this.#items = [];
    this.#ids.clear();
    this.#lastId = null;
  }

  #insertionIndex(id) {
    let low = 0;
    let high = this.#items.length;
    while (low < high) {
      const middle = (low + high) >> 1;
      if (this.#items[middle].id > id) low = middle + 1;
      else high = middle;
    }
    return low;
  }
}

export function matchesFilter(alert, { zoneId, kind } = {}) {
  if (zoneId && alert.zone?.id !== zoneId) return false;
  return !(kind && alert.kind !== kind);
}

/**
 * Turns a burst of alerts into one line instead of a wall of toasts.
 *
 * Ten thousand devices crossing a large zone produce alerts faster than anyone can
 * read them, so past a few per second the individual toasts stop and the caller's
 * timer collects them into a single "N new alerts".
 */
export class ToastAggregator {
  #windowMs;
  #threshold;
  #recent = [];
  #suppressed = 0;

  constructor({ windowMs = 1_000, threshold = 5 } = {}) {
    this.#windowMs = windowMs;
    this.#threshold = threshold;
  }

  offer(alert, nowMs = Date.now()) {
    this.#prune(nowMs);
    this.#recent.push(nowMs);
    if (this.#recent.length > this.#threshold) {
      this.#suppressed += 1;
      return null;
    }
    return { kind: "single", alert };
  }

  /** What the burst swallowed since the last flush, or null if nothing did. */
  flush(nowMs = Date.now()) {
    this.#prune(nowMs);
    if (this.#suppressed === 0) return null;
    const count = this.#suppressed;
    this.#suppressed = 0;
    return { kind: "aggregate", count };
  }

  #prune(nowMs) {
    const cutoff = nowMs - this.#windowMs;
    while (this.#recent.length > 0 && this.#recent[0] <= cutoff) this.#recent.shift();
  }
}

/**
 * The alert list.
 *
 * New alerts are prepended in batches rather than re-rendering the feed: a fleet
 * crossing a large zone produces hundreds of alerts a second, and the list has to stay
 * scrollable while that happens.
 */
export class AlertFeed {
  #list;
  #empty;
  #count;
  #template;
  #zoneFilter;
  #onPick;
  #filter = {};
  #zones = new Map();

  constructor({ list, empty, count, template, zoneFilter, kindFilter, onPick, onFilterChange }) {
    this.#list = list;
    this.#empty = empty;
    this.#count = count;
    this.#template = template;
    this.#zoneFilter = zoneFilter;
    this.#onPick = onPick;

    const changed = () => {
      this.#filter = { zoneId: zoneFilter.value || undefined, kind: kindFilter.value || undefined };
      onFilterChange?.(this.#filter);
    };
    zoneFilter.addEventListener("change", changed);
    kindFilter.addEventListener("change", changed);
  }

  get filter() {
    return this.#filter;
  }

  setZones(zones) {
    this.#zones = zones;
    const selected = this.#zoneFilter.value;
    const options = [buildOption("", "All zones")];
    for (const zone of zones.values()) options.push(buildOption(zone.id, zone.name));
    this.#zoneFilter.replaceChildren(...options);
    // A filter pinned to a zone that has just been deleted falls back to "all zones"
    // instead of leaving the feed mysteriously empty.
    this.#zoneFilter.value = zones.has(selected) ? selected : "";
    this.#filter = { ...this.#filter, zoneId: this.#zoneFilter.value || undefined };
  }

  rebuild(items) {
    const visible = items.filter((alert) => matchesFilter(alert, this.#filter)).slice(0, MAX_ROWS);
    this.#list.replaceChildren(...visible.map((alert) => this.#row(alert)));
    this.#refreshEmptyState(items.length);
  }

  /** `arrived` is in arrival order, so the newest of the batch ends up on top. */
  prepend(arrived, total) {
    const rows = arrived
      .filter((alert) => matchesFilter(alert, this.#filter))
      .map((alert) => this.#row(alert))
      .reverse();
    if (rows.length > 0) this.#list.prepend(...rows);
    while (this.#list.childElementCount > MAX_ROWS) this.#list.lastElementChild.remove();
    this.#refreshEmptyState(total);
  }

  clear() {
    this.#list.replaceChildren();
    this.#refreshEmptyState(0);
  }

  #refreshEmptyState(total) {
    this.#count.textContent = String(total);
    this.#empty.hidden = this.#list.childElementCount > 0;
  }

  #row(alert) {
    const element = this.#template.content.firstElementChild.cloneNode(true);
    const pick = (role) => element.querySelector(`[data-role="${role}"]`);
    const kind = pick("kind");
    kind.textContent = alert.kind;
    kind.dataset.kind = alert.kind;
    pick("device").textContent = alert.device_id;
    pick("zone").textContent = this.#zones.get(alert.zone?.id)?.name ?? alert.zone?.name ?? "—";
    const time = pick("time");
    time.textContent = formatClock(alert.occurred_at);
    time.dateTime = alert.occurred_at;
    element.querySelector("button").addEventListener("click", () => this.#onPick?.(alert));
    return element;
  }
}

function buildOption(value, label) {
  const element = document.createElement("option");
  element.value = value;
  element.textContent = label;
  return element;
}
