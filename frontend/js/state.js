/**
 * Client state that several views share: who is inside which zone, the access token,
 * and the small event bus the modules talk over.
 */

/** A minimal event bus; listeners are copied before dispatch so they may unsubscribe. */
export class Emitter {
  #listeners = new Map();

  on(type, listener) {
    const listeners = this.#listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.#listeners.set(type, listeners);
    return () => listeners.delete(listener);
  }

  emit(type, payload, onError = reportListenerError) {
    for (const listener of [...(this.#listeners.get(type) ?? [])]) {
      try {
        listener(payload);
      } catch (error) {
        // One failing view must not stop a position frame reaching the others.
        onError(error, type);
      }
    }
  }
}

function reportListenerError(error, type) {
  console.error(`listener for "${type}" failed`, error);
}

/**
 * Which devices are inside which of the user's zones.
 *
 * Seeded from `GET /api/v1/geozones/presence` and then kept current by enter/exit
 * alerts, which is exactly the information the server already pushes — no polling.
 */
export class PresenceTracker {
  #byZone = new Map();
  #byDevice = new Map();

  setSnapshot(zones) {
    this.clear();
    for (const [zoneId, deviceIds] of Object.entries(zones ?? {})) {
      for (const deviceId of deviceIds) this.#link(zoneId, deviceId);
    }
  }

  /** Returns true when the set of highlighted devices changed. */
  applyAlert(alert) {
    const zoneId = alert.zone?.id;
    if (!zoneId) return false;
    if (alert.kind === "exit") return this.#unlink(zoneId, alert.device_id);
    return this.#link(zoneId, alert.device_id);
  }

  removeZone(zoneId) {
    const devices = this.#byZone.get(zoneId);
    if (!devices) return false;
    let changed = false;
    for (const deviceId of [...devices]) {
      changed = this.#unlink(zoneId, deviceId) || changed;
    }
    this.#byZone.delete(zoneId);
    return changed;
  }

  devicesIn(zoneId) {
    return this.#byZone.get(zoneId) ?? new Set();
  }

  zonesOf(deviceId) {
    return this.#byDevice.get(deviceId) ?? new Set();
  }

  get deviceIds() {
    return new Set(this.#byDevice.keys());
  }

  clear() {
    this.#byZone.clear();
    this.#byDevice.clear();
  }

  #link(zoneId, deviceId) {
    const zone = this.#byZone.get(zoneId) ?? new Set();
    zone.add(deviceId);
    this.#byZone.set(zoneId, zone);

    // Only the first zone a device enters changes whether it is highlighted.
    const zones = this.#byDevice.get(deviceId);
    if (zones) {
      zones.add(zoneId);
      return false;
    }
    this.#byDevice.set(deviceId, new Set([zoneId]));
    return true;
  }

  #unlink(zoneId, deviceId) {
    this.#byZone.get(zoneId)?.delete(deviceId);
    const zones = this.#byDevice.get(deviceId);
    if (!zones?.delete(zoneId)) return false;
    if (zones.size > 0) return false;
    this.#byDevice.delete(deviceId);
    return true;
  }
}

const TOKEN_KEY = "geotrack.token";

/**
 * The access token, kept in `localStorage` so a reload stays logged in.
 *
 * A browser in private mode (or with site data blocked) throws on every storage call;
 * the in-memory copy keeps the session usable for as long as the tab lives.
 */
export class TokenStore {
  #storage;
  #key;
  #cached = null;

  constructor(storage, key = TOKEN_KEY) {
    this.#storage = storage;
    this.#key = key;
    this.#cached = this.#read();
  }

  get() {
    return this.#cached;
  }

  set(token) {
    this.#cached = token;
    try {
      this.#storage?.setItem(this.#key, token);
    } catch {
      // Persistence is a convenience; the session still works without it.
    }
  }

  clear() {
    this.#cached = null;
    try {
      this.#storage?.removeItem(this.#key);
    } catch {
      /* see set() */
    }
  }

  #read() {
    try {
      return this.#storage?.getItem(this.#key) ?? null;
    } catch {
      return null;
    }
  }
}
