/**
 * Geofences: drawing on the map and the settings list beside it.
 *
 * The server owns the geometry. The drawing plugin is only an input device: every
 * gesture is turned into a REST call and the circle is then re-drawn from the row the
 * server returned, so what the user sees is always what the processor will test
 * against — and so a zone edited on a phone lands on the laptop through the same path
 * as a zone edited locally.
 */

import { createGeomanInstance } from "@geoman-io/maplibre-geoman-free";

import { ApiError } from "./api.js";
import {
  MAX_RADIUS_M,
  MIN_RADIUS_M,
  circleMoved,
  circleRing,
  clampRadius,
  ringToCircle,
} from "./geo.js";
import { formatDistance } from "./format.js";

const CIRCLE_SEGMENTS = 80;

const geomanOptions = {
  settings: {
    // The dashboard supplies its own toolbar; the plugin's own control bar would be a
    // second, differently styled set of buttons on the same map.
    useControlsUi: false,
    throttlingDelay: 60,
  },
  controls: {
    draw: {
      marker: { uiEnabled: false },
      circle_marker: { uiEnabled: false },
      text_marker: { uiEnabled: false },
      line: { uiEnabled: false },
      rectangle: { uiEnabled: false },
      polygon: { uiEnabled: false },
      ellipse: { uiEnabled: false },
      freehand: { uiEnabled: false },
      custom_shape: { uiEnabled: false },
    },
  },
  layerStyles: {
    circle: {
      gm_main: [
        {
          type: "fill",
          paint: {
            "fill-color": ["coalesce", ["get", "color"], "#4da3ff"],
            "fill-opacity": 0.12,
          },
        },
        {
          type: "line",
          paint: {
            "line-color": ["coalesce", ["get", "color"], "#4da3ff"],
            "line-width": 2,
            "line-opacity": 0.9,
          },
        },
      ],
    },
  },
};

export class ZoneEditor {
  #map;
  #api;
  #gm = null;
  #zones = new Map();
  #suppress = 0;
  #mode = null;
  #onChange;
  #onError;
  #onMode;

  constructor({ map, api, onChange, onError, onMode }) {
    this.#map = map;
    this.#api = api;
    this.#onChange = onChange;
    this.#onError = onError;
    this.#onMode = onMode;
  }

  get zones() {
    return this.#zones;
  }

  async init({ timeoutMs = 15_000 } = {}) {
    // The plugin loads its marker sprite before it reports itself ready; if that fetch
    // never settles it simply never resolves, so the wait is bounded here.
    this.#gm = await withTimeout(
      createGeomanInstance(this.#map, geomanOptions),
      timeoutMs,
      "The zone editor did not finish loading.",
    );
    this.#map.on("gm:create", (event) => this.#onCreate(event));
    this.#map.on("gm:remove", (event) => this.#onRemove(event));
    for (const name of ["gm:editend", "gm:dragend", "gm:changeend", "gm:scaleend"]) {
      this.#map.on(name, (event) => this.#onEdited(event));
    }
    return this;
  }

  /** Replace everything on the map with what the API currently holds. */
  async load() {
    const page = await this.#api.listZones();
    this.#zones = new Map(page.items.map((zone) => [zone.id, zone]));
    await this.#withSuppressed(async () => {
      await this.#gm.features.deleteAll();
      for (const zone of this.#zones.values()) await this.#draw(zone);
    });
    this.#onChange?.();
    return page;
  }

  /** A `zone` frame: another session of this user changed something. */
  async applyEvent({ op, zone }) {
    if (op === "deleted") {
      this.#zones.delete(zone.id);
      await this.#withSuppressed(() => this.#erase(zone.id));
    } else {
      const known = this.#zones.get(zone.id);
      // Frames can overtake each other; an older version must not undo a newer one.
      if (known && known.version > zone.version) return;
      this.#zones.set(zone.id, zone);
      await this.#withSuppressed(() => this.#draw(zone));
    }
    this.#onChange?.();
  }

  get mode() {
    return this.#mode;
  }

  /**
   * Switch the map into one editing mode, or out of all of them.
   *
   * Exactly one at a time: the plugin's modes are mutually exclusive, so enabling two
   * of them silently leaves only the last one on.
   *
   * There is no resize mode here. The free build draws no edge handle for a circle, so
   * the radius is edited as a number in the zone list, which is both precise and
   * reachable from the keyboard — a geofence is specified in metres anyway.
   */
  async setMode(mode) {
    if (!this.#gm) return null;
    await this.#gm.disableAllModes();
    this.#mode = this.#mode === mode ? null : mode;
    switch (this.#mode) {
      case "draw":
        await this.#gm.enableDraw("circle");
        break;
      case "move":
        await this.#gm.enableGlobalDragMode();
        break;
      case "remove":
        await this.#gm.enableGlobalRemovalMode();
        break;
      default:
        break;
    }
    this.#onMode?.(this.#mode);
    return this.#mode;
  }

  async patch(zoneId, changes) {
    const zone = this.#zones.get(zoneId);
    if (!zone) return null;
    try {
      const updated = await this.#api.updateZone(zoneId, changes, { version: zone.version });
      this.#zones.set(zoneId, updated);
      await this.#withSuppressed(() => this.#draw(updated));
      this.#onChange?.();
      return updated;
    } catch (error) {
      await this.#recover(zoneId, error, "Could not update the zone");
      return null;
    }
  }

  async remove(zoneId) {
    try {
      await this.#api.deleteZone(zoneId);
      this.#zones.delete(zoneId);
      await this.#withSuppressed(() => this.#erase(zoneId));
      this.#onChange?.();
      return true;
    } catch (error) {
      await this.#recover(zoneId, error, "Could not delete the zone");
      return false;
    }
  }

  clear() {
    this.#zones.clear();
    return this.#withSuppressed(() => this.#gm?.features.deleteAll() ?? Promise.resolve());
  }

  // --- drawing gestures ---------------------------------------------------

  async #onCreate(event) {
    if (this.#suppress > 0 || event.shape !== "circle") return;
    const feature = event.feature;
    const circle = this.#readCircle(feature);
    this.#discard(feature);
    if (!circle) return;

    try {
      const zone = await this.#api.createZone({
        name: `Zone ${this.#zones.size + 1}`,
        latitude: circle.center[1],
        longitude: circle.center[0],
        radius_m: clampRadius(circle.radiusM),
      });
      this.#zones.set(zone.id, zone);
      await this.#withSuppressed(() => this.#draw(zone));
      this.#onChange?.({ created: zone });
    } catch (error) {
      this.#report(error, "Could not create the zone");
    }
  }

  async #onEdited(event) {
    if (this.#suppress > 0) return;
    for (const feature of featuresOf(event)) {
      const zoneId = String(feature.id);
      const zone = this.#zones.get(zoneId);
      if (!zone) continue;
      const circle = this.#readCircle(feature);
      // A gesture that ended where it started still ends the edit mode. Writing that
      // back would bump the zone's version and wake every other session for nothing.
      if (!circle || !circleMoved(asCircle(zone), circle)) continue;
      await this.patch(zoneId, {
        latitude: circle.center[1],
        longitude: circle.center[0],
        radius_m: clampRadius(circle.radiusM),
      });
    }
  }

  async #onRemove(event) {
    if (this.#suppress > 0) return;
    const zoneId = String(event.feature?.id ?? "");
    if (!this.#zones.has(zoneId)) return;
    const removed = await this.remove(zoneId);
    if (!removed) {
      // The zone is still on the server, so put the circle back.
      const zone = this.#zones.get(zoneId);
      if (zone) await this.#withSuppressed(() => this.#draw(zone));
    }
  }

  // --- internals ----------------------------------------------------------

  /**
   * Take the plugin's sketch off the map once it has finished creating it.
   *
   * The sketch goes either way: on success the server's version replaces it, on failure
   * nothing should be left behind pretending to be a zone. Deleting it from inside the
   * create event, though, leaves the drawer unable to start another shape — and it
   * announces nothing, so the toolbar would go on claiming to be armed while every
   * further click did nothing. One turn later the plugin is done with the feature and
   * drawing stays live for the next zone.
   */
  #discard(feature) {
    globalThis.setTimeout(() => {
      void this.#withSuppressed(() => this.#gm.features.delete(feature));
    }, 0);
  }

  #readCircle(feature) {
    const geoJson = feature?.getGeoJson?.();
    const ring = geoJson?.geometry?.coordinates?.[0];
    if (!Array.isArray(ring) || ring.length < 4) return null;
    const hint = feature.getShapeProperty?.("center") ?? null;
    const circle = ringToCircle(ring, Array.isArray(hint) ? [hint[0], hint[1]] : null);
    return circle && Number.isFinite(circle.radiusM) ? circle : null;
  }

  #draw(zone) {
    const center = [zone.longitude, zone.latitude];
    return this.#gm.features.importGeoJson(
      {
        type: "Feature",
        id: zone.id,
        properties: {
          id: zone.id,
          shape: "circle",
          center,
          color: zone.color,
          name: zone.name,
        },
        geometry: {
          type: "Polygon",
          coordinates: [circleRing(center, zone.radius_m, CIRCLE_SEGMENTS)],
        },
      },
      { idPropertyName: "id", overwrite: true },
    );
  }

  async #erase(zoneId) {
    if (this.#gm.features.get("gm_main", zoneId)) await this.#gm.features.delete(zoneId);
  }

  /**
   * Run a programmatic change without the plugin's events looping back as user input.
   *
   * The id check in each handler is the real guarantee; this counter keeps the common
   * case from even reaching them.
   */
  async #withSuppressed(work) {
    this.#suppress += 1;
    try {
      await work();
    } finally {
      this.#suppress -= 1;
    }
  }

  /** After a rejected write, show what happened and put the map back in step. */
  async #recover(zoneId, error, title) {
    this.#report(error, title);
    if (!(error instanceof ApiError)) return;
    if (error.status === 404) {
      this.#zones.delete(zoneId);
      await this.#withSuppressed(() => this.#erase(zoneId));
      this.#onChange?.();
      return;
    }
    if (error.isConflict) {
      try {
        const zone = await this.#api.getZone(zoneId);
        this.#zones.set(zone.id, zone);
        await this.#withSuppressed(() => this.#draw(zone));
        this.#onChange?.();
      } catch {
        await this.load();
      }
    }
  }

  #report(error, title) {
    this.#onError?.(title, error instanceof ApiError ? error.message : String(error));
  }
}

function withTimeout(promise, timeoutMs, message) {
  return new Promise((resolve, reject) => {
    const timer = globalThis.setTimeout(() => reject(new Error(message)), timeoutMs);
    promise.then(
      (value) => {
        globalThis.clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        globalThis.clearTimeout(timer);
        reject(error);
      },
    );
  });
}

const asCircle = (zone) => ({ center: [zone.longitude, zone.latitude], radiusM: zone.radius_m });

function featuresOf(event) {
  if (event.features?.length) return event.features;
  return event.feature ? [event.feature] : [];
}

/** The zone settings list. Rows are updated in place so typing is never interrupted. */
export class ZonePanel {
  #list;
  #empty;
  #count;
  #template;
  #rows = new Map();
  #handlers;
  #selected = null;

  constructor({ list, empty, count, template, handlers }) {
    this.#list = list;
    this.#empty = empty;
    this.#count = count;
    this.#template = template;
    this.#handlers = handlers;
  }

  render(zones, presence) {
    const ordered = [...zones.values()].sort((a, b) => a.created_at.localeCompare(b.created_at));
    this.#count.textContent = String(ordered.length);
    this.#empty.hidden = ordered.length > 0;

    for (const [zoneId, row] of this.#rows) {
      if (!zones.has(zoneId)) {
        row.element.remove();
        this.#rows.delete(zoneId);
      }
    }
    for (const zone of ordered) {
      const row = this.#rows.get(zone.id) ?? this.#createRow(zone.id);
      this.#fill(row, zone, presence?.devicesIn(zone.id).size ?? 0);
      this.#list.append(row.element);
    }
  }

  select(zoneId) {
    this.#selected = zoneId;
    for (const [id, row] of this.#rows) {
      row.element.classList.toggle("is-selected", id === zoneId);
    }
  }

  #createRow(zoneId) {
    const element = this.#template.content.firstElementChild.cloneNode(true);
    const pick = (role) => element.querySelector(`[data-role="${role}"]`);
    const row = {
      element,
      swatch: pick("swatch"),
      name: pick("name"),
      expand: pick("expand"),
      settings: pick("settings"),
      fly: pick("fly"),
      radius: pick("radius"),
      inside: pick("inside"),
      color: pick("color"),
      radiusInput: pick("radius-input"),
      enter: pick("enter"),
      exit: pick("exit"),
      dwellOn: pick("dwell-on"),
      dwellField: pick("dwell-field"),
      dwell: pick("dwell"),
      remove: pick("delete"),
    };

    row.expand.addEventListener("click", () => {
      const open = row.settings.hidden;
      row.settings.hidden = !open;
      row.expand.setAttribute("aria-expanded", String(open));
    });
    row.element.addEventListener("focusin", () => this.#handlers.onSelect?.(zoneId));
    row.fly.addEventListener("click", () => this.#handlers.onFly?.(zoneId));
    row.name.addEventListener("change", () => {
      const name = row.name.value.trim();
      if (name) this.#handlers.onPatch?.(zoneId, { name });
      else this.#handlers.onRevert?.(zoneId);
    });
    row.color.addEventListener("change", () =>
      this.#handlers.onPatch?.(zoneId, { color: row.color.value.toLowerCase() }),
    );
    row.radiusInput.addEventListener("change", () => {
      const radius = clampRadius(Number(row.radiusInput.value));
      row.radiusInput.value = String(radius);
      this.#handlers.onPatch?.(zoneId, { radius_m: radius });
    });
    row.enter.addEventListener("change", () =>
      this.#handlers.onPatch?.(zoneId, { alert_on_enter: row.enter.checked }),
    );
    row.exit.addEventListener("change", () =>
      this.#handlers.onPatch?.(zoneId, { alert_on_exit: row.exit.checked }),
    );
    row.dwellOn.addEventListener("change", () => {
      row.dwellField.hidden = !row.dwellOn.checked;
      const interval = row.dwellOn.checked ? clampDwell(Number(row.dwell.value)) : null;
      this.#handlers.onPatch?.(zoneId, { dwell_alert_interval_s: interval });
    });
    row.dwell.addEventListener("change", () => {
      if (!row.dwellOn.checked) return;
      const interval = clampDwell(Number(row.dwell.value));
      row.dwell.value = String(interval);
      this.#handlers.onPatch?.(zoneId, { dwell_alert_interval_s: interval });
    });
    row.remove.addEventListener("click", () => this.#handlers.onRemove?.(zoneId));

    this.#rows.set(zoneId, row);
    return row;
  }

  #fill(row, zone, insideCount) {
    row.swatch.style.background = zone.color;
    setValue(row.name, zone.name);
    row.radius.textContent = formatDistance(zone.radius_m);
    row.inside.textContent = insideCount > 0 ? `${insideCount} inside` : "";
    setValue(row.color, zone.color);
    setValue(row.radiusInput, String(Math.round(zone.radius_m)));
    row.radiusInput.min = String(MIN_RADIUS_M);
    row.radiusInput.max = String(MAX_RADIUS_M);
    setChecked(row.enter, zone.alert_on_enter);
    setChecked(row.exit, zone.alert_on_exit);
    const dwellOn = zone.dwell_alert_interval_s !== null;
    setChecked(row.dwellOn, dwellOn);
    row.dwellField.hidden = !dwellOn;
    setValue(row.dwell, String(zone.dwell_alert_interval_s ?? 60));
    row.element.classList.toggle("is-selected", this.#selected === zone.id);
  }
}

function clampDwell(value) {
  if (!Number.isFinite(value)) return 60;
  return Math.min(86_400, Math.max(10, Math.round(value)));
}

/** Never overwrite the control the user is currently editing. */
function setValue(input, value) {
  if (input !== document.activeElement && input.value !== value) input.value = value;
}

function setChecked(input, value) {
  if (input !== document.activeElement) input.checked = value;
}
