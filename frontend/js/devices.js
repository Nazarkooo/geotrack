/**
 * The live device set, kept in the shape the GPU wants.
 *
 * Positions, colours and radii live in typed arrays handed to the scatterplot layer as
 * binary attributes, so a tick costs one buffer upload instead of ten thousand object
 * allocations.
 *
 * Two things follow from that choice:
 *
 * - A device keeps its slot for the lifetime of the page. Markers are matched between
 *   frames by slot index, so a slot that changed owner would drag a marker across the
 *   map. Devices that stop being reported are hidden in place — radius and alpha zero,
 *   which also takes them out of picking — rather than compacted out.
 * - The glide between one report and the next is computed here. The layer's own
 *   attribute transitions cover accessor-generated attributes, not externally supplied
 *   buffers; interpolating in the store keeps the fast path and, unlike a transition
 *   the layer runs for every instance at once, lets a device that has just appeared
 *   start where it is instead of sliding in from wherever its slot last pointed.
 */

const PLAIN_COLOR = [110, 168, 255, 220];
const HIGHLIGHT_COLOR = [255, 208, 92, 255];
const MARKER_RADIUS_PX = 3.4;

export class DeviceStore {
  #capacity;
  #staleMs;
  #drawn;
  #origin;
  #target;
  #since;
  #colors;
  #radii;
  #slotIds = [];
  #slots = new Map();
  #entries = new Map();
  #highlighted = new Set();
  #plainColor;
  #highlightColor;

  constructor({ capacity = 4_096, staleMs = 300_000, plainColor, highlightColor } = {}) {
    this.#capacity = Math.max(1, capacity);
    this.#staleMs = staleMs;
    this.#drawn = new Float64Array(this.#capacity * 2);
    this.#origin = new Float64Array(this.#capacity * 2);
    this.#target = new Float64Array(this.#capacity * 2);
    this.#since = new Float64Array(this.#capacity);
    this.#colors = new Uint8Array(this.#capacity * 4);
    this.#radii = new Float32Array(this.#capacity);
    this.#plainColor = plainColor ?? PLAIN_COLOR;
    this.#highlightColor = highlightColor ?? HIGHLIGHT_COLOR;
  }

  /** Devices currently reporting. */
  get size() {
    return this.#entries.size;
  }

  /** Slots drawn, including the hidden ones kept for devices that may come back. */
  get slotCount() {
    return this.#slotIds.length;
  }

  get buffers() {
    return { drawn: this.#drawn, target: this.#target, colors: this.#colors, radii: this.#radii };
  }

  slotOf(deviceId) {
    return this.#slots.get(deviceId) ?? -1;
  }

  /** The device's true latest position, not the one currently on screen. */
  get(deviceId) {
    return this.#entries.get(deviceId) ?? null;
  }

  deviceAt(slot) {
    const deviceId = this.#slotIds[slot];
    const entry = deviceId === undefined ? undefined : this.#entries.get(deviceId);
    return entry ? { id: deviceId, ...entry } : null;
  }

  ids() {
    return [...this.#entries.keys()];
  }

  /**
   * Merge one `positions` frame.
   *
   * A frame flagged `full` is the gateway's answer to a viewport change: it is the
   * complete set for that viewport, so anything absent from it is no longer visible.
   */
  apply(items, { full = false, nowMs = Date.now() } = {}) {
    const seen = full ? new Set() : null;
    for (const [deviceId, latitude, longitude, reportedMs] of items) {
      seen?.add(deviceId);
      const known = this.#entries.get(deviceId);
      if (known && known.reportedMs >= reportedMs) continue;

      const fresh = !known;
      const slot = fresh ? this.#admit(deviceId) : this.#slots.get(deviceId);
      // A device that is already on screen glides from where it is being drawn; one
      // that has just appeared starts where it is.
      this.#origin[slot * 2] = fresh ? longitude : this.#drawn[slot * 2];
      this.#origin[slot * 2 + 1] = fresh ? latitude : this.#drawn[slot * 2 + 1];
      this.#target[slot * 2] = longitude;
      this.#target[slot * 2 + 1] = latitude;
      this.#since[slot] = nowMs;
      if (fresh) {
        this.#drawn[slot * 2] = longitude;
        this.#drawn[slot * 2 + 1] = latitude;
      }
      this.#entries.set(deviceId, { latitude, longitude, reportedMs });
    }
    if (seen) {
      this.remove([...this.#entries.keys()].filter((deviceId) => !seen.has(deviceId)));
    }
  }

  remove(deviceIds) {
    for (const deviceId of deviceIds) {
      if (!this.#entries.delete(deviceId)) continue;
      this.#hide(this.#slots.get(deviceId));
    }
  }

  /** Forget devices whose last report is older than the gateway's staleness window. */
  sweep(nowMs) {
    const dropped = [];
    for (const [deviceId, entry] of this.#entries) {
      if (nowMs - entry.reportedMs > this.#staleMs) dropped.push(deviceId);
    }
    this.remove(dropped);
    return dropped;
  }

  /**
   * Advance the drawn positions towards the reported ones.
   *
   * Returns true while at least one marker is still on its way, which is how the map
   * knows whether another frame is worth drawing.
   */
  interpolate(nowMs, durationMs) {
    let moving = false;
    for (let slot = 0; slot < this.#slotIds.length; slot += 1) {
      const progress = durationMs > 0 ? (nowMs - this.#since[slot]) / durationMs : 1;
      if (progress >= 1) {
        this.#drawn[slot * 2] = this.#target[slot * 2];
        this.#drawn[slot * 2 + 1] = this.#target[slot * 2 + 1];
        continue;
      }
      moving = true;
      const t = progress > 0 ? progress : 0;
      for (const axis of [0, 1]) {
        const from = this.#origin[slot * 2 + axis];
        this.#drawn[slot * 2 + axis] = from + (this.#target[slot * 2 + axis] - from) * t;
      }
    }
    return moving;
  }

  /** Put every marker on its reported position at once. */
  snap() {
    this.#drawn.set(this.#target.subarray(0, this.#slotIds.length * 2));
  }

  /** Recolour the devices currently inside one of the user's zones. */
  setHighlighted(deviceIds) {
    const next = deviceIds instanceof Set ? deviceIds : new Set(deviceIds);
    for (const deviceId of this.#highlighted) {
      if (!next.has(deviceId)) this.#paint(deviceId, this.#plainColor);
    }
    for (const deviceId of next) {
      if (!this.#highlighted.has(deviceId)) this.#paint(deviceId, this.#highlightColor);
    }
    this.#highlighted = next;
  }

  isHighlighted(deviceId) {
    return this.#highlighted.has(deviceId);
  }

  layerData() {
    const length = this.#slotIds.length;
    return {
      length,
      attributes: {
        getPosition: { value: this.#drawn.subarray(0, length * 2), size: 2 },
        // Declared normalized so the layer reads the bytes as 0–255 colour channels
        // instead of warning and guessing.
        getFillColor: { value: this.#colors.subarray(0, length * 4), size: 4, normalized: true },
        getRadius: { value: this.#radii.subarray(0, length), size: 1 },
      },
    };
  }

  #admit(deviceId) {
    const known = this.#slots.get(deviceId);
    if (known !== undefined) {
      this.#show(known, deviceId);
      return known;
    }
    const slot = this.#slotIds.length;
    if (slot >= this.#capacity) this.#grow();
    this.#slotIds.push(deviceId);
    this.#slots.set(deviceId, slot);
    this.#show(slot, deviceId);
    return slot;
  }

  #show(slot, deviceId) {
    this.#radii[slot] = MARKER_RADIUS_PX;
    this.#write(slot, this.#highlighted.has(deviceId) ? this.#highlightColor : this.#plainColor);
  }

  #hide(slot) {
    if (slot === undefined) return;
    this.#radii[slot] = 0;
    this.#colors[slot * 4 + 3] = 0;
  }

  #grow() {
    this.#capacity *= 2;
    this.#drawn = grown(this.#drawn, Float64Array, this.#capacity * 2);
    this.#origin = grown(this.#origin, Float64Array, this.#capacity * 2);
    this.#target = grown(this.#target, Float64Array, this.#capacity * 2);
    this.#since = grown(this.#since, Float64Array, this.#capacity);
    this.#colors = grown(this.#colors, Uint8Array, this.#capacity * 4);
    this.#radii = grown(this.#radii, Float32Array, this.#capacity);
  }

  #paint(deviceId, color) {
    // A device that is not reporting stays invisible whatever its zones say.
    if (!this.#entries.has(deviceId)) return;
    const slot = this.#slots.get(deviceId);
    if (slot !== undefined) this.#write(slot, color);
  }

  #write(slot, [r, g, b, a]) {
    const offset = slot * 4;
    this.#colors[offset] = r;
    this.#colors[offset + 1] = g;
    this.#colors[offset + 2] = b;
    this.#colors[offset + 3] = a;
  }
}

function grown(source, Kind, length) {
  const next = new Kind(length);
  next.set(source);
  return next;
}
