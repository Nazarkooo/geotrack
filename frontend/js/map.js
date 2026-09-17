/**
 * The map surface: a MapLibre basemap with a deck.gl overlay for the device layer.
 *
 * Ten thousand markers cannot be DOM nodes and cannot be a GeoJSON source that is
 * replaced four times a second, so devices are drawn by the GPU from the typed arrays
 * the device store already maintains. The overlay is interleaved, which keeps the
 * markers inside the basemap's own render pass instead of a second canvas on top.
 */

import { AttributionControl, Map as MapLibreMap, NavigationControl, ScaleControl } from "maplibre-gl";

import { bboxEquals, boundsToBBox } from "./geo.js";
import { prefersReducedMotion } from "./ui.js";

const STYLE_URL = "https://tiles.openfreemap.org/styles/dark";
const FALLBACK_VIEW = { center: [30.5234, 50.4501], zoom: 11 };
const VIEWPORT_DEBOUNCE_MS = 200;
const TRACK_COLOR = [255, 208, 92, 210];

export class MapView {
  #map;
  #overlay = null;
  #resizes = null;
  #deck;
  #store = null;
  #trackPath = null;
  #dirty = true;
  #tickMs;
  #raf = null;
  #viewportHandle = null;
  #lastBBox = null;
  #onViewportChange;
  #onDevicePick;
  #animate;

  constructor({ container, onViewportChange, onDevicePick, deck = globalThis.deck, tickMs = 250 }) {
    if (!deck?.MapLibreOverlay) {
      throw new Error("deck.gl did not load; the device layer cannot be drawn.");
    }
    this.#deck = deck;
    this.#tickMs = tickMs;
    this.#onViewportChange = onViewportChange;
    this.#onDevicePick = onDevicePick;
    this.#animate = !prefersReducedMotion();

    this.#map = new MapLibreMap({
      container,
      style: STYLE_URL,
      center: FALLBACK_VIEW.center,
      zoom: FALLBACK_VIEW.zoom,
      attributionControl: false,
      maxPitch: 0,
      // A dashboard is read, not explored in 3D; keeping it flat also keeps the
      // viewport rectangle the gateway filters on honest.
      pitchWithRotate: false,
    });
    // The public dark style asks for a fill pattern its sprite sheet does not carry.
    // Answering with a transparent pixel keeps that upstream gap out of the console
    // without hiding anything the dashboard itself draws.
    this.#map.setMissingStyleImageResolver((id) => {
      if (!this.#map.hasImage(id)) {
        this.#map.addImage(id, { width: 1, height: 1, data: new Uint8Array(4) });
      }
    });
    this.#map.addControl(new NavigationControl({ showCompass: false }), "bottom-right");
    this.#map.addControl(new ScaleControl({ unit: "metric" }), "bottom-right");
    this.#map.addControl(new AttributionControl({ compact: true }), "bottom-right");
    this.#map.on("moveend", () => this.#scheduleViewport());
  }

  get map() {
    return this.#map;
  }

  /**
   * Resolves once the style is in and layers may be added.
   *
   * The timeout matters: MapLibre fetches its worker and basemap over the network, and
   * a blocked request leaves the `load` event pending forever. Failing loudly turns
   * that into a message instead of a dashboard stuck on "Connecting…".
   */
  ready({ timeoutMs = 20_000 } = {}) {
    if (this.#map.loaded()) return Promise.resolve(this);
    return new Promise((resolve, reject) => {
      const timer = globalThis.setTimeout(
        () => reject(new Error("The basemap did not finish loading.")),
        timeoutMs,
      );
      const settle = (run) => (argument) => {
        globalThis.clearTimeout(timer);
        run(argument);
      };
      this.#map.once("load", settle(() => resolve(this)));
      this.#map.once(
        "error",
        settle((event) => reject(event?.error ?? new Error("The basemap failed to load."))),
      );
    });
  }

  /**
   * Start drawing. Called after the zone editor is installed so that the device dots
   * sit above the zone circles rather than under them.
   */
  attachDevices(store) {
    this.#store = store;
    this.#overlay = new this.#deck.MapLibreOverlay({
      interleaved: true,
      layers: [],
      getTooltip: null,
    });
    this.#map.addControl(this.#overlay);
    // The overlay reads its size from the map container when the map reports a resize.
    // A layout change that does not reach the map — the window resizing, a phone
    // rotating, a panel opening — leaves the device layer projecting against the old
    // size, which puts every marker visibly off the basemap. Watching the container and
    // telling the map about it keeps the two in step.
    this.#resizes = new globalThis.ResizeObserver(() => {
      this.#map.resize();
      this.#dirty = true;
    });
    this.#resizes.observe(this.#map.getContainer());
    this.#frame();
    this.#emitViewport();
  }

  invalidate() {
    this.#dirty = true;
  }

  showTrack(points) {
    this.#trackPath = points.map((point) => [point.longitude, point.latitude]);
    this.#dirty = true;
  }

  clearTrack() {
    this.#trackPath = null;
    this.#dirty = true;
  }

  flyTo(latitude, longitude, { zoom } = {}) {
    this.#map.flyTo({
      center: [longitude, latitude],
      zoom: zoom ?? Math.max(this.#map.getZoom(), 13),
      duration: this.#animate ? 900 : 0,
      essential: true,
    });
  }

  fitCircle(latitude, longitude, radiusM) {
    // A circle's bounding box in degrees; good enough to frame it before the fly-in.
    const dLat = (radiusM / 111_320) * 1.6;
    const dLon = dLat / Math.max(0.2, Math.cos((latitude * Math.PI) / 180));
    this.#map.fitBounds(
      [
        [longitude - dLon, latitude - dLat],
        [longitude + dLon, latitude + dLat],
      ],
      { duration: this.#animate ? 700 : 0, padding: 60 },
    );
  }

  destroy() {
    this.#resizes?.disconnect();
    if (this.#raf !== null) globalThis.cancelAnimationFrame(this.#raf);
    if (this.#viewportHandle !== null) globalThis.clearTimeout(this.#viewportHandle);
    this.#map.remove();
  }

  // --- internals ----------------------------------------------------------

  /**
   * One animation frame: advance the glide, and upload only if something changed.
   *
   * Driven by the display rather than by a timer, so the markers move at the screen's
   * own rate and a hidden tab costs nothing.
   */
  #frame() {
    this.#raf = globalThis.requestAnimationFrame(() => this.#frame());
    if (!this.#store) return;
    const moving = this.#animate
      ? this.#store.interpolate(Date.now(), this.#tickMs)
      : (this.#store.snap(), false);
    if (!this.#dirty && !moving) return;
    this.#render();
  }

  #render() {
    if (!this.#overlay || !this.#store) return;
    this.#dirty = false;

    const layers = [
      new this.#deck.ScatterplotLayer({
        id: "devices",
        data: this.#store.layerData(),
        pickable: true,
        stroked: false,
        // Radius comes from the store so a device that stopped reporting can be hidden
        // in place; a zero radius is also invisible to picking.
        radiusUnits: "pixels",
        radiusMinPixels: 0,
        onClick: (info) => {
          if (info.index >= 0) this.#onDevicePick?.(info.index);
          return true;
        },
      }),
    ];
    if (this.#trackPath && this.#trackPath.length > 1) {
      layers.unshift(
        new this.#deck.PathLayer({
          id: "device-track",
          data: [{ path: this.#trackPath }],
          getPath: (d) => d.path,
          getColor: TRACK_COLOR,
          getWidth: 2.5,
          widthUnits: "pixels",
          capRounded: true,
          jointRounded: true,
          pickable: false,
        }),
      );
    }
    this.#overlay.setProps({ layers });
  }

  #scheduleViewport() {
    if (this.#viewportHandle !== null) globalThis.clearTimeout(this.#viewportHandle);
    this.#viewportHandle = globalThis.setTimeout(() => {
      this.#viewportHandle = null;
      this.#emitViewport();
    }, VIEWPORT_DEBOUNCE_MS);
  }

  #emitViewport() {
    const bbox = boundsToBBox(this.#map.getBounds());
    if (bboxEquals(this.#lastBBox, bbox)) return;
    this.#lastBBox = bbox;
    this.#onViewportChange?.(bbox);
  }
}
