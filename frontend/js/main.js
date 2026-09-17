/**
 * Composition root: builds the views, opens the socket and routes frames.
 *
 * Nothing else in the application knows about both the network and the DOM; this file
 * is the only place the two meet.
 */

import { AlertFeed, AlertLog, ToastAggregator } from "./alerts.js";
import { ApiClient, ApiError } from "./api.js";
import { DeviceStore } from "./devices.js";
import { MapView } from "./map.js";
import { PresenceTracker, TokenStore } from "./state.js";
import {
  Announcer,
  ConnectionIndicator,
  DeviceCard,
  LoginView,
  PanelShell,
  SessionsView,
  StatsView,
  Toaster,
  byId,
} from "./ui.js";
import { RealtimeClient, websocketUrl } from "./ws.js";
import { ZoneEditor, ZonePanel } from "./zones.js";

const DEVICE_STALE_MS = 300_000;
const SWEEP_INTERVAL_MS = 30_000;
const FEED_FLUSH_MS = 250;
const TRACK_WINDOW_MS = 15 * 60_000;
const TICK_MS = 250;

const api = new ApiClient();
const tokens = new TokenStore(safeLocalStorage());
const devices = new DeviceStore({ staleMs: DEVICE_STALE_MS });
const alertLog = new AlertLog();
const presence = new PresenceTracker();
const toastPolicy = new ToastAggregator({ windowMs: 1_000, threshold: 5 });

// One matcher for every view that has to behave differently over the map than beside it.
const compactLayout = globalThis.matchMedia?.("(max-width: 720px)") ?? null;
const TOAST_CEILING = { compact: 2, roomy: 4 };
const toastCeiling = () => (compactLayout?.matches ? TOAST_CEILING.compact : TOAST_CEILING.roomy);

const toaster = new Toaster(byId("toasts"), { maxVisible: toastCeiling() });
compactLayout?.addEventListener?.("change", () => toaster.setMaxVisible(toastCeiling()));
const announcer = new Announcer(byId("announcer"));
const stats = new StatsView({
  devices: byId("stat-devices"),
  rate: byId("stat-rate"),
  backlog: byId("stat-backlog"),
  rtt: byId("stat-rtt"),
});
const sessionsView = new SessionsView({
  toggle: byId("sessions-toggle"),
  popover: byId("sessions-popover"),
  list: byId("sessions-list"),
  count: byId("sessions-count"),
});
const connection = new ConnectionIndicator({
  root: byId("connection"),
  dot: byId("connection-dot"),
  label: byId("connection-text"),
  onRetry: () => realtime?.connect(),
});
const panel = new PanelShell({
  panel: byId("panel"),
  handle: byId("panel-handle"),
  tabs: { zones: byId("tab-zones"), alerts: byId("tab-alerts") },
  panes: { zones: byId("pane-zones"), alerts: byId("pane-alerts") },
  compact: compactLayout,
});

let mapView = null;
let zoneEditor = null;
let zonePanel = null;
let alertFeed = null;
let realtime = null;
let deviceCard = null;
let pendingAlerts = [];
let viewport = null;
let sweepTimer = null;
let feedTimer = null;

const loginView = new LoginView({
  form: byId("login-form"),
  input: byId("login-username"),
  error: byId("login-error"),
  submit: byId("login-submit"),
  onSubmit: signIn,
});

byId("logout").addEventListener("click", () => signOut());

// --- session ---------------------------------------------------------------

async function signIn(username) {
  loginView.setBusy(true);
  try {
    const session = await api.login(username);
    tokens.set(session.access_token);
    await startSession(session.user);
  } catch (error) {
    loginView.showError(
      error instanceof ApiError ? error.message : "Sign in failed. Please try again.",
    );
  } finally {
    loginView.setBusy(false);
  }
}

function signOut({ message } = {}) {
  realtime?.close();
  realtime = null;
  if (sweepTimer !== null) globalThis.clearInterval(sweepTimer);
  if (feedTimer !== null) globalThis.clearInterval(feedTimer);
  sweepTimer = feedTimer = null;

  alertLog.clear();
  presence.clear();
  alertFeed?.clear();
  zoneEditor?.clear();
  deviceCard?.hide();
  stats.reset();
  api.setToken(null);
  tokens.clear();

  byId("app").hidden = true;
  byId("login").hidden = false;
  loginView.reset();
  if (message) loginView.showError(message);
  loginView.focus();
}

async function startSession(user) {
  byId("login").hidden = true;
  byId("app").hidden = false;
  byId("current-user").textContent = user.username;

  try {
    await buildWorkspace();
  } catch (error) {
    // Everything else hangs off the map, so this is fatal — but it has to be said out
    // loud rather than leaving the header stuck on "Connecting…".
    connection.set({ state: "closed" });
    toaster.show({
      title: "The map could not start",
      detail: `${error?.message ?? error} Reload the page to try again.`,
      tone: "error",
      timeoutMs: 0,
    });
    return;
  }
  await loadInitialState();
  openSocket();
}

async function buildWorkspace() {
  if (mapView) return;

  mapView = new MapView({
    container: byId("map"),
    tickMs: TICK_MS,
    onViewportChange: (bbox) => {
      viewport = bbox;
      realtime?.setViewport(bbox);
    },
    onDevicePick: (slot) => showDevice(devices.deviceAt(slot)),
  });
  await mapView.ready();

  // The zone editor installs its own layers first so the device dots stay on top.
  zoneEditor = new ZoneEditor({
    map: mapView.map,
    api,
    onChange: () => refreshZoneViews(),
    onError: (title, detail) => toaster.error(title, detail),
    onMode: (mode) => syncTools(mode),
  });
  await zoneEditor.init();
  mapView.attachDevices(devices);

  deviceCard = new DeviceCard({
    root: byId("device-card"),
    fields: {
      id: byId("device-id"),
      latitude: byId("device-lat"),
      longitude: byId("device-lon"),
      age: byId("device-age"),
      zones: byId("device-zones"),
    },
    buttons: {
      track: byId("device-track"),
      follow: byId("device-follow"),
      close: byId("device-close"),
    },
    onTrack: showTrack,
    onFollow: (deviceId) => {
      const device = devices.get(deviceId);
      if (device) mapView.flyTo(device.latitude, device.longitude);
    },
    onClose: () => mapView.clearTrack(),
  });

  zonePanel = new ZonePanel({
    list: byId("zone-list"),
    empty: byId("zones-empty"),
    count: byId("zones-count"),
    template: byId("zone-row-template"),
    handlers: {
      onPatch: (zoneId, changes) => zoneEditor.patch(zoneId, changes),
      onRemove: (zoneId) => zoneEditor.remove(zoneId),
      onRevert: () => refreshZoneViews(),
      onSelect: (zoneId) => zonePanel.select(zoneId),
      onFly: (zoneId) => {
        const zone = zoneEditor.zones.get(zoneId);
        if (zone) mapView.fitCircle(zone.latitude, zone.longitude, zone.radius_m);
      },
    },
  });

  alertFeed = new AlertFeed({
    list: byId("alert-feed"),
    empty: byId("alerts-empty"),
    count: byId("alerts-count"),
    template: byId("alert-row-template"),
    zoneFilter: byId("alert-zone-filter"),
    kindFilter: byId("alert-kind-filter"),
    onPick: focusAlert,
    onFilterChange: () => alertFeed.rebuild(alertLog.items),
  });

  wireTools();

  sweepTimer = globalThis.setInterval(() => {
    const dropped = devices.sweep(Date.now());
    if (dropped.length > 0) mapView.invalidate();
  }, SWEEP_INTERVAL_MS);

  feedTimer = globalThis.setInterval(flushAlerts, FEED_FLUSH_MS);
}

const TOOL_HINTS = {
  draw: "Draw mode: click the centre of the zone, then click again to set its radius. Stays on for the next zone.",
  move: "Move mode: drag a zone to a new place.",
  remove: "Remove mode: click a zone to delete it.",
};

function toolButtons() {
  return { draw: byId("tool-draw"), move: byId("tool-move"), remove: byId("tool-remove") };
}

/** Keep the toolbar showing the mode the map is actually in. */
function syncTools(active) {
  for (const [name, button] of Object.entries(toolButtons())) {
    button.setAttribute("aria-pressed", String(name === active));
  }
  announcer.say(TOOL_HINTS[active] ?? "Editing tools off.");
}

function wireTools() {
  for (const [mode, button] of Object.entries(toolButtons())) {
    button.addEventListener("click", () => zoneEditor.setMode(mode));
  }
}

async function loadInitialState() {
  try {
    await zoneEditor.load();
    presence.setSnapshot((await api.zonePresence()).zones);
    devices.setHighlighted(presence.deviceIds);
    refreshZoneViews();
    await backfillAlerts();
  } catch (error) {
    handleApiError(error, "Could not load your zones");
  }
}

// --- socket ----------------------------------------------------------------

function openSocket() {
  realtime = new RealtimeClient({ url: websocketUrl(globalThis.location), token: api.token });

  realtime.on("status", (status) => connection.set(status));
  realtime.on("hello", onHello);
  realtime.on("positions", onPositions);
  realtime.on("alert", onAlert);
  realtime.on("zone", (frame) => zoneEditor.applyEvent(frame));
  realtime.on("sessions", (frame) => sessionsView.render(frame.sessions, realtime.sessionId));
  realtime.on("stats", (frame) => stats.update(frame));
  realtime.on("rtt", ({ rttMs }) => stats.setRoundTrip(rttMs));
  realtime.on("server-error", (frame) => toaster.error("Gateway rejected a message", frame.detail));
  realtime.on("frame-error", () => toaster.error("Received a message it could not read"));
  realtime.on("unauthorized", () =>
    signOut({ message: "Your session expired. Please sign in again." }),
  );
  realtime.on("session-limit", () => {
    toaster.show({
      title: "Too many sessions",
      detail: "Close one of your other GeoTrack tabs, then press the connection pill to retry.",
      tone: "warning",
      timeoutMs: 0,
    });
  });
  realtime.on("slow-consumer", () => {
    toaster.show({
      title: "Connection fell behind",
      detail: "The map was dropped because it could not keep up; reconnecting.",
      tone: "warning",
    });
  });

  if (viewport) realtime.setViewport(viewport);
  realtime.connect();
}

async function onHello(frame) {
  sessionsView.render([], frame.session_id);
  if (!frame.resumed) return;

  // Anything could have changed while the socket was down, and alerts raised in the
  // gap were never pushed: re-read the state that the stream alone cannot repair.
  toaster.show({ title: "Reconnected", tone: "success", timeoutMs: 2_500 });
  try {
    await zoneEditor.load();
    presence.setSnapshot((await api.zonePresence()).zones);
    devices.setHighlighted(presence.deviceIds);
    refreshZoneViews();
    await backfillAlerts();
  } catch (error) {
    handleApiError(error, "Could not refresh after reconnecting");
  }
}

function onPositions(frame) {
  devices.apply(frame.items, { full: frame.full });
  if (frame.removed?.length) devices.remove(frame.removed);
  mapView.invalidate();

  const selected = deviceCard?.deviceId;
  if (selected) {
    const device = devices.get(selected);
    if (device) {
      deviceCard.update({
        latitude: device.latitude,
        longitude: device.longitude,
        reportedMs: device.reportedMs,
        zoneNames: zoneNamesFor(selected),
      });
    }
  }
}

function onAlert(frame) {
  const alert = frame.alert;
  if (!alertLog.add(alert)) return;
  pendingAlerts.push(alert);

  if (presence.applyAlert(alert)) {
    devices.setHighlighted(presence.deviceIds);
    mapView.invalidate();
  }

  const toast = toastPolicy.offer(alert, Date.now());
  if (toast?.kind === "single") {
    const summary = `${alert.kind} · ${alert.device_id} · ${alert.zone?.name ?? "zone"}`;
    toaster.show({ title: alertTitle(alert), detail: summary, tone: toneFor(alert.kind) });
    announcer.say(summary);
  }
}

function flushAlerts() {
  const burst = toastPolicy.flush(Date.now());
  if (burst) {
    toaster.show({ title: `${burst.count} more alerts`, tone: "info", timeoutMs: 3_000 });
    announcer.say(`${burst.count} more alerts`);
  }
  if (pendingAlerts.length === 0) return;
  alertFeed.prepend(pendingAlerts, alertLog.items.length);
  pendingAlerts = [];
  refreshZoneCounts();
}

async function backfillAlerts() {
  const page = await api.listAlerts({ afterId: alertLog.lastId ?? undefined, limit: 200 });
  for (const alert of page.items) presence.applyAlert(alert);
  alertLog.addMany(page.items);
  devices.setHighlighted(presence.deviceIds);
  alertFeed.rebuild(alertLog.items);
  mapView.invalidate();
}

// --- views -----------------------------------------------------------------

function refreshZoneViews() {
  zonePanel.render(zoneEditor.zones, presence);
  alertFeed.setZones(zoneEditor.zones);
}

function refreshZoneCounts() {
  zonePanel.render(zoneEditor.zones, presence);
}

function showDevice(device) {
  if (!device) return;
  mapView.clearTrack();
  deviceCard.show({
    id: device.id,
    latitude: device.latitude,
    longitude: device.longitude,
    reportedMs: device.reportedMs,
    zoneNames: zoneNamesFor(device.id),
  });
}

async function showTrack(deviceId) {
  try {
    const since = new Date(Date.now() - TRACK_WINDOW_MS).toISOString();
    const track = await api.deviceTrack(deviceId, { since, limit: 1_000 });
    if (track.points.length < 2) {
      toaster.show({ title: "No recent track", detail: "This device has too few reports stored." });
      return;
    }
    mapView.showTrack(track.points);
    announcer.say(`Showing ${track.points.length} recent points for ${deviceId}.`);
  } catch (error) {
    handleApiError(error, "Could not load the track");
  }
}

function focusAlert(alert) {
  mapView.flyTo(alert.latitude, alert.longitude);
  const device = devices.get(alert.device_id);
  deviceCard.show({
    id: alert.device_id,
    latitude: device?.latitude ?? alert.latitude,
    longitude: device?.longitude ?? alert.longitude,
    reportedMs: device?.reportedMs ?? Date.parse(alert.occurred_at),
    zoneNames: zoneNamesFor(alert.device_id),
  });
}

function zoneNamesFor(deviceId) {
  return [...presence.zonesOf(deviceId)]
    .map((zoneId) => zoneEditor.zones.get(zoneId)?.name)
    .filter(Boolean);
}

const alertTitle = (alert) =>
  ({ enter: "Device entered a zone", exit: "Device left a zone", dwell: "Device still inside" })[
    alert.kind
  ] ?? "Alert";

const toneFor = (kind) => ({ enter: "success", exit: "warning", dwell: "info" })[kind] ?? "info";

function handleApiError(error, title) {
  if (error instanceof ApiError && error.isUnauthorized) {
    signOut({ message: "Your session expired. Please sign in again." });
    return;
  }
  toaster.error(title, error instanceof ApiError ? error.message : String(error));
}

function safeLocalStorage() {
  try {
    return globalThis.localStorage;
  } catch {
    return null;
  }
}

// --- start -----------------------------------------------------------------

async function boot() {
  const token = tokens.get();
  if (!token) {
    byId("login").hidden = false;
    loginView.focus();
    return;
  }
  api.setToken(token);
  try {
    const user = await api.me();
    await startSession(user);
  } catch (error) {
    tokens.clear();
    api.setToken(null);
    byId("login").hidden = false;
    if (error instanceof ApiError && !error.isUnauthorized) {
      loginView.showError(error.message);
    }
    loginView.focus();
  }
}

boot().catch((error) => {
  byId("login").hidden = false;
  toaster.error("The dashboard failed to start", String(error?.message ?? error));
});
