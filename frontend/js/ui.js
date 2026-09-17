/**
 * Chrome around the map: login, header, toasts, the device card and the panel shell.
 *
 * Every view takes the elements it owns, so nothing here runs on import and the module
 * stays testable outside a browser.
 */

import { formatClock, formatCoordinate, formatCount, formatRate, relativeAge } from "./format.js";

export const byId = (id) => document.getElementById(id);

export function prefersReducedMotion() {
  return globalThis.matchMedia?.("(prefers-reduced-motion: reduce)").matches ?? false;
}

/**
 * Transient messages. Errors stay until dismissed; everything else expires.
 *
 * The stack is bounded. Rate limiting upstream decides how many alerts are worth
 * announcing per second, but a steady few per second still piles up faster than the
 * five-second lifetime clears, and a column of cards down the whole window hides the
 * map it is reporting on. Past the ceiling the oldest goes, preferring ones that would
 * have expired on their own over ones the user is meant to act on. Nothing is lost:
 * every alert is in the feed, and the aggregate toast counts the burst.
 */
export class Toaster {
  #container;
  #doc;
  #timers;
  #maxVisible;
  #expiry = new Map();
  #live = [];
  #nextId = 1;

  constructor(container, { doc = globalThis.document, timers = globalThis, maxVisible = 4 } = {}) {
    this.#container = container;
    this.#doc = doc;
    this.#timers = timers;
    this.#maxVisible = maxVisible;
  }

  /** A phone shows a sliver of map, so fewer notifications may sit on top of it. */
  setMaxVisible(maxVisible) {
    this.#maxVisible = maxVisible;
    while (this.#live.length > this.#maxVisible) this.#evictOldest();
  }

  show({ title, detail = "", tone = "info", timeoutMs = 5_000 }) {
    const id = this.#nextId++;
    this.#makeRoom();
    const toast = this.#doc.createElement("div");
    toast.className = `toast toast--${tone}`;
    toast.dataset.toastId = String(id);

    const text = this.#doc.createElement("div");
    text.className = "toast__text";
    const heading = this.#doc.createElement("strong");
    heading.className = "toast__title";
    heading.textContent = title;
    text.append(heading);
    if (detail) {
      const note = this.#doc.createElement("span");
      note.className = "toast__detail";
      note.textContent = detail;
      text.append(note);
    }

    const close = this.#doc.createElement("button");
    close.className = "iconbutton";
    close.type = "button";
    close.setAttribute("aria-label", "Dismiss notification");
    close.textContent = "✕";
    close.addEventListener("click", () => this.dismiss(id));

    toast.append(text, close);
    this.#container.append(toast);
    this.#live.push({ id, sticky: timeoutMs <= 0 });

    if (timeoutMs > 0) {
      this.#expiry.set(id, this.#timers.setTimeout(() => this.dismiss(id), timeoutMs));
    }
    return id;
  }

  error(title, detail) {
    return this.show({ title, detail, tone: "error", timeoutMs: 9_000 });
  }

  dismiss(id) {
    const timer = this.#expiry.get(id);
    if (timer !== undefined) this.#timers.clearTimeout(timer);
    this.#expiry.delete(id);
    this.#live = this.#live.filter((entry) => entry.id !== id);
    this.#container.querySelector(`[data-toast-id="${id}"]`)?.remove();
  }

  /** Free a slot for one more toast. */
  #makeRoom() {
    while (this.#live.length >= this.#maxVisible) this.#evictOldest();
  }

  /** Oldest and most disposable first: a message the user must act on goes last. */
  #evictOldest() {
    const oldest = this.#live.find((entry) => !entry.sticky) ?? this.#live[0];
    this.dismiss(oldest.id);
  }
}

/** The connection pill, including the countdown to the next reconnect attempt. */
export class ConnectionIndicator {
  #root;
  #dot;
  #label;
  #timer = null;
  #retryAt = 0;

  constructor({ root, dot, label, onRetry }) {
    this.#root = root;
    this.#dot = dot;
    this.#label = label;
    root.addEventListener("click", () => {
      if (this.#root.classList.contains("is-down")) onRetry?.();
    });
  }

  set({ state, retryInMs = 0 }) {
    this.#stopCountdown();
    this.#root.classList.remove("is-live", "is-waiting", "is-down");
    this.#dot.classList.remove("is-live");

    switch (state) {
      case "live":
        this.#root.classList.add("is-live");
        this.#label.textContent = "Live";
        break;
      case "connecting":
        this.#root.classList.add("is-waiting");
        this.#label.textContent = "Connecting…";
        break;
      case "reconnecting":
        this.#root.classList.add("is-waiting");
        this.#retryAt = Date.now() + retryInMs;
        this.#label.textContent = this.#countdownText();
        this.#timer = globalThis.setInterval(() => {
          this.#label.textContent = this.#countdownText();
        }, 250);
        break;
      default:
        this.#root.classList.add("is-down");
        this.#label.textContent = "Offline — retry";
    }
  }

  #countdownText() {
    const seconds = Math.max(0, Math.ceil((this.#retryAt - Date.now()) / 1_000));
    return seconds > 0 ? `Reconnecting in ${seconds}s` : "Reconnecting…";
  }

  #stopCountdown() {
    if (this.#timer !== null) {
      globalThis.clearInterval(this.#timer);
      this.#timer = null;
    }
  }
}

export class StatsView {
  #devices;
  #rate;
  #backlog;
  #rtt;

  constructor({ devices, rate, backlog, rtt }) {
    this.#devices = devices;
    this.#rate = rate;
    this.#backlog = backlog;
    this.#rtt = rtt;
  }

  update(frame) {
    this.#devices.textContent = formatCount(frame.devices);
    this.#rate.textContent = formatRate(frame.updates_per_s);
    this.#backlog.textContent = formatCount(frame.backlog);
  }

  setRoundTrip(rttMs) {
    this.#rtt.textContent = Number.isFinite(rttMs) ? `${Math.round(rttMs)} ms` : "—";
  }

  reset() {
    for (const element of [this.#devices, this.#rate, this.#backlog, this.#rtt]) {
      element.textContent = "—";
    }
  }
}

/** The other browsers signed in as this user, from the gateway's session directory. */
export class SessionsView {
  #toggle;
  #popover;
  #list;
  #count;

  constructor({ toggle, popover, list, count }) {
    this.#toggle = toggle;
    this.#popover = popover;
    this.#list = list;
    this.#count = count;

    toggle.addEventListener("click", () => this.#setOpen(popover.hidden));
    document.addEventListener("click", (event) => {
      if (!popover.hidden && !popover.contains(event.target) && !toggle.contains(event.target)) {
        this.#setOpen(false);
      }
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && !popover.hidden) this.#setOpen(false);
    });
  }

  render(sessions, ownSessionId) {
    this.#count.textContent = String(sessions.length);
    this.#list.replaceChildren(
      ...sessions.map((session) => {
        const row = document.createElement("li");
        if (session.id === ownSessionId) row.className = "is-self";
        const label = document.createElement("span");
        label.textContent = session.id === ownSessionId ? `${session.label} (this tab)` : session.label;
        const when = document.createElement("span");
        when.className = "sessions__when";
        when.textContent = formatClock(session.connected_at);
        row.append(label, when);
        return row;
      }),
    );
    if (sessions.length === 0) {
      const row = document.createElement("li");
      row.textContent = "No other sessions.";
      this.#list.append(row);
    }
  }

  #setOpen(open) {
    this.#popover.hidden = !open;
    this.#toggle.setAttribute("aria-expanded", String(open));
  }
}

export class LoginView {
  #form;
  #input;
  #error;
  #submit;

  constructor({ form, input, error, submit, onSubmit }) {
    this.#form = form;
    this.#input = input;
    this.#error = error;
    this.#submit = submit;
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      this.clearError();
      onSubmit(this.#input.value);
    });
  }

  focus() {
    this.#input.focus();
  }

  setBusy(busy) {
    this.#submit.disabled = busy;
    this.#submit.textContent = busy ? "Signing in…" : "Sign in";
  }

  showError(message) {
    this.#error.textContent = message;
    this.#error.hidden = false;
  }

  clearError() {
    this.#error.hidden = true;
    this.#error.textContent = "";
  }

  reset() {
    this.#form.reset();
    this.clearError();
    this.setBusy(false);
  }
}

/** Details for the device the user picked, with an age that keeps ticking. */
export class DeviceCard {
  #root;
  #fields;
  #deviceId = null;
  #reportedMs = null;
  #timer = null;
  #onTrack;
  #onFollow;
  #onClose;

  constructor({ root, fields, buttons, onTrack, onFollow, onClose }) {
    this.#root = root;
    this.#fields = fields;
    this.#onTrack = onTrack;
    this.#onFollow = onFollow;
    this.#onClose = onClose;

    buttons.track.addEventListener("click", () => this.#deviceId && this.#onTrack(this.#deviceId));
    buttons.follow.addEventListener("click", () => this.#deviceId && this.#onFollow(this.#deviceId));
    buttons.close.addEventListener("click", () => this.hide());
  }

  get deviceId() {
    return this.#deviceId;
  }

  show({ id, latitude, longitude, reportedMs, zoneNames }) {
    this.#deviceId = id;
    this.#reportedMs = reportedMs;
    this.#fields.id.textContent = id;
    this.#fields.latitude.textContent = formatCoordinate(latitude);
    this.#fields.longitude.textContent = formatCoordinate(longitude);
    this.#fields.zones.textContent = zoneNames.length > 0 ? zoneNames.join(", ") : "none";
    this.#root.hidden = false;
    this.#tick();
    if (this.#timer === null) this.#timer = globalThis.setInterval(() => this.#tick(), 1_000);
  }

  /** Same device, fresher position: refresh in place rather than reopening the card. */
  update({ latitude, longitude, reportedMs, zoneNames }) {
    if (this.#root.hidden) return;
    this.#reportedMs = reportedMs;
    this.#fields.latitude.textContent = formatCoordinate(latitude);
    this.#fields.longitude.textContent = formatCoordinate(longitude);
    if (zoneNames) this.#fields.zones.textContent = zoneNames.length > 0 ? zoneNames.join(", ") : "none";
    this.#tick();
  }

  hide() {
    this.#root.hidden = true;
    this.#deviceId = null;
    if (this.#timer !== null) {
      globalThis.clearInterval(this.#timer);
      this.#timer = null;
    }
    this.#onClose?.();
  }

  #tick() {
    this.#fields.age.textContent = relativeAge(
      this.#reportedMs === null ? null : Date.now() - this.#reportedMs,
    );
  }
}

/** Tabs on the desktop, a collapsible sheet on a phone. */
export class PanelShell {
  #panel;
  #tabs;
  #panes;
  #active;

  constructor({ panel, handle, tabs, panes, compact = null }) {
    this.#panel = panel;
    this.#tabs = tabs;
    this.#panes = panes;
    this.#active = Object.keys(tabs)[0];

    for (const [name, tab] of Object.entries(tabs)) {
      tab.addEventListener("click", () => this.select(name));
    }
    handle.addEventListener("click", () => this.toggleCollapsed());

    // Beside the map the panel is a column and always open; over it, on a phone, it is a
    // drawer — opening it by default would hand the user a list where they expected a map.
    if (compact) {
      this.setCollapsed(compact.matches);
      compact.addEventListener?.("change", (event) => this.setCollapsed(event.matches));
    }
  }

  select(name) {
    this.#active = name;
    for (const [key, tab] of Object.entries(this.#tabs)) {
      const active = key === name;
      tab.classList.toggle("is-active", active);
      tab.setAttribute("aria-selected", String(active));
      this.#panes[key].hidden = !active;
    }
    this.setCollapsed(false);
  }

  get active() {
    return this.#active;
  }

  toggleCollapsed() {
    this.setCollapsed(!this.#panel.classList.contains("is-collapsed"));
  }

  setCollapsed(collapsed) {
    this.#panel.classList.toggle("is-collapsed", collapsed);
    this.#panel.querySelector(".panel__handle")?.setAttribute("aria-expanded", String(!collapsed));
  }
}

/** Screen-reader announcements for events that are otherwise only visual. */
export class Announcer {
  #element;

  constructor(element) {
    this.#element = element;
  }

  say(message) {
    this.#element.textContent = message;
  }
}
