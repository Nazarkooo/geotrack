/**
 * The dashboard's end of the realtime protocol (spec version 1).
 *
 * Responsibilities kept here and nowhere else: the handshake, frame decoding, the
 * viewport subscription, round-trip measurement, and reconnection. Everything that
 * survives a reconnect — the viewport, the token — lives on the client, so a dropped
 * socket costs the user nothing but a short gap.
 */

import { Emitter } from "./state.js";

const PROTOCOL = "geotrack.v1";
const PING_INTERVAL_MS = 5_000;
const DEFAULT_BACKOFF = { baseMs: 500, factor: 1.8, maxMs: 20_000, jitter: 0.25 };

// Application close codes from the protocol; anything else is a transport hiccup.
const CLOSE_NORMAL = 1000;
const CLOSE_SLOW_CONSUMER = 1013;
const CLOSE_UNAUTHORIZED = 4401;
const CLOSE_TOO_MANY_SESSIONS = 4429;

export function websocketUrl(location, path = "/ws") {
  const scheme = location.protocol === "https:" ? "wss:" : "ws:";
  return `${scheme}//${location.host}${path}`;
}

export class RealtimeClient {
  #emitter = new Emitter();
  #url;
  #token;
  #WebSocket;
  #timers;
  #now;
  #random;
  #pingIntervalMs;
  #backoff;

  #socket = null;
  #state = "idle";
  #attempt = 0;
  #retryHandle = null;
  #retryInMs = 0;
  #pingHandle = null;
  #pendingPing = null;
  #viewport = null;
  #sentViewport = null;
  #sessionId = null;
  #everConnected = false;

  constructor({
    url,
    token = null,
    WebSocketImpl,
    timers = globalThis,
    now = () => Date.now(),
    random = Math.random,
    pingIntervalMs = PING_INTERVAL_MS,
    backoff = {},
  }) {
    this.#url = url;
    this.#token = token;
    this.#WebSocket = WebSocketImpl ?? globalThis.WebSocket;
    this.#timers = timers;
    this.#now = now;
    this.#random = random;
    this.#pingIntervalMs = pingIntervalMs;
    this.#backoff = { ...DEFAULT_BACKOFF, ...backoff };
  }

  on(type, listener) {
    return this.#emitter.on(type, listener);
  }

  get state() {
    return this.#state;
  }

  get sessionId() {
    return this.#sessionId;
  }

  /** Milliseconds until the next attempt; the header renders it as a countdown. */
  get retryInMs() {
    return this.#retryInMs;
  }

  setToken(token) {
    this.#token = token;
  }

  connect() {
    if (this.#socket || this.#retryHandle !== null) return;
    this.#open();
  }

  /**
   * The viewport the user is looking at.
   *
   * Stored rather than sent directly: it has to survive a reconnect, and an identical
   * box must not cost a frame — the map fires `moveend` for every inertial settle.
   */
  setViewport(bbox) {
    this.#viewport = bbox;
    this.#flushViewport();
  }

  /** Deliberate shutdown: no retry, no timers left running. */
  close() {
    this.#clearRetry();
    this.#stopPing();
    const socket = this.#socket;
    this.#socket = null;
    this.#sentViewport = null;
    this.#sessionId = null;
    if (socket) {
      socket.onopen = socket.onmessage = socket.onclose = socket.onerror = null;
      if (socket.readyState !== 3) socket.close(CLOSE_NORMAL, "client closed");
    }
    this.#setState("closed");
  }

  // --- internals ----------------------------------------------------------

  #open() {
    this.#clearRetry();
    this.#sentViewport = null;
    this.#setState("connecting");

    const protocols = [PROTOCOL];
    if (this.#token) protocols.push(`bearer.${this.#token}`);

    const socket = new this.#WebSocket(this.#url, protocols);
    socket.binaryType = "arraybuffer";
    this.#socket = socket;

    socket.onopen = () => this.#emitter.emit("open", null);
    socket.onmessage = (event) => this.#receive(event.data);
    socket.onclose = (event) => this.#onClose(socket, event);
    // `error` is always followed by `close`, which carries the code that matters.
    socket.onerror = () => {};
  }

  #receive(data) {
    if (data instanceof ArrayBuffer) {
      this.#dispatch(new TextDecoder().decode(new Uint8Array(data)));
    } else if (ArrayBuffer.isView(data)) {
      this.#dispatch(new TextDecoder().decode(data));
    } else if (typeof data === "string") {
      this.#dispatch(data);
    } else if (typeof data?.arrayBuffer === "function") {
      // A browser that ignored `binaryType`; decoding a Blob is asynchronous.
      data.arrayBuffer().then((buffer) => this.#receive(buffer), () => {});
    }
  }

  #dispatch(text) {
    let frame;
    try {
      frame = JSON.parse(text);
    } catch (error) {
      this.#emitter.emit("frame-error", { error, text });
      return;
    }

    switch (frame.type) {
      case "hello":
        this.#onHello(frame);
        break;
      case "positions":
      case "alert":
      case "zone":
      case "sessions":
      case "stats":
        this.#emitter.emit(frame.type, frame);
        break;
      case "pong":
        this.#onPong(frame);
        break;
      case "error":
        this.#emitter.emit("server-error", frame);
        break;
      default:
        // Forward compatibility: a newer gateway may send frames this build predates.
        break;
    }
  }

  #onHello(frame) {
    this.#sessionId = frame.session_id;
    this.#attempt = 0;
    this.#retryInMs = 0;
    this.#setState("live");
    this.#flushViewport();
    this.#startPing();
    this.#emitter.emit("hello", { ...frame, resumed: this.#everConnected });
    this.#everConnected = true;
  }

  #onPong(frame) {
    if (this.#pendingPing === null || frame.t !== this.#pendingPing.t) return;
    const rttMs = this.#now() - this.#pendingPing.sentAt;
    this.#pendingPing = null;
    this.#emitter.emit("rtt", { rttMs, serverT: frame.server_t });
  }

  #onClose(socket, event) {
    if (socket !== this.#socket) return;
    this.#socket = null;
    this.#stopPing();
    this.#sessionId = null;
    this.#sentViewport = null;

    if (event.code === CLOSE_UNAUTHORIZED) {
      this.#setState("closed");
      this.#emitter.emit("unauthorized", { reason: event.reason });
      return;
    }
    if (event.code === CLOSE_TOO_MANY_SESSIONS) {
      this.#setState("closed");
      this.#emitter.emit("session-limit", { reason: event.reason });
      return;
    }
    if (event.code === CLOSE_SLOW_CONSUMER) {
      this.#emitter.emit("slow-consumer", { reason: event.reason });
    }
    this.#scheduleRetry();
  }

  #scheduleRetry() {
    const { baseMs, factor, maxMs, jitter } = this.#backoff;
    const plain = Math.min(maxMs, baseMs * factor ** this.#attempt);
    this.#attempt += 1;
    this.#retryInMs = Math.round(plain * (1 + jitter * (2 * this.#random() - 1)));
    this.#setState("reconnecting");
    this.#retryHandle = this.#timers.setTimeout(() => {
      this.#retryHandle = null;
      this.#open();
    }, this.#retryInMs);
  }

  #clearRetry() {
    if (this.#retryHandle !== null) {
      this.#timers.clearTimeout(this.#retryHandle);
      this.#retryHandle = null;
    }
  }

  #startPing() {
    this.#stopPing();
    this.#pingHandle = this.#timers.setInterval(() => {
      // A ping still unanswered means the socket is wedged; the next close handles it.
      const t = Math.trunc(this.#now());
      this.#pendingPing = { t, sentAt: this.#now() };
      this.#send({ type: "ping", t });
    }, this.#pingIntervalMs);
  }

  #stopPing() {
    if (this.#pingHandle !== null) {
      this.#timers.clearInterval(this.#pingHandle);
      this.#pingHandle = null;
    }
    this.#pendingPing = null;
  }

  #flushViewport() {
    if (this.#state !== "live" || !this.#viewport) return;
    if (this.#sentViewport && sameBox(this.#sentViewport, this.#viewport)) return;
    if (this.#send({ type: "viewport", bbox: this.#viewport })) {
      this.#sentViewport = this.#viewport;
    }
  }

  #send(payload) {
    if (!this.#socket || this.#socket.readyState !== 1) return false;
    try {
      this.#socket.send(JSON.stringify(payload));
      return true;
    } catch {
      // The socket died between the check and the write; the close handler retries.
      return false;
    }
  }

  #setState(state) {
    this.#state = state;
    this.#emitter.emit("status", {
      state,
      attempt: this.#attempt,
      retryInMs: state === "reconnecting" ? this.#retryInMs : 0,
    });
  }
}

function sameBox(a, b) {
  return a.length === b.length && a.every((value, index) => value === b[index]);
}
