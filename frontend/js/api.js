/**
 * REST client for `/api/v1`.
 *
 * Every failure — transport, problem+json, or an error page from the proxy — leaves
 * here as an `ApiError` carrying a sentence the UI can show without further work.
 */

const BASE = "/api/v1";

export class ApiError extends Error {
  constructor(message, { status = 0, code = "error", detail = null, body = null } = {}) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.detail = detail;
    this.body = body;
  }

  get isUnauthorized() {
    return this.status === 401;
  }

  get isConflict() {
    return this.status === 409 || this.status === 412;
  }

  /** The server is up but shedding load; the caller may retry. */
  get isBusy() {
    return this.status === 503;
  }
}

export class ApiClient {
  #fetch;
  #base;
  #token = null;

  constructor({ fetchImpl, base = BASE } = {}) {
    this.#fetch = fetchImpl ?? ((...args) => globalThis.fetch(...args));
    this.#base = base;
  }

  get token() {
    return this.#token;
  }

  setToken(token) {
    this.#token = token;
  }

  // --- auth ---------------------------------------------------------------

  async login(username) {
    const session = await this.#request("POST", "/auth/login", {
      body: { username: username.trim().toLowerCase() },
    });
    this.#token = session.access_token;
    return session;
  }

  me() {
    return this.#request("GET", "/auth/me");
  }

  // --- geozones -----------------------------------------------------------

  listZones({ limit = 500, offset = 0 } = {}) {
    return this.#request("GET", "/geozones", { query: { limit, offset } });
  }

  getZone(zoneId) {
    return this.#request("GET", `/geozones/${encodeURIComponent(zoneId)}`);
  }

  createZone(zone) {
    return this.#request("POST", "/geozones", { body: zone });
  }

  /** Partial update; `version` sends `If-Match` so a stale panel cannot clobber a peer. */
  updateZone(zoneId, changes, { version } = {}) {
    return this.#request("PATCH", `/geozones/${encodeURIComponent(zoneId)}`, {
      body: changes,
      headers: version === undefined ? {} : { "If-Match": `"${version}"` },
    });
  }

  deleteZone(zoneId) {
    return this.#request("DELETE", `/geozones/${encodeURIComponent(zoneId)}`);
  }

  zonePresence() {
    return this.#request("GET", "/geozones/presence");
  }

  zoneDevices(zoneId, { limit = 500 } = {}) {
    return this.#request("GET", `/geozones/${encodeURIComponent(zoneId)}/devices`, {
      query: { limit },
    });
  }

  // --- alerts and devices -------------------------------------------------

  listAlerts({ afterId, beforeId, limit = 200, zoneId, deviceId } = {}) {
    return this.#request("GET", "/alerts", {
      query: {
        after_id: afterId,
        before_id: beforeId,
        limit,
        zone_id: zoneId,
        device_id: deviceId,
      },
    });
  }

  listDevices({ bbox, limit = 1_000 } = {}) {
    return this.#request("GET", "/devices", {
      query: { bbox: bbox ? bbox.join(",") : undefined, limit },
    });
  }

  device(deviceId) {
    return this.#request("GET", `/devices/${encodeURIComponent(deviceId)}`);
  }

  deviceTrack(deviceId, { since, until, limit } = {}) {
    return this.#request("GET", `/devices/${encodeURIComponent(deviceId)}/track`, {
      query: { since, until, limit },
    });
  }

  // --- plumbing -----------------------------------------------------------

  async #request(method, path, { query, body, headers = {}, signal } = {}) {
    const url = this.#base + path + encodeQuery(query);
    const requestHeaders = { Accept: "application/json", ...headers };
    if (this.#token) requestHeaders.Authorization = `Bearer ${this.#token}`;
    if (body !== undefined) requestHeaders["Content-Type"] = "application/json";

    let response;
    try {
      response = await this.#fetch(url, {
        method,
        headers: requestHeaders,
        body: body === undefined ? undefined : JSON.stringify(body),
        signal,
      });
    } catch (error) {
      if (error?.name === "AbortError") throw error;
      throw new ApiError("Could not reach the server. Check your connection and retry.", {
        code: "network_unreachable",
        detail: String(error?.message ?? error),
      });
    }

    if (!response.ok) throw await problemToError(response);
    if (response.status === 204) return null;
    return response.json();
  }
}

function encodeQuery(query) {
  if (!query) return "";
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value === undefined || value === null || value === "") continue;
    params.set(key, String(value));
  }
  const encoded = params.toString();
  return encoded ? `?${encoded}` : "";
}

async function problemToError(response) {
  const body = await readJson(response);
  if (!body) {
    return new ApiError(`The server answered ${response.status}. Please retry.`, {
      status: response.status,
      code: "http_error",
    });
  }
  return new ApiError(describeProblem(body, response.status), {
    status: response.status,
    code: body.code ?? "http_error",
    detail: body.detail ?? null,
    body,
  });
}

async function readJson(response) {
  try {
    return await response.json();
  } catch {
    // An error page from the proxy, or an empty body.
    return null;
  }
}

function describeProblem(body, status) {
  const fields = (body.errors ?? [])
    .map((error) => {
      const field = Array.isArray(error.loc) ? error.loc[error.loc.length - 1] : null;
      return field ? `${field}: ${error.msg}` : error.msg;
    })
    .filter(Boolean);
  if (fields.length > 0) return fields.join("; ");
  return body.detail || body.title || `The server answered ${status}.`;
}
