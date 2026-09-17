/**
 * Spherical helpers shared by the map, the zone editor and the viewport subscription.
 *
 * The server is the authority on every distance it acts upon (PostGIS measures on the
 * WGS84 spheroid). These functions only have to agree closely enough to draw a circle
 * and to recover the centre/radius a drag gesture produced, so a sphere is plenty:
 * the disagreement with the spheroid is a few parts in a thousand, far below the
 * 2% padding the server's candidate filter already carries.
 */

export const EARTH_RADIUS_M = 6_371_008.8;
export const MIN_RADIUS_M = 10;
export const MAX_RADIUS_M = 50_000;

const DEG = Math.PI / 180;
const RAD = 180 / Math.PI;

/** Great-circle distance in metres between two `[lon, lat]` points. */
export function haversineMeters(a, b) {
  const lat1 = a[1] * DEG;
  const lat2 = b[1] * DEG;
  const dLat = lat2 - lat1;
  const dLon = (b[0] - a[0]) * DEG;
  const h =
    Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  return 2 * EARTH_RADIUS_M * Math.asin(Math.min(1, Math.sqrt(h)));
}

/** The point `distanceM` away from `[lon, lat]` along `bearingDeg` (0 = north). */
export function destination([lon, lat], bearingDeg, distanceM) {
  const angular = distanceM / EARTH_RADIUS_M;
  const bearing = bearingDeg * DEG;
  const lat1 = lat * DEG;
  const lon1 = lon * DEG;
  const sinLat = Math.sin(lat1) * Math.cos(angular) +
    Math.cos(lat1) * Math.sin(angular) * Math.cos(bearing);
  const lat2 = Math.asin(sinLat);
  const lon2 = lon1 +
    Math.atan2(
      Math.sin(bearing) * Math.sin(angular) * Math.cos(lat1),
      Math.cos(angular) - Math.sin(lat1) * sinLat,
    );
  return [wrapLongitude(lon2 * RAD), lat2 * RAD];
}

/**
 * A closed ring approximating a circle, first vertex due north.
 *
 * The north-first ordering is what makes `ringToCircle` able to read the radius back
 * from a single vertex, and it matches the convention the drawing plugin uses.
 */
export function circleRing(center, radiusM, steps = 80) {
  const ring = [];
  for (let i = 0; i < steps; i += 1) {
    ring.push(destination(center, (i * 360) / steps, radiusM));
  }
  ring.push(ring[0]);
  return ring;
}

/**
 * Read a circle back out of a polygon ring.
 *
 * `centerHint` is the centre the drawing plugin recorded on the feature; without it
 * the centre is the ring's spherical mean, which is exact for evenly spaced vertices
 * and — unlike averaging longitudes — does not fall apart across the antimeridian.
 */
export function ringToCircle(ring, centerHint) {
  const vertices = closedRing(ring) ? ring.slice(0, -1) : ring.slice();
  if (vertices.length === 0) return null;

  const center = centerHint ?? sphericalMean(vertices);
  const radiusM = vertices.reduce((sum, vertex) => sum + haversineMeters(center, vertex), 0) /
    vertices.length;
  return { center, radiusM };
}

function closedRing(ring) {
  if (ring.length < 2) return false;
  const first = ring[0];
  const last = ring[ring.length - 1];
  return first[0] === last[0] && first[1] === last[1];
}

function sphericalMean(vertices) {
  let x = 0;
  let y = 0;
  let z = 0;
  for (const [lon, lat] of vertices) {
    const latRad = lat * DEG;
    const lonRad = lon * DEG;
    x += Math.cos(latRad) * Math.cos(lonRad);
    y += Math.cos(latRad) * Math.sin(lonRad);
    z += Math.sin(latRad);
  }
  const hypotenuse = Math.hypot(x, y);
  return [Math.atan2(y, x) * RAD, Math.atan2(z, hypotenuse) * RAD];
}

/**
 * Half a metre of centre movement, or of radius.
 *
 * Below that a "change" is the rounding in the ring a drag gesture hands back — the
 * circle is transported as an 80-point polygon and read back by averaging — not the
 * user's hand.
 */
export const CIRCLE_EPSILON_M = 0.5;

/** Did this gesture actually move the circle, or only end where it started? */
export function circleMoved(current, next, epsilonM = CIRCLE_EPSILON_M) {
  const moved = haversineMeters(current.center, next.center);
  return moved >= epsilonM || Math.abs(next.radiusM - current.radiusM) >= epsilonM;
}

/** Round to centimetres and hold the radius inside the range the API accepts. */
export function clampRadius(radiusM) {
  if (!Number.isFinite(radiusM)) return MIN_RADIUS_M;
  const rounded = Math.round(radiusM * 100) / 100;
  return Math.min(MAX_RADIUS_M, Math.max(MIN_RADIUS_M, rounded));
}

/**
 * Fold a longitude into [-180, 180].
 *
 * The antimeridian is reported as +180 (an exact -180 input is left alone), so a
 * viewport whose eastern edge sits on the date line does not read as `west > east`
 * and get mistaken for a wrapping box.
 */
export function wrapLongitude(lon) {
  // Returned untouched when already in range: the modulo below is exact only in
  // theory, and rounding a viewport edge would make every idle map look panned.
  if (lon >= -180 && lon <= 180) return lon;
  const wrapped = (((lon + 180) % 360) + 360) % 360 - 180;
  return wrapped === -180 ? 180 : wrapped;
}

const clampLatitude = (lat) => Math.min(90, Math.max(-90, lat));

/**
 * Turn a map's `LngLatBounds` into the `[west, south, east, north]` the gateway wants.
 *
 * MapLibre happily reports longitudes outside [-180, 180] once the user pans around
 * the world, and a fully zoomed-out map reports a span wider than the globe; the
 * gateway rejects both, so they are normalised here.
 */
export function boundsToBBox(bounds) {
  const rawWest = bounds.getWest();
  const rawEast = bounds.getEast();
  const south = clampLatitude(bounds.getSouth());
  const north = clampLatitude(bounds.getNorth());

  if (![rawWest, rawEast, south, north].every(Number.isFinite)) {
    return [-180, -90, 180, 90];
  }
  const ordered = south <= north ? [south, north] : [north, south];
  if (rawEast - rawWest >= 360) {
    return [-180, ordered[0], 180, ordered[1]];
  }
  return [wrapLongitude(rawWest), ordered[0], wrapLongitude(rawEast), ordered[1]];
}

// ~0.11 m at the equator: below this a "move" is the map settling, not the user panning.
const BBOX_EPSILON = 1e-6;

export function bboxEquals(a, b) {
  if (!a || !b) return false;
  return a.every((value, index) => Math.abs(value - b[index]) < BBOX_EPSILON);
}
