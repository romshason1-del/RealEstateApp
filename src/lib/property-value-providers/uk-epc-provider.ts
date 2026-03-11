/**
 * UK EPC (Energy Performance Certificate) Provider
 * Uses DLUHC Open Data: https://epc.opendatacommunities.org/
 * Extracts floor area (m²) for valuation: estimated_value = floor_area × average_price_per_m²
 * Max 5s timeout - EPC failure must never block the valuation response.
 */

const EPC_SEARCH_URL = "https://epc.opendatacommunities.org/api/v1/domestic/search";
const EPC_TIMEOUT_MS = 5000;

export type EPCResult = {
  total_floor_area_m2: number;
  address: string;
  postcode: string;
} | null;

function parseFloatSafe(val: unknown): number {
  if (typeof val === "number" && Number.isFinite(val) && val > 0) return val;
  const n = parseFloat(String(val ?? "").replace(/[^\d.]/g, ""));
  return Number.isFinite(n) && n > 0 ? n : 0;
}

function getAuthHeader(): string | null {
  const email = (process.env.EPC_API_EMAIL ?? "").trim();
  const key = (process.env.EPC_API_KEY ?? "").trim();
  if (!email || !key) return null;
  const encoded = Buffer.from(`${email}:${key}`, "utf-8").toString("base64");
  return `Basic ${encoded}`;
}

function logEPC(method: string, status: number | string, detail?: string): void {
  const msg = `[EPC] ${method} status=${status}${detail ? ` ${detail}` : ""}`;
  if (status === "error" || (typeof status === "number" && status >= 400)) {
    console.warn(msg);
  } else if (typeof process !== "undefined" && process.env?.NODE_ENV === "development") {
    console.debug(msg);
  }
}

/**
 * Search EPC by postcode. Returns floor areas for matching properties.
 */
async function searchEPCByPostcode(postcode: string): Promise<Array<{ total_floor_area_m2: number; address: string }>> {
  const auth = getAuthHeader();
  if (!auth) return [];

  const pc = (postcode ?? "").trim().replace(/\s+/g, "").toUpperCase();
  if (!pc || pc.length < 4) return [];

  try {
    const params = new URLSearchParams({ postcode: pc, size: "100" });
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), EPC_TIMEOUT_MS);
    const res = await fetch(`${EPC_SEARCH_URL}?${params.toString()}`, {
      headers: { Accept: "application/json", Authorization: auth },
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    logEPC("postcode", res.status, `postcode=${pc}`);

    if (!res.ok) {
      logEPC("postcode", res.status, "non-OK, skipping");
      return [];
    }

    const data = (await res.json()) as { rows?: Array<Record<string, unknown>> };
    const rows = data?.rows ?? [];
    const out: Array<{ total_floor_area_m2: number; address: string }> = [];

    for (const row of rows) {
      const area = parseFloatSafe(row["total-floor-area"] ?? row["total_floor_area"] ?? row["TOTAL_FLOOR_AREA"]);
      if (area <= 0) continue;
      const addr = [row["address"], row["address1"], row["ADDRESS1"], row["address-1"]]
        .map((a) => String(a ?? "").trim())
        .find(Boolean) ?? "";
      out.push({ total_floor_area_m2: area, address: addr });
    }
    return out;
  } catch (e) {
    logEPC("postcode", "error", e instanceof Error ? e.message : String(e));
    return [];
  }
}

/**
 * Search EPC by address string (e.g. "10 Downing Street" or "liverpool road").
 */
async function searchEPCByAddress(address: string): Promise<Array<{ total_floor_area_m2: number; address: string }>> {
  const auth = getAuthHeader();
  if (!auth) return [];

  const q = (address ?? "").trim();
  if (!q || q.length < 3) return [];

  try {
    const params = new URLSearchParams({ address: q, size: "50" });
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), EPC_TIMEOUT_MS);
    const res = await fetch(`${EPC_SEARCH_URL}?${params.toString()}`, {
      headers: { Accept: "application/json", Authorization: auth },
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    logEPC("address", res.status, `address=${q.slice(0, 30)}`);

    if (!res.ok) {
      logEPC("address", res.status, "non-OK, skipping");
      return [];
    }

    const data = (await res.json()) as { rows?: Array<Record<string, unknown>> };
    const rows = data?.rows ?? [];
    const out: Array<{ total_floor_area_m2: number; address: string }> = [];

    for (const row of rows) {
      const area = parseFloatSafe(row["total-floor-area"] ?? row["total_floor_area"] ?? row["TOTAL_FLOOR_AREA"]);
      if (area <= 0) continue;
      const addr = [row["address"], row["address1"], row["ADDRESS1"]]
        .map((a) => String(a ?? "").trim())
        .find(Boolean) ?? "";
      out.push({ total_floor_area_m2: area, address: addr });
    }
    return out;
  } catch (e) {
    logEPC("address", "error", e instanceof Error ? e.message : String(e));
    return [];
  }
}

/**
 * Get floor area for a specific property. Tries address first, then postcode.
 * Returns the best match (exact address > postcode with address match).
 */
export async function fetchEPCFloorArea(
  postcode: string,
  addressParts?: { houseNumber?: string; street?: string; city?: string }
): Promise<EPCResult | null> {
  const pc = (postcode ?? "").trim();
  const houseNum = (addressParts?.houseNumber ?? "").trim();
  const street = (addressParts?.street ?? "").trim();
  const city = (addressParts?.city ?? "").trim();

  const fullAddress = [houseNum, street, city].filter(Boolean).join(" ");
  const searchByAddress = fullAddress.length >= 5;

  if (searchByAddress) {
    const byAddr = await searchEPCByAddress(fullAddress);
    if (byAddr.length > 0) {
      const best = byAddr.reduce((a, b) => (a.total_floor_area_m2 > 0 ? a : b));
      return {
        total_floor_area_m2: best.total_floor_area_m2,
        address: best.address,
        postcode: pc,
      };
    }
  }

  if (pc.length >= 4) {
    const byPostcode = await searchEPCByPostcode(pc);
    if (byPostcode.length > 0) {
      const streetNorm = street.toLowerCase().replace(/[^\w\s]/g, "");
      const matchByStreet = streetNorm
        ? byPostcode.find((r) => r.address.toLowerCase().includes(streetNorm.split(/\s+/)[0] ?? ""))
        : null;
      const best = matchByStreet ?? byPostcode[0];
      return {
        total_floor_area_m2: best.total_floor_area_m2,
        address: best.address,
        postcode: pc,
      };
    }
  }

  return null;
}

/**
 * Get floor areas for properties in a postcode/street. Used to compute average_price_per_m2.
 */
export async function fetchEPCFloorAreasForArea(
  postcode: string,
  street?: string
): Promise<Array<{ total_floor_area_m2: number }>> {
  const byPostcode = await searchEPCByPostcode(postcode);
  if (byPostcode.length === 0) return [];

  if (street && street.trim().length >= 2) {
    const streetNorm = street.toLowerCase().replace(/[^\w\s]/g, "");
    const firstWord = streetNorm.split(/\s+/)[0] ?? "";
    if (firstWord) {
      const onStreet = byPostcode.filter((r) => r.address.toLowerCase().includes(firstWord));
      if (onStreet.length >= 2) {
        return onStreet.map((r) => ({ total_floor_area_m2: r.total_floor_area_m2 }));
      }
    }
  }

  return byPostcode.map((r) => ({ total_floor_area_m2: r.total_floor_area_m2 }));
}

export function isEPCConfigured(): boolean {
  const email = (process.env.EPC_API_EMAIL ?? "").trim();
  const key = (process.env.EPC_API_KEY ?? "").trim();
  return Boolean(email && key);
}
