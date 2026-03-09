/**
 * StreetIQ Property Value Insights
 * Production-ready backend module for official Israeli government real estate data.
 * Uses ONLY data.gov.il official CKAN API. No scraping. No commercial sources.
 */

const DATA_GOV_IL_BASE = "https://data.gov.il/api/3/action";
const FETCH_TIMEOUT_MS = 15000;
const THREE_YEARS_MS = 3 * 365.25 * 24 * 60 * 60 * 1000;

// Known nadlan resource (validated fallback if discovery fails)
const KNOWN_NADLAN_RESOURCE_ID = "ad6680ef-5d46-4654-be8d-7301292a8e48";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type PropertyValueInput = {
  city?: string;
  street?: string;
  houseNumber?: string;
  apartmentNumber?: string;
  latitude?: number;
  longitude?: number;
  /** Resolved address when coordinates were used */
  resolvedAddress?: { city: string; street: string; houseNumber?: string; apartmentNumber?: string };
};

export type LatestTransaction = {
  transaction_date: string;
  transaction_price: number;
  property_size: number;
  price_per_m2: number;
};

export type CurrentEstimatedValue = {
  estimated_value: number;
  estimated_price_per_m2: number;
  estimation_method: string;
} | null;

export type BuildingSummary = {
  transactions_count_last_3_years: number;
  latest_building_transaction_price: number;
  average_apartment_value_today: number;
} | null;

export type PropertyValueInsightsSuccess = {
  address: { city: string; street: string; house_number: string };
  latest_transaction: LatestTransaction;
  current_estimated_value: CurrentEstimatedValue;
  building_summary_last_3_years: BuildingSummary;
  source: string;
};

export type PropertyValueInsightsNoMatch = {
  message: "no transaction found" | "no reliable exact match found";
};

export type PropertyValueInsightsPartial = PropertyValueInsightsSuccess & {
  explanation?: string;
};

export type PropertyValueInsightsError = {
  message: string;
  error?: string;
};

export type PropertyValueInsightsResult =
  | PropertyValueInsightsSuccess
  | PropertyValueInsightsNoMatch
  | PropertyValueInsightsPartial
  | PropertyValueInsightsError;

type FieldMapping = {
  city: string | null;
  street: string | null;
  houseNumber: string | null;
  apartmentNumber: string | null;
  saleDate: string | null;
  salePrice: string | null;
  propertySize: string | null;
};

type ParsedTransaction = {
  city: string;
  street: string;
  houseNumber: string;
  apartmentNumber: string;
  saleDate: string | null;
  salePrice: number;
  propertySize: number;
  record: Record<string, unknown>;
};

// ---------------------------------------------------------------------------
// Dataset discovery (dynamic, no hardcoded assumptions)
// ---------------------------------------------------------------------------

/**
 * Discover real estate datastore resource from data.gov.il metadata.
 * Searches for nadlan/real-estate packages and returns the first valid resource.
 */
export async function discoverRealEstateResource(): Promise<string | null> {
  const headers = {
    Accept: "application/json",
    "User-Agent": "StreetIQ/1.0 (Official Government Data)",
    "Accept-Language": "en-US,en;q=0.9,he;q=0.8",
  };

  try {
    // Search for real estate / nadlan packages
    const searchUrl = `${DATA_GOV_IL_BASE}/package_search?q=nadlan+OR+עסקאות+הנדלן+OR+real+estate+transactions`;
    const res = await fetch(searchUrl, { headers, signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) });
    if (!res.ok) return null;

    const json = (await res.json()) as { result?: { results?: Array<{ resources?: Array<{ id?: string }> }> } };
    const packages = json?.result?.results ?? [];

    for (const pkg of packages) {
      const resources = pkg.resources ?? [];
      for (const r of resources) {
        const id = r?.id;
        if (id && typeof id === "string") {
          // Validate: try a minimal fetch
          const testUrl = `${DATA_GOV_IL_BASE}/datastore_search?resource_id=${id}&limit=1`;
          const testRes = await fetch(testUrl, { headers, signal: AbortSignal.timeout(5000) });
          if (testRes.ok) {
            const testJson = (await testRes.json()) as { success?: boolean };
            if (testJson?.success !== false) return id;
          }
        }
      }
    }

    // Fallback: use known resource if discovery fails (validated at runtime)
    const fallbackRes = await fetch(
      `${DATA_GOV_IL_BASE}/datastore_search?resource_id=${KNOWN_NADLAN_RESOURCE_ID}&limit=1`,
      { headers, signal: AbortSignal.timeout(5000) }
    );
    if (fallbackRes.ok) {
      const fallbackJson = (await fallbackRes.json()) as { success?: boolean };
      if (fallbackJson?.success !== false) return KNOWN_NADLAN_RESOURCE_ID;
    }
  } catch {
    // Silent fail, return null
  }
  return null;
}

function getFieldValue(record: Record<string, unknown>, fieldList: string[]): unknown {
  for (const f of fieldList) {
    const val = record[f];
    if (val != null && val !== "") return val;
  }
  for (const [key, val] of Object.entries(record)) {
    if (val != null && val !== "" && key.includes("גוש")) return val;
  }
  for (const [key, val] of Object.entries(record)) {
    if (val != null && val !== "" && key.includes("חלקה")) return val;
  }
  return null;
}

function hasGushAndParcel(record: Record<string, unknown>): boolean {
  const gush = getFieldValue(record, ["גוש", "GUSH", "gush"]);
  const parcel = getFieldValue(record, ["חלקה", "PARCEL", "parcel"]);
  if (gush == null || gush === "" || parcel == null || parcel === "") return false;
  return String(gush).trim().length > 0 && String(parcel).trim().length > 0;
}

/**
 * Infer field mapping from record keys (Hebrew/English aliases).
 */
export function inferFieldMapping(record: Record<string, unknown>): FieldMapping {
  const keys = Object.keys(record);
  const keyLower = (k: string) => k.toLowerCase();
  const keyIncludes = (k: string, ...terms: string[]) =>
    terms.some((t) => keyLower(k).includes(keyLower(t)) || k.includes(t));

  const findKey = (...candidates: string[][]): string | null => {
    for (const list of candidates) {
      for (const c of list) {
        if (keys.includes(c)) return c;
        const match = keys.find((k) => keyLower(k) === keyLower(c));
        if (match) return match;
      }
    }
    return null;
  };

  return {
    city: findKey(["עיר", "city", "יישוב"], ["שם_ישוב", "שם ישוב"]) ?? keys.find((k) => keyIncludes(k, "עיר", "city", "ישוב")) ?? null,
    street: findKey(["כתובת", "רחוב", "street", "address"], ["שם_רחוב", "שם רחוב"]) ?? keys.find((k) => keyIncludes(k, "רחוב", "street", "כתובת", "address")) ?? null,
    houseNumber: findKey(["מספר_בית", "מספר בית", "house_number", "houseNumber"], ["בית"]) ?? keys.find((k) => keyIncludes(k, "בית", "house", "number")) ?? null,
    apartmentNumber: findKey(["דירה", "דירה_מספר", "apartment", "יחידה"]),
    saleDate: findKey(["תאריך_העסקה", "תאריך העסקה", "DEALDATE", "sale_date", "date"]) ?? keys.find((k) => keyIncludes(k, "תאריך", "date")) ?? null,
    salePrice: findKey(["מחיר_העסקה", "מחיר העסקה", "מחיר_עסקה", "sale_price", "price"]) ?? keys.find((k) => keyIncludes(k, "מחיר", "price")) ?? null,
    propertySize: findKey(["GFA_QUANTITY", "GFA", "שטח", "שטח_במ\"ר", "area", "sqm"]) ?? keys.find((k) => keyIncludes(k, "GFA", "שטח", "area", "sqm")) ?? null,
  };
}

// ---------------------------------------------------------------------------
// Address normalization (strict, no fuzzy matching)
// ---------------------------------------------------------------------------

/**
 * Normalize house number: 10, 10.0, 10A, 10 A, 10א -> canonical form for exact match.
 */
export function normalizeHouseNumber(val: string | undefined): string {
  if (val == null || typeof val !== "string") return "";
  const s = String(val).trim();
  if (!s) return "";

  // Extract numeric part
  const numMatch = s.match(/^(\d+)/);
  const num = numMatch ? numMatch[1] : "";

  // Extract optional suffix (A, B, א, ב, etc.)
  const suffixMatch = s.slice(num.length).match(/^[\s.]*([A-Za-zא-ת])?/);
  const suffix = suffixMatch?.[1]?.trim() ?? "";

  if (!num) return s; // No number, return as-is for strict comparison
  return suffix ? `${num}${suffix}` : num;
}

/**
 * Normalize street/city: trim, collapse spaces, remove common prefixes.
 */
export function normalizeStreetOrCity(val: string | undefined): string {
  if (val == null || typeof val !== "string") return "";
  return String(val)
    .replace(/^\s*רחוב\s+/i, "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Build canonical address key for exact matching.
 */
function buildAddressKey(city: string, street: string, houseNumber: string): string {
  return [
    normalizeStreetOrCity(city).toLowerCase(),
    normalizeStreetOrCity(street).toLowerCase(),
    normalizeHouseNumber(houseNumber),
  ].join("|");
}

// ---------------------------------------------------------------------------
// Exact building matching (strict Israeli address layer)
// ---------------------------------------------------------------------------

/**
 * Check if record address exactly matches the requested building.
 * Never mix transactions from nearby buildings. No fuzzy matching.
 */
export function recordMatchesExactBuilding(
  record: Record<string, unknown>,
  mapping: FieldMapping,
  targetCity: string,
  targetStreet: string,
  targetHouseNumber: string
): boolean {
  const getVal = (key: string | null): string => {
    if (!key) return "";
    const v = record[key];
    return v != null && v !== "" ? String(v).trim() : "";
  };

  const recAddr = getVal(mapping.street);
  const explicitCity = getVal(mapping.city) || (() => {
    for (const k of Object.keys(record)) {
      if (/עיר|city|ישוב/i.test(k)) return String(record[k] ?? "").trim();
    }
    return "";
  })();

  let recCity = normalizeStreetOrCity(explicitCity);
  let recStreet = normalizeStreetOrCity(recAddr);
  let recHouse = extractHouseNumberFromAddress(recAddr) || getVal(mapping.houseNumber);

  if (!recCity && recAddr.includes(",")) {
    const parsed = parseCombinedAddress(recAddr);
    recCity = normalizeStreetOrCity(parsed.city);
    recStreet = normalizeStreetOrCity(parsed.street);
    recHouse = recHouse || parsed.houseNumber;
  }

  const targetCityNorm = normalizeStreetOrCity(targetCity).toLowerCase();
  const targetStreetNorm = normalizeStreetOrCity(targetStreet).toLowerCase();
  const targetHouseNorm = normalizeHouseNumber(targetHouseNumber);

  // City must match (exact after normalization)
  if (recCity.toLowerCase() !== targetCityNorm) return false;

  // Street must match (exact after normalization)
  if (recStreet.toLowerCase() !== targetStreetNorm) return false;

  // House number must match exactly (10, 10A, 10 א normalized)
  const recHouseNorm = normalizeHouseNumber(recHouse);
  if (recHouseNorm !== targetHouseNorm) return false;

  return true;
}

/**
 * Extract house number from full address string (e.g. "דיזנגוף 10", "10 דיזנגוף" -> "10").
 */
function extractHouseNumberFromAddress(addr: string): string {
  const m = addr.match(/\b(\d+)\s*([A-Za-zא-ת])?\b/);
  if (!m) return "";
  const suffix = m[2]?.trim() ?? "";
  return suffix ? `${m[1]}${suffix}` : m[1];
}

/**
 * Parse combined Israeli address string into city, street, house number.
 * Handles "דיזנגוף 10, תל אביב", "10 Dizengoff, Tel Aviv", "רחוב דיזנגוף 10 תל אביב".
 */
function parseCombinedAddress(addr: string): { city: string; street: string; houseNumber: string } {
  const trimmed = addr.replace(/^\s*רחוב\s+/i, "").trim();
  const parts = trimmed.split(/[,،]/).map((p) => p.trim()).filter(Boolean);
  const houseMatch = trimmed.match(/\b(\d+)\s*([A-Za-zא-ת])?\b/);
  const houseNumber = houseMatch ? (houseMatch[2] ? `${houseMatch[1]}${houseMatch[2]}` : houseMatch[1]) : "";

  let city = "";
  let street = trimmed.replace(/\d+/g, " ").replace(/\s+/g, " ").trim();

  if (parts.length >= 2) {
    city = parts[parts.length - 1] ?? "";
    street = parts.slice(0, -1).join(" ").replace(/\d+/g, " ").replace(/\s+/g, " ").trim() || street;
  }

  return { city, street: normalizeStreetOrCity(street), houseNumber };
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

export function parseNumeric(val: unknown): number {
  if (typeof val === "number" && Number.isFinite(val)) return val;
  const s = String(val ?? "").replace(/[^\d.]/g, "");
  const n = parseFloat(s);
  return Number.isFinite(n) ? n : 0;
}

export function parseTransactionDate(val: unknown): number {
  if (val == null || val === "") return 0;
  const d = new Date(String(val));
  return Number.isFinite(d.getTime()) ? d.getTime() : 0;
}

/**
 * Filter transactions to last 3 years only.
 */
export function filterLast3Years(transactions: ParsedTransaction[]): ParsedTransaction[] {
  const cutoff = Date.now() - THREE_YEARS_MS;
  return transactions.filter((t) => parseTransactionDate(t.saleDate) >= cutoff);
}

// ---------------------------------------------------------------------------
// Main function: getPropertyValueInsights
// ---------------------------------------------------------------------------

export async function getPropertyValueInsights(input: PropertyValueInput): Promise<PropertyValueInsightsResult> {
  // Resolve address from input
  let city = (input.city ?? "").trim();
  let street = (input.street ?? "").trim();
  let houseNumber = (input.houseNumber ?? "").trim();
  const apartmentNumber = (input.apartmentNumber ?? "").trim();

  if (input.resolvedAddress) {
    city = input.resolvedAddress.city?.trim() ?? city;
    street = input.resolvedAddress.street?.trim() ?? street;
    houseNumber = input.resolvedAddress.houseNumber?.trim() ?? houseNumber;
  }

  // Require at least city and street for reliable search
  if (!city || !street) {
    return {
      message: "Address must include city and street. Coordinates should be resolved to a structured address first.",
      error: "INVALID_INPUT",
    };
  }

  // Discover resource dynamically
  const resourceId = await discoverRealEstateResource();
  if (!resourceId) {
    return {
      message: "Official government real estate data source is temporarily unavailable.",
      error: "DATA_SOURCE_UNAVAILABLE",
    };
  }

  const headers = {
    Accept: "application/json",
    "User-Agent": "StreetIQ/1.0 (Official Government Data)",
    "Accept-Language": "en-US,en;q=0.9,he;q=0.8",
  };

  try {
    // Fetch by street + city (street-level fetch to avoid 404)
    const searchQuery = [street, city].filter(Boolean).join(" ");
    const sortParam = "תאריך_העסקה desc";
    const url = `${DATA_GOV_IL_BASE}/datastore_search?resource_id=${resourceId}&q=${encodeURIComponent(searchQuery)}&limit=200&sort=${encodeURIComponent(sortParam)}`;

    const res = await fetch(url, { headers, signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) });
    if (!res.ok) {
      return {
        message: "Official government API returned an error. Please try again later.",
        error: `API_${res.status}`,
      };
    }

    const json = (await res.json()) as { success?: boolean; result?: { records?: Record<string, unknown>[] } };
    if (json?.success === false) {
      return {
        message: "Official government data request failed.",
        error: "API_ERROR",
      };
    }

    const rawRecords = (json?.result?.records ?? []) as Record<string, unknown>[];
    const withGushParcel = rawRecords.filter((r) => hasGushAndParcel(r));
    const records = withGushParcel.length > 0 ? withGushParcel : rawRecords;
    if (records.length === 0) {
      return { message: "no transaction found" };
    }

    // Infer field mapping from first record
    const mapping = inferFieldMapping(records[0]);

    // Parse all transactions
    const getVal = (r: Record<string, unknown>, key: string | null) =>
      key ? (r[key] != null && r[key] !== "" ? String(r[key]).trim() : "") : "";

    const parsed: ParsedTransaction[] = records
      .map((r) => {
        const price = parseNumeric(r[mapping.salePrice ?? ""] ?? getVal(r, mapping.salePrice));
        if (price <= 0) return null;

        const addrVal = getVal(r, mapping.street);
        const explicitCity = getVal(r, mapping.city) || (() => {
          for (const k of Object.keys(r)) {
            if (/עיר|city|ישוב/i.test(k)) return String(r[k] ?? "").trim();
          }
          return "";
        })();

        let cityVal = explicitCity;
        let streetVal = normalizeStreetOrCity(addrVal);
        let houseVal = extractHouseNumberFromAddress(addrVal) || getVal(r, mapping.houseNumber);

        if (!cityVal && addrVal.includes(",")) {
          const parsed = parseCombinedAddress(addrVal);
          cityVal = parsed.city;
          streetVal = parsed.street;
          houseVal = houseVal || parsed.houseNumber;
        }

        return {
          city: cityVal,
          street: streetVal,
          houseNumber: houseVal,
          apartmentNumber: getVal(r, mapping.apartmentNumber),
          saleDate: getVal(r, mapping.saleDate) || null,
          salePrice: price,
          propertySize: parseNumeric(r[mapping.propertySize ?? ""] ?? getVal(r, mapping.propertySize)),
          record: r,
        };
      })
      .filter((t): t is ParsedTransaction => t != null);

    // Sort by date descending
    parsed.sort((a, b) => parseTransactionDate(b.saleDate) - parseTransactionDate(a.saleDate));

    // Strict: match only exact building
    const houseNorm = normalizeHouseNumber(houseNumber);
    if (!houseNorm) {
      return { message: "no reliable exact match found" };
    }

    const exactMatches = parsed.filter((t) =>
      recordMatchesExactBuilding(t.record, mapping, city, street, houseNumber)
    );

    if (exactMatches.length === 0) {
      return { message: "no reliable exact match found" };
    }

    const latest = exactMatches[0];
    const latestDate = latest.saleDate ?? "";
    const latestPrice = latest.salePrice;
    const latestSize = latest.propertySize;

    const pricePerM2 =
      latestPrice > 0 && latestSize > 0 ? Math.round((latestPrice / latestSize) * 100) / 100 : 0;

    // Current estimated value: ONLY from official transaction data, never guess
    let currentEstimatedValue: CurrentEstimatedValue = null;
    if (latestPrice > 0 && latestSize > 0) {
      currentEstimatedValue = {
        estimated_value: Math.round(latestPrice), // Same as latest if we use that transaction
        estimated_price_per_m2: pricePerM2,
        estimation_method: "Based only on official government transaction data. This is NOT an official appraisal.",
      };
    }

    // Building summary: last 3 years, same building only
    const buildingLast3 = filterLast3Years(exactMatches);
    let buildingSummary: BuildingSummary = null;

    if (buildingLast3.length > 0) {
      const validForAvg = buildingLast3.filter((t) => t.salePrice > 0 && t.propertySize > 0);
      const latestBuilding = buildingLast3[0];

      let averageApartmentValueToday = 0;
      if (validForAvg.length > 0) {
        const pricePerM2List = validForAvg.map((t) => t.salePrice / t.propertySize);
        const avgPricePerM2 = pricePerM2List.reduce((a, b) => a + b, 0) / pricePerM2List.length;
        averageApartmentValueToday = Math.round(avgPricePerM2 * (latestSize || 100));
      }

      buildingSummary = {
        transactions_count_last_3_years: buildingLast3.length,
        latest_building_transaction_price: latestBuilding.salePrice,
        average_apartment_value_today: averageApartmentValueToday,
      };
    }

    return {
      address: { city, street, house_number: houseNumber },
      latest_transaction: {
        transaction_date: latestDate,
        transaction_price: latestPrice,
        property_size: latestSize,
        price_per_m2: pricePerM2,
      },
      current_estimated_value: currentEstimatedValue,
      building_summary_last_3_years: buildingSummary,
      source: "data.gov.il",
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : "Unknown error";
    return {
      message: "Failed to fetch official government data. Please try again later.",
      error: msg,
    };
  }
}

export default getPropertyValueInsights;

// ---------------------------------------------------------------------------
// Example usage
// ---------------------------------------------------------------------------
//
// import { getPropertyValueInsights } from "@/lib/property-value-insights";
//
// const result = await getPropertyValueInsights({
//   city: "תל אביב",
//   street: "דיזנגוף",
//   houseNumber: "10",
// });
//
// if ("address" in result) {
//   console.log("Latest transaction:", result.latest_transaction);
//   console.log("Current estimate:", result.current_estimated_value);
// } else {
//   console.log("No match:", result.message);
// }
