/**
 * StreetIQ Property Value Insights
 * Production-ready backend module for official Israeli government real estate data.
 * Uses ONLY data.gov.il official CKAN API. No scraping. No commercial sources.
 * All user-facing output is English-only.
 */

import {
  toCanonicalAddress,
  cityKeyToEnglish,
  hasHebrew,
} from "./address-canonical";

const FETCH_TIMEOUT_MS = 15000;
const THREE_YEARS_MS = 3 * 365.25 * 24 * 60 * 60 * 1000;

// Israeli Real Estate Transactions (Nadlan) - data.gov.il CKAN datastore
// Direct resource ID - no discovery
const NADLAN_RESOURCE_ID = "ad6680ef-5d46-4654-be8d-7301292a8e48";
const DATA_GOV_IL_DATASTORE_URL = "https://data.gov.il/api/3/action/datastore_search";

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

export type MatchQuality = "exact_building" | "exact_property" | "nearby_building" | "no_reliable_match";

export type PropertyValueInsightsDebug = {
  raw_input_address: { city: string; street: string; house_number: string };
  canonical_address: { city_key: string; street_key: string; house_key: string };
  records_fetched: number;
  records_after_filter: number;
  exact_matches_count: number;
  nearby_matches_count?: number;
  street_matches_found?: string[];
  building_numbers_found?: string[];
  distance_from_requested_m?: number;
  dataset_sample?: Array<{ city: string; street: string; house_number: string; canonical: { city_key: string; street_key: string; house_key: string } }>;
  rejection_reason?: string;
  /** HTTP status of last API response */
  api_status?: number;
  /** Error message from API if request failed */
  api_error?: string;
  /** Number of records returned by API (before any filtering) */
  records_returned?: number;
  /** First 5 raw records from dataset for structure verification */
  raw_dataset_sample?: Record<string, unknown>[];
};

const PROXIMITY_RADIUS_M = 25;

export type PropertyValueInsightsSuccess = {
  address: { city: string; street: string; house_number: string };
  match_quality: MatchQuality;
  latest_transaction: LatestTransaction;
  current_estimated_value: CurrentEstimatedValue;
  building_summary_last_3_years: BuildingSummary;
  explanation?: string;
  debug?: PropertyValueInsightsDebug;
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
 * Normalize house number: 18, 18.0, 18A, 18 A, 18א -> canonical form for exact match.
 */
export function normalizeHouseNumber(val: string | undefined): string {
  if (val == null || typeof val !== "string") return "";
  const s = String(val).trim();
  if (!s) return "";

  const numMatch = s.match(/^(\d+)/);
  const num = numMatch ? numMatch[1] : "";
  const suffixMatch = s.slice(num.length).match(/^[\s.]*([A-Za-zא-ת])?/);
  const suffix = suffixMatch?.[1]?.trim() ?? "";

  if (!num) return s;
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
 * Uses canonical address comparison to handle English (Google) vs Hebrew (dataset) format mismatch.
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

  // Canonical comparison: handles English input vs Hebrew dataset
  const targetCanon = toCanonicalAddress(targetCity, targetStreet, targetHouseNumber);
  const recCanon = toCanonicalAddress(recCity, recStreet, recHouse);

  if (targetCanon.cityKey !== recCanon.cityKey) return false;
  if (targetCanon.streetKey !== recCanon.streetKey) return false;
  if (targetCanon.houseKey !== recCanon.houseKey) return false;

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

/** Extract numeric part of house number for +/- 1 comparison. "10", "10A" -> 10. Never match 10 with 100. */
function parseHouseNumberNumeric(house: string): number | null {
  const m = house.match(/^(\d+)/);
  if (!m) return null;
  const n = parseInt(m[1], 10);
  return Number.isFinite(n) ? n : null;
}

/** Check if record is on same street, same city, and building number within +/- 1. */
function recordMatchesNearbyBuilding(
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

  const targetCanon = toCanonicalAddress(targetCity, targetStreet, targetHouseNumber);
  const recCanon = toCanonicalAddress(recCity, recStreet, recHouse);

  if (targetCanon.cityKey !== recCanon.cityKey) return false;
  if (targetCanon.streetKey !== recCanon.streetKey) return false;

  const targetNum = parseHouseNumberNumeric(targetHouseNumber);
  const recNum = parseHouseNumberNumeric(recHouse);
  if (targetNum == null || recNum == null) return false;
  if (Math.abs(targetNum - recNum) > 1) return false;

  return true;
}

function extractCoordinatesFromRecord(record: Record<string, unknown>): { lat: number; lng: number } | null {
  let lat: number | null = null;
  let lng: number | null = null;
  for (const [key, val] of Object.entries(record)) {
    const k = String(key).toLowerCase();
    const v = parseNumeric(val);
    if (!Number.isFinite(v)) continue;
    if (k.includes("lat") || k === "y") lat = v;
    if (k.includes("lon") || k.includes("lng") || k === "x") lng = v;
  }
  if (lat != null && lng != null && Math.abs(lat) <= 90 && Math.abs(lng) <= 180) {
    return { lat, lng };
  }
  return null;
}

function haversineDistanceMeters(
  lat1: number, lng1: number,
  lat2: number, lng2: number
): number {
  const R = 6371000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLng / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

/**
 * Parse combined Israeli address string into city, street, house number.
 * Handles "דיזנגוף 10, תל אביב", "10 Dizengoff, Tel Aviv", "רחוב דיזנגוף 10 תל אביב", "123 Dizengoff St, Tel Aviv".
 */
function parseCombinedAddress(addr: string): { city: string; street: string; houseNumber: string } {
  const trimmed = addr
    .replace(/^\s*רחוב\s+/i, "")
    .replace(/\b(St|Street|Str|Ave|Avenue|Rd|Road)\b\.?/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
  const parts = trimmed.split(/[,،]/).map((p) => p.trim()).filter(Boolean);

  if (parts[parts.length - 1]?.match(/^(Israel|ישראל)$/i)) {
    parts.pop();
  }

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

  const headers = {
    Accept: "application/json",
    "User-Agent": "StreetIQ/1.0 (Official Government Data)",
    "Accept-Language": "en-US,en;q=0.9,he;q=0.8",
  };

  const FETCH_LIMIT = 750;
  const sortParam = "תאריך_העסקה desc";

  try {
    const inputCanon = toCanonicalAddress(city, street, houseNumber);

    // Single direct request: resource_id + limit=750 + sort (no q)
    // GET https://data.gov.il/api/3/action/datastore_search?resource_id=...&limit=750&sort=...
    const url = `${DATA_GOV_IL_DATASTORE_URL}?${new URLSearchParams({
      resource_id: NADLAN_RESOURCE_ID,
      limit: String(FETCH_LIMIT),
      sort: sortParam,
    }).toString()}`;

    const res = await fetch(url, { headers, signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) });
    const apiStatus = res.status;
    const body = (await res.json().catch(() => ({}))) as {
      success?: boolean;
      error?: { message?: string };
      result?: { records?: Record<string, unknown>[]; total?: number };
    };
    const rawRecords = body?.result?.records ?? [];
    const apiError = !res.ok || body?.success === false
      ? (body?.error?.message ?? `HTTP ${res.status}`)
      : undefined;

    const recordsReturned = rawRecords.length;
    const first5 = rawRecords.slice(0, 5);

    console.log(
      `[property-value-insights] API: status=${apiStatus} records_returned=${recordsReturned} error=${apiError ?? "—"}`
    );
    if (first5.length > 0) {
      console.log("[property-value-insights] First 5 records:", JSON.stringify(first5, null, 2));
    }

    const debugForFailure = {
      api_status: apiStatus,
      api_error: apiError,
      records_returned: recordsReturned,
      records_fetched: recordsReturned,
      records_after_filter: 0,
      exact_matches_count: 0,
      raw_input_address: { city, street, house_number: houseNumber },
      canonical_address: {
        city_key: inputCanon.cityKey,
        street_key: inputCanon.streetKey,
        house_key: inputCanon.houseKey,
      },
      raw_dataset_sample: first5,
    };

    // Only apply street/building filtering AFTER confirming records are returned
    if (rawRecords.length === 0) {
      return {
        message: "Official government API returned an error. Please try again later.",
        error: "API_NO_RECORDS",
        debug: debugForFailure,
      };
    }

    const withGushParcel = rawRecords.filter((r) => hasGushAndParcel(r));
    const records = withGushParcel.length > 0 ? withGushParcel : rawRecords;
    if (records.length === 0) {
      console.log("[property-value-insights] REJECTED: no valid records after filter", { city, street, houseNumber });
      return {
        message: "no transaction found",
        debug: {
          ...debugForFailure,
          rejection_reason: "Records returned but none passed GUSH/PARCEL filter. Check raw_dataset_sample for structure.",
        },
      };
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

    // Limit to relevant city when we have it (canonical matching after fetch)
    const inputCanonForMatch = toCanonicalAddress(city, street, houseNumber);
    const parsedInCity = inputCanonForMatch.cityKey
      ? parsed.filter((t) => toCanonicalAddress(t.city, t.street, t.houseNumber).cityKey === inputCanonForMatch.cityKey)
      : parsed;

    // Debug: street names and building numbers found in dataset
    const streetNamesFound = [...new Set(parsedInCity.map((t) => t.street))].filter(Boolean).slice(0, 15);
    const buildingNumbersFound = [...new Set(parsedInCity.map((t) => t.houseNumber))].filter(Boolean).slice(0, 25);
    console.log("[property-value-insights] Records fetched:", rawRecords.length, "| In city:", parsedInCity.length, "| Streets:", streetNamesFound.length, "| Buildings:", buildingNumbersFound.length);

    // Strict: match only exact building - never use street-level or nearby data
    const houseNorm = normalizeHouseNumber(houseNumber);

    const debugBase: PropertyValueInsightsDebug = {
      raw_input_address: { city, street, house_number: houseNumber },
      canonical_address: {
        city_key: inputCanonForMatch.cityKey,
        street_key: inputCanonForMatch.streetKey,
        house_key: inputCanonForMatch.houseKey,
      },
      records_fetched: rawRecords.length,
      records_after_filter: parsedInCity.length,
      exact_matches_count: 0,
      street_matches_found: streetNamesFound,
      building_numbers_found: buildingNumbersFound,
      api_status: apiStatus,
      api_error: apiError,
      records_returned: recordsReturned,
      raw_dataset_sample: rawRecords.slice(0, 5),
    };

    if (!houseNorm) {
      console.log("[property-value-insights] REJECTED: no house number", debugBase);
      return {
        message: "no reliable exact match found",
        debug: { ...debugBase, rejection_reason: "House number is required for exact building match." },
      };
    }

    const exactMatches = parsedInCity.filter((t) =>
      recordMatchesExactBuilding(t.record, mapping, city, street, houseNumber)
    );
    debugBase.exact_matches_count = exactMatches.length;

    let matches: ParsedTransaction[] = exactMatches;
    let matchQuality: MatchQuality = "exact_building";

    if (exactMatches.length === 0) {
      const nearbyMatches = parsedInCity.filter((t) =>
        recordMatchesNearbyBuilding(t.record, mapping, city, street, houseNumber)
      );
      debugBase.nearby_matches_count = nearbyMatches.length;

      console.log("[property-value-insights] No exact match. Street matches:", debugBase.street_matches_found?.length ?? 0, "Building numbers:", debugBase.building_numbers_found ?? []);

      if (nearbyMatches.length > 0) {
        matches = nearbyMatches;
        matchQuality = "nearby_building";
        console.log("[property-value-insights] Using nearby_building fallback:", nearbyMatches.length, "matches");
      } else if (input.latitude != null && input.longitude != null && Number.isFinite(input.latitude) && Number.isFinite(input.longitude)) {
        const inputLat = input.latitude;
        const inputLng = input.longitude;
        const withCoords = parsedInCity.filter((t) => {
          const coords = extractCoordinatesFromRecord(t.record);
          if (!coords) return false;
          const dist = haversineDistanceMeters(inputLat, inputLng, coords.lat, coords.lng);
          if (dist > PROXIMITY_RADIUS_M) return false;
          const c = toCanonicalAddress(t.city, t.street, t.houseNumber);
          return c.streetKey === inputCanonForMatch.streetKey && c.cityKey === inputCanonForMatch.cityKey;
        });
        if (withCoords.length > 0) {
          withCoords.sort((a, b) => {
            const coordsA = extractCoordinatesFromRecord(a.record);
            const coordsB = extractCoordinatesFromRecord(b.record);
            if (!coordsA || !coordsB) return 0;
            const distA = haversineDistanceMeters(inputLat, inputLng, coordsA.lat, coordsA.lng);
            const distB = haversineDistanceMeters(inputLat, inputLng, coordsB.lat, coordsB.lng);
            return distA - distB;
          });
          const closest = withCoords[0];
          const coords = extractCoordinatesFromRecord(closest.record);
          const dist = coords ? haversineDistanceMeters(inputLat, inputLng, coords.lat, coords.lng) : 0;
          debugBase.distance_from_requested_m = Math.round(dist * 10) / 10;
          matches = withCoords;
          matchQuality = "nearby_building";
          console.log("[property-value-insights] Using proximity fallback:", withCoords.length, "matches within 25m, closest:", debugBase.distance_from_requested_m, "m");
        }
      }

      if (matches.length === 0) {
        debugBase.dataset_sample = parsedInCity.slice(0, 5).map((t) => {
          const c = toCanonicalAddress(t.city, t.street, t.houseNumber);
          return {
            city: t.city,
            street: t.street,
            house_number: t.houseNumber,
            canonical: { city_key: c.cityKey, street_key: c.streetKey, house_key: c.houseKey },
          };
        });
        const mismatchHint = debugBase.dataset_sample?.length
          ? ` Sample dataset values did not match. Street matches: ${debugBase.street_matches_found?.length ?? 0}, building numbers: ${(debugBase.building_numbers_found ?? []).join(", ")}`
          : "";
        console.log("[property-value-insights] REJECTED: no exact or nearby match", debugBase);
        const sanitizedDebug = { ...debugBase };
        if (sanitizedDebug.dataset_sample) {
          sanitizedDebug.dataset_sample = sanitizedDebug.dataset_sample.map((s) => ({
            city: hasHebrew(s.city) ? "[Hebrew]" : s.city,
            street: hasHebrew(s.street) ? "[Hebrew]" : s.street,
            house_number: s.house_number,
            canonical: s.canonical,
          }));
        }
        return {
          message: "no reliable exact match found",
          debug: {
            ...sanitizedDebug,
            rejection_reason: `No transactions matched. Input: city_key=${inputCanonForMatch.cityKey}, street_key=${inputCanonForMatch.streetKey}, house_key=${inputCanonForMatch.houseKey}.${mismatchHint}`,
          },
        };
      }
    }

    const latest = matches[0];
    const latestDate = latest.saleDate ?? "";
    const latestPrice = latest.salePrice;
    const latestSize = latest.propertySize;

    const pricePerM2 =
      latestPrice > 0 && latestSize > 0 ? Math.round((latestPrice / latestSize) * 100) / 100 : 0;

    // Current estimated value: from latest matched transaction (exact or nearby)
    let currentEstimatedValue: CurrentEstimatedValue = null;
    if (latestPrice > 0 && latestSize > 0) {
      const estimatedPricePerM2 = latestPrice / latestSize;
      const methodText = matchQuality === "exact_building"
        ? "Based only on the latest exact official transaction. This is NOT an official appraisal."
        : "Based on the closest verified transaction on this street. This is NOT an official appraisal.";
      currentEstimatedValue = {
        estimated_value: Math.round(estimatedPricePerM2 * latestSize),
        estimated_price_per_m2: Math.round(estimatedPricePerM2 * 100) / 100,
        estimation_method: methodText,
      };
    }

    // Building summary: last 3 years, same matched set (exact or nearby)
    const buildingLast3 = filterLast3Years(matches);
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

    const displayCity = cityKeyToEnglish(inputCanonForMatch.cityKey) || city;
    const displayStreet = street; // Keep original; dataset street may be Hebrew but we display user's input

    const explanation = matchQuality === "exact_building"
      ? `Exact match for ${displayCity}, ${displayStreet} ${houseNumber}. ${matches.length} official transaction(s) for this building.`
      : `Based on the closest verified transaction on this street.`;

    return {
      address: { city: displayCity, street: displayStreet, house_number: houseNumber },
      match_quality: matchQuality,
      latest_transaction: {
        transaction_date: latestDate,
        transaction_price: latestPrice,
        property_size: latestSize,
        price_per_m2: pricePerM2,
      },
      current_estimated_value: currentEstimatedValue,
      building_summary_last_3_years: buildingSummary,
      explanation,
      debug: debugBase,
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
