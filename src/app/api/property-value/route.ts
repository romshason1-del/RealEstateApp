/**
 * StreetIQ Property Value API
 * GET /api/property-value?city=...&street=...&houseNumber=...
 * GET /api/property-value?address=... (parsed to city, street, houseNumber)
 * Uses only official Israeli government real estate data (data.gov.il).
 * Returns results ONLY for exact building matches.
 */

import { NextRequest, NextResponse } from "next/server";
import { BigQuery } from "@google-cloud/bigquery";
import { createClient } from "@supabase/supabase-js";
import getPropertyValueInsights from "@/lib/property-value-insights";
import { getFrancePropertyResult, mapLivabilityToRating, normalizeLot } from "@/lib/bigquery-france-service";
import { isBigQueryConfigured } from "@/lib/bigquery-client";
import { parseAddressFromFullString, parseUSAddressFromFullString, parseUKAddressFromFullString, parseFRAddressFromFullString, extractFlatPrefix } from "@/lib/address-parse";
import { fetchNeighborhoodStats } from "@/lib/property-value-providers/us-census-provider";
import { fetchMarketTrend } from "@/lib/property-value-providers/us-fhfa-provider";
import { computeNeighborhoodRating } from "@/lib/neighborhood-rating";
import { fetchUKHPIForLocality, fetchUKHPIIndicesForLocality, estimateValueFromHPI } from "@/lib/property-value-providers/uk-house-price-index-provider";
import { fetchUKNeighborhoodStats, computeUKLivabilityRating } from "@/lib/property-value-providers/uk-ons-census-provider";
import { fetchEPCFloorArea, fetchEPCFloorAreasForArea, isEPCConfigured } from "@/lib/property-value-providers/uk-epc-provider";
import { isUSMockEnabled } from "@/lib/property-value-providers/config";
import { emptyFranceResponse, type FrancePropertyResponse } from "@/lib/france-response-contract";

const CACHE = new Map<string, { data: Record<string, unknown>; ts: number }>();
const CACHE_TTL_MS = 5 * 60 * 1000;
const FR_ERROR_CACHE = new Map<string, { data: Record<string, unknown>; ts: number }>();
const FR_ERROR_CACHE_TTL_MS = 60 * 1000;
const MAX_ADDRESS_LENGTH = 200;

/** Parse numeric fields from BigQuery / JSON (commas, stray chars). */
function frParseNumericLoose(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const raw = String(value).trim();
  if (!raw) return null;
  const cleaned = raw.replace(",", ".").replace(/[^\d.-]/g, "");
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

/**
 * France DVF-derived **staging** tables (and fallbacks like `property_area_fallback`) store money in
 * **centimes** (1/100 €). Use for API JSON only; do not use for row matching / sorting.
 */
function frDvfMoneyCentsToEuros(value: unknown): number | null {
  const n = frParseNumericLoose(value);
  return n == null ? null : n / 100;
}

/**
 * `streetiq_gold.property_latest_facts` uses a different fixed-point convention than raw DVF centimes:
 * `last_sale_price` and `price_per_m2` are stored in **thousandths of a euro** (raw integer N ⇒ N/1000 €).
 * Using centime scaling (/100) on this table inflates amounts by 10× (e.g. 173 886 € shown as 1 738 860 €).
 * Apply this converter exactly once when reading those columns; keep sorting/matching on raw N.
 */
function frPropertyLatestFactsMoneyToEuros(raw: unknown): number | null {
  const n = frParseNumericLoose(raw);
  return n == null ? null : n / 1000;
}

/** Sale date within last 5 years (France building-similar cohort). */
function frSaleDateWithinFiveYears(raw: unknown): boolean {
  if (raw === null || raw === undefined) return false;
  const d = raw instanceof Date ? raw : new Date(String(raw));
  const t = d.getTime();
  if (!Number.isFinite(t) || Number.isNaN(t)) return false;
  const fiveYearsMs = 5 * 365.25 * 24 * 60 * 60 * 1000;
  const now = Date.now();
  return t <= now + 86400000 && now - t <= fiveYearsMs;
}

/**
 * Remove bottom/top `fraction` of items by numeric score (e.g. 0.1 = 10%).
 * No-op if too few rows or trim would empty the list.
 */
function frTrimFractionExtremes<T>(items: T[], getNumeric: (x: T) => number, fraction: number): T[] {
  if (items.length < 3) return items;
  const decorated = items.map((item) => ({ item, v: getNumeric(item) }));
  decorated.sort((a, b) => a.v - b.v);
  const n = decorated.length;
  const dropEach = Math.floor(n * fraction);
  if (dropEach < 1) return items;
  if (n - 2 * dropEach < 1) return items;
  return decorated.slice(dropEach, n - dropEach).map((x) => x.item);
}

/** Scale sale prices in legacy `building_sales` arrays (cents → euros). */
function mapFranceBuildingSalesPricesToEuros(sales: unknown[] | undefined | null): Array<Record<string, unknown>> {
  if (!Array.isArray(sales)) return [];
  return sales.map((s) => {
    const row = s as Record<string, unknown>;
    return {
      ...row,
      price: frDvfMoneyCentsToEuros(row.price) ?? 0,
    };
  });
}

function buildCacheKey(
  city: string,
  street: string,
  houseNumber: string,
  lat?: number,
  lng?: number,
  state?: string,
  zip?: string,
  postcode?: string
): string {
  const parts = [city.trim().toLowerCase(), street.trim().toLowerCase(), houseNumber.trim()];
  if (state) parts.push(state.trim().toUpperCase());
  if (zip) parts.push(zip.trim());
  if (postcode) parts.push(postcode.trim().toUpperCase());
  const base = parts.join("|");
  if (Number.isFinite(lat) && Number.isFinite(lng)) {
    return `${base}|${lat}|${lng}`;
  }
  return base;
}

function validateInput(
  city: string,
  street: string,
  countryCode?: string,
  postcode?: string,
  opts?: { latitude?: number; longitude?: number; addressParam?: string; rawInputAddress?: string }
): { valid: boolean; error?: string } {
  const code = (countryCode ?? "").toUpperCase();
  const isUK = code === "UK" || code === "GB";
  if (isUK) {
    const pc = (postcode ?? "").trim();
    const hasStreetAndCity = !!(city.trim() && street.trim());
    if ((!pc || pc.length === 0) && !hasStreetAndCity) return { valid: false, error: "postcode or street and town is required for UK addresses" };
    if (pc.length > MAX_ADDRESS_LENGTH) return { valid: false, error: "postcode too long" };
      return { valid: true };
  }
  const isIL = code === "IL";
  if (isIL) {
    const hasAddress = !!(city.trim() || street.trim() || (postcode ?? "").trim());
    if (!hasAddress) return { valid: false, error: "address is required for Israel" };
    if (city.length > MAX_ADDRESS_LENGTH || street.length > MAX_ADDRESS_LENGTH) return { valid: false, error: "address too long" };
    return { valid: true };
  }
  const isFR = code === "FR";
  if (isFR) {
    const hasParsed = !!(city.trim() || street.trim() || (postcode ?? "").trim());
    const hasFullAddress = !!((opts?.addressParam ?? "").trim() || (opts?.rawInputAddress ?? "").trim());
    if (!hasParsed && !hasFullAddress) return { valid: false, error: "city, street, postcode, or full address required for France addresses" };
    if (city.length > MAX_ADDRESS_LENGTH || street.length > MAX_ADDRESS_LENGTH || ((postcode ?? "").trim().length || 0) > MAX_ADDRESS_LENGTH) return { valid: false, error: "address too long" };
    return { valid: true };
  }
  if (!city || typeof city !== "string" || city.trim().length === 0) {
    return { valid: false, error: "city is required" };
  }
  if (!street || typeof street !== "string" || street.trim().length === 0) {
    return { valid: false, error: "street is required" };
  }
  if (city.length > MAX_ADDRESS_LENGTH || street.length > MAX_ADDRESS_LENGTH) {
    return { valid: false, error: "address fields too long" };
  }
  return { valid: true };
}

/** Normalize French postcode for source lookup: preserve leading zeros, 5-digit string. */
function normalizePostcodeForFranceSource(postcode: string): string {
  const s = (postcode ?? "").toString().replace(/\s+/g, "").trim();
  if (!s || !/^\d+$/.test(s)) return s;
  return s.length >= 5 ? s.slice(0, 5) : s.padStart(5, "0");
}

/** Normalize French commune/city for source lookup. PARIS 4E ARRONDISSEMENT -> PARIS, etc. */
function normalizeCityForFranceSource(city: string): string {
  const c = (city ?? "")
    .trim()
    .toUpperCase()
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .replace(/[^A-Z0-9 ]/g, " ");
  if (!c.trim()) return "";
  const m = c.match(/^(PARIS|MARSEILLE|LYON|LYONS)\s*(?:\d{1,2}(?:ER|E|ÈME)?\s*(?:ARRONDISSEMENT)?)?$/i);
  if (m) return m[1].toUpperCase();
  return c.replace(/\s+/g, " ").trim();
}

/** Check if two city strings match (handles arrondissements). */
function frCityMatches(userCity: string, banCity: string): boolean {
  const u = normalizeCityForFranceSource(userCity);
  const b = normalizeCityForFranceSource(banCity);
  if (!u || !b) return true;
  return u === b || b.startsWith(u + " ") || u.startsWith(b + " ");
}

const ROUTE_TIMEOUT_MS = 6000;
const ROUTE_TIMEOUT_MS_UK = 20000;
const ROUTE_TIMEOUT_MS_FR = 15000;
const LAND_REGISTRY_TIMEOUT_MS = 18000;
const PROVIDER_TIMEOUT_MS = 2500;

function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  return Promise.race([
    promise.then((v) => {
      if (process.env.NODE_ENV === "development") console.debug(`[property-value] Provider finished: ${label}`);
      return v;
    }),
    new Promise<never>((_, reject) =>
      setTimeout(() => reject(new Error(`timeout:${label}`)), ms)
    ),
  ]);
}

function buildUKMinimalResponse(): Record<string, unknown> {
  return {
    message: "Request timeout - partial data",
    uk_no_property_record: true,
    property_result: {
      exact_value: null,
      exact_value_message: "No exact UK property record found for this address",
      value_level: "no_match" as const,
      last_transaction: { amount: 0, date: null, message: "No recorded transaction found" as const },
      street_average: null,
      street_average_message: "No street-level average found" as const,
      livability_rating: "POOR" as const,
    },
  };
}

function getGoldBigQueryClient(): BigQuery {
  const key = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY || "{}") as { project_id?: string; client_email?: string; private_key?: string };
  if (key.private_key) key.private_key = key.private_key.replace(/\\n/g, "\n");
  if (!key.project_id) throw new Error("project_id missing from GOOGLE_SERVICE_ACCOUNT_KEY JSON");
  return new BigQuery({ projectId: key.project_id, credentials: key });
}

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  let city = searchParams.get("city") ?? "";
  let street = searchParams.get("street") ?? "";
  let houseNumber = searchParams.get("houseNumber") ?? searchParams.get("house_number") ?? "";
  let state = searchParams.get("state") ?? "";
  let zip = searchParams.get("zip") ?? searchParams.get("zipCode") ?? "";
  let postcode = searchParams.get("postcode") ?? searchParams.get("postCode") ?? "";
  const addressParam = searchParams.get("address") ?? "";
  const rawInputAddress = searchParams.get("rawInputAddress") ?? "";
  const selectedFormattedAddress = searchParams.get("selectedFormattedAddress") ?? "";
  const aptNumber = searchParams.get("apt_number") ?? searchParams.get("aptNumber") ?? "";
  const countryCode = searchParams.get("countryCode") ?? searchParams.get("country") ?? "IL";
  const latParam = searchParams.get("latitude");
  const lngParam = searchParams.get("longitude");
  const latitude = latParam ? parseFloat(latParam) : undefined;
  const longitude = lngParam ? parseFloat(lngParam) : undefined;

  const addressForParse = addressParam.trim() || (rawInputAddress.trim() || "");
  if (addressForParse) {
    const codeRaw = (countryCode ?? "").toUpperCase();
    const code = codeRaw === "RE" ? "FR" : codeRaw;
    if (code === "US") {
      const parsed = parseUSAddressFromFullString(addressForParse);
      city = parsed.city || city;
      street = parsed.street || street;
      houseNumber = parsed.houseNumber || houseNumber;
      state = parsed.state || state;
      zip = parsed.zip || zip;
    } else if (code === "UK" || code === "GB") {
      if (rawInputAddress.trim() && selectedFormattedAddress.trim()) {
        const flatFromRaw = extractFlatPrefix(rawInputAddress);
        const parsedSelected = parseUKAddressFromFullString(selectedFormattedAddress);
        city = parsedSelected.city || city;
        postcode = parsedSelected.postcode || postcode;
        houseNumber = flatFromRaw || parsedSelected.houseNumber || houseNumber;
        const selTrimmed = selectedFormattedAddress.replace(/,?\s*(UK|United Kingdom)\s*$/i, "").trim();
        const postcodeRe = /\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b/i;
        const pcMatch = selTrimmed.match(postcodeRe);
        const beforePc = pcMatch ? selTrimmed.slice(0, pcMatch.index).trim() : selTrimmed;
        const selParts = beforePc.split(",").map((p) => p.trim()).filter(Boolean);
        street = (parsedSelected.houseNumber && parsedSelected.street?.trim())
          ? parsedSelected.street.trim()
          : selParts.length >= 2
            ? selParts.slice(0, -1).join(", ")
            : (parsedSelected.street || street);
      } else {
        const parsed = parseUKAddressFromFullString(addressForParse);
        street = parsed.street || street;
        city = parsed.city || city;
        postcode = parsed.postcode || postcode;
        houseNumber = parsed.houseNumber || houseNumber;
      }
    } else if (code === "IT") {
      const parsed = parseAddressFromFullString(addressForParse);
      if (parsed.city) city = city || parsed.city;
      if (parsed.street) street = street || parsed.street;
      if (parsed.houseNumber) houseNumber = houseNumber || parsed.houseNumber;
    } else if (code === "FR") {
      const parsed = parseFRAddressFromFullString(addressForParse);
      if (parsed.city) city = city || parsed.city;
      if (parsed.street) street = street || parsed.street;
      if (parsed.houseNumber) houseNumber = houseNumber || parsed.houseNumber;
      if (parsed.postcode) postcode = postcode || parsed.postcode;
    } else {
      if (!city || !street) {
        const parsed = parseAddressFromFullString(addressForParse);
        if (parsed.city) city = city || parsed.city;
        if (parsed.street) street = street || parsed.street;
        if (parsed.houseNumber) houseNumber = houseNumber || parsed.houseNumber;
      }
    }
  }

  const validation = validateInput(city.trim(), street.trim(), countryCode, ((postcode ?? "").trim() || (zip ?? "").trim()), { latitude, longitude, addressParam, rawInputAddress });
  if (!validation.valid) {
    return NextResponse.json(
      { message: validation.error, error: "INVALID_INPUT" },
      { status: 400 }
    );
  }

  const ukPostcode = (countryCode ?? "").toUpperCase() === "UK" || (countryCode ?? "").toUpperCase() === "GB"
    ? (postcode.trim() || zip.trim())
    : undefined;
  const isUK = (countryCode ?? "").toUpperCase() === "UK" || (countryCode ?? "").toUpperCase() === "GB";
  const raw = (isUK && rawInputAddress.trim()) ? `|raw:${rawInputAddress.trim()}` : "";
  const sel = (isUK && selectedFormattedAddress.trim()) ? `|sel:${selectedFormattedAddress.trim()}` : "";
  // UK: cache key includes raw+selected so same address always yields same cached response (including level)
  const cacheKey = buildCacheKey(city, street, houseNumber, latitude, longitude, state, zip, ukPostcode) + raw + sel;
  const isUS = (countryCode ?? "").toUpperCase() === "US";
  const isFR = (() => {
    const c = (countryCode ?? "").toUpperCase();
    return c === "FR" || c === "RE";
  })();
  const isIL = (countryCode ?? "").toUpperCase() === "IL";
  const usMockMode = isUS && isUSMockEnabled();

  if (isIL) {
    try {
      const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
      const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
      if (supabaseUrl && supabaseKey) {
        const supabase = createClient(supabaseUrl, supabaseKey, {
          auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false },
        });
        let fullAddress = addressParam.trim();
        if (!fullAddress) {
          fullAddress = [street.trim(), houseNumber.trim(), city.trim()].filter(Boolean).join(", ").trim();
        }
        if (fullAddress) {
          const norm = fullAddress.trim().toLowerCase().replace(/\s+/g, " ");
          const { data: rows } = await supabase
            .from("properties_israel")
            .select("address, current_value, last_sale_info, street_avg_price, neighborhood_quality")
            .ilike("address", "%" + norm + "%")
            .limit(5);
          const arr = rows != null ? rows : [];
          const match = arr[0];
          if (match) {
            const currentValue = match.current_value != null ? Number(match.current_value) : null;
            const streetAvg = match.street_avg_price != null ? Number(match.street_avg_price) : null;
            let lastAmount = 0;
            let lastDate: string | null = null;
            if (match.last_sale_info) {
              const parts = String(match.last_sale_info).split(" · ");
              if (parts[0]) lastAmount = parseFloat(String(parts[0]).replace(/[^\d.-]/g, "")) || 0;
              if (parts[1]) lastDate = parts[1].trim();
            }
            let livability = "FAIR";
            if (match.neighborhood_quality) {
              const nq = String(match.neighborhood_quality).toLowerCase().trim();
              const map: Record<string, string> = { poor: "POOR", fair: "FAIR", good: "GOOD", "very good": "VERY GOOD", excellent: "EXCELLENT" };
              if (map[nq]) livability = map[nq];
            }
            return NextResponse.json({
              address: { city: city.trim(), street: street.trim(), house_number: houseNumber.trim() },
              data_source: "properties_israel",
              property_result: {
                exact_value: currentValue,
                exact_value_message: currentValue == null ? "No data for this address" : null,
                value_level: "property-level",
                last_transaction: { amount: lastAmount, date: lastDate, message: lastAmount > 0 ? undefined : "No recorded transaction" },
                street_average: streetAvg,
                street_average_message: streetAvg == null ? "No street average" : null,
                livability_rating: livability,
              },
            });
          }
        }
      }
    } catch {
      // Fall through
    }
  }

  if (isFR) {
    // Minimal France gold-table path (exact -> area fallback -> no data).
    const rawInput = (addressParam || rawInputAddress || "").trim();
    const countryDetected = (countryCode ?? "").toUpperCase();
    console.log("[FR_ENTRY] raw_input=" + (rawInput || "(empty)"));
    console.log("[FR_ENTRY] address_param=" + (addressParam || "(empty)"));
    try {
      console.log("[FR_STEP] entered");
      const frStartTs = Date.now();

      const fullRawAddress = (rawInputAddress || addressParam || rawInput || [houseNumber, street, city, postcode || zip].filter(Boolean).join(", ")).trim();
      console.log("[FR_ENTRY] fullRawAddress=" + (fullRawAddress || "(empty)"));
      console.log("[FR_PARSE] raw_input=" + (fullRawAddress || "(empty)"));
      console.log("[FR_PARSE] parser_started=" + (fullRawAddress ? "true" : "false"));

      const rawPostcodeMatch = fullRawAddress.match(/\b(\d{5})\b/);
      const frRawPostcodeToken = rawPostcodeMatch ? rawPostcodeMatch[1] : null;

      let cityParsed = city.trim();
      let streetParsed = street.trim();
      let houseNumberParsed = houseNumber.trim();
      let postcodeParsed = (postcode || zip || "").trim();

      if (fullRawAddress) {
        const parsed = parseFRAddressFromFullString(fullRawAddress);
        if (parsed.houseNumber) houseNumberParsed = houseNumberParsed || parsed.houseNumber;
        if (parsed.street) streetParsed = streetParsed || parsed.street;
        if (parsed.city) cityParsed = cityParsed || parsed.city;
        if (parsed.postcode) postcodeParsed = postcodeParsed || parsed.postcode;
        if (frRawPostcodeToken) postcodeParsed = frRawPostcodeToken;
        if (!parsed.city && !parsed.street && !parsed.postcode && !parsed.houseNumber) {
          const fallback = parseAddressFromFullString(fullRawAddress);
          if (fallback.houseNumber) houseNumberParsed = houseNumberParsed || fallback.houseNumber;
          if (fallback.street) streetParsed = streetParsed || fallback.street;
          if (fallback.city) cityParsed = cityParsed || fallback.city;
        }
      }

      console.log("[FR_PARSE] parsed_house_number=" + (houseNumberParsed || "(empty)"));
      console.log("[FR_PARSE] parsed_street=" + (streetParsed || "(empty)"));
      console.log("[FR_PARSE] parsed_postcode=" + (postcodeParsed || "(empty)"));
      console.log("[FR_PARSE] parsed_city=" + (cityParsed || "(empty)"));

      const requestedLotNorm = normalizeLot(aptNumber) || null;
      const normalizedRequestedLot = requestedLotNorm ? (requestedLotNorm.replace(/^0+/, "") || requestedLotNorm) : null;
      // Strict: any non-empty apt param skips lot prompt (avoids normalize edge cases vs raw UI input).
      const aptNumberRawTrimmed = (aptNumber ?? "").trim();
      const submittedLotPresent =
        aptNumberRawTrimmed.length > 0 ||
        (normalizedRequestedLot != null && String(normalizedRequestedLot).length > 0);
      console.log("[FR_STEP] lot_received");
      console.log("[FR_LOT_API] apt_number raw", {
        apt_number: searchParams.get("apt_number"),
        aptNumber: searchParams.get("aptNumber"),
      });
      console.log("[FR_LOT_API] normalizedRequestedLot", normalizedRequestedLot);
      console.log("[FR_DEBUG] submitted_lot_and_normalized_lot", {
        submittedLot: aptNumber?.trim() || null,
        requestedLotNorm,
        normalizedLot: normalizedRequestedLot,
      });
      console.log("[FR_GOLD] request_start", {
        city: cityParsed,
        street: streetParsed,
        houseNumber: houseNumberParsed,
        postcode: postcodeParsed,
        aptNumber: (aptNumber ?? "").trim(),
      });
      console.log("[FR_BAN] entered_france_flow", {
        hasBigQueryKey: !!process.env.GOOGLE_SERVICE_ACCOUNT_KEY,
        // projectId is printed inside [FR_INIT] too, but log here to correlate request->query quickly.
        // (We keep it best-effort: it may be undefined if init throws.)
      });
      console.log("[FR_INIT] creating BigQuery client");
      let bq: BigQuery;
      try {
        const key = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY || "{}");
        console.log("[FR_INIT] projectId from key:", (key as any)?.project_id);
        bq = new BigQuery({
          projectId: (key as any)?.project_id,
          credentials: key,
        });
        console.log("[FR_INIT] BigQuery client created");
      } catch (err) {
        console.error("[FR_ERROR] BigQuery init failed", err);
        return new Response(JSON.stringify({ success: false, error: "BigQuery init failed" }), { status: 200 });
      }
      const country = "FR";
      let cityNorm = cityParsed;
      let streetNorm = streetParsed;
      let postcodeNorm = postcodeParsed;
      let houseNumberNorm = houseNumberParsed;
      // Use the normalized lot token for exact matching (avoid leading-zero mismatches).
      const unitNumberNorm = normalizedRequestedLot ?? "";
      const propertyType = (searchParams.get("property_type") ?? searchParams.get("propertyType") ?? "").trim() || null;
      const inputSurfaceRaw = searchParams.get("surface_m2") ?? searchParams.get("surfaceM2");
      const inputSurfaceM2 = inputSurfaceRaw ? Number(inputSurfaceRaw) : NaN;
      const validInputSurfaceM2 = Number.isFinite(inputSurfaceM2) && inputSurfaceM2 > 0 ? inputSurfaceM2 : null;
      const queryWithTimeout = async <T,>(opts: Parameters<typeof bq.query>[0], label: string): Promise<T> => {
        const timeoutMs = 10000;
        try {
          return (await Promise.race([
            bq.query(opts),
            new Promise<never>((_, reject) => setTimeout(() => reject(new Error(`timeout:${label}`)), timeoutMs)),
          ])) as T;
        } catch (err) {
          const sql = typeof (opts as any)?.query === "string" ? (opts as any).query : "";
          const tableMatch = sql.match(/FROM\s+`([^`]+)`/i);
          const tableName = tableMatch?.[1] ?? null;
          const saleDateColumnUsed = sql.includes("latest_sale_date")
            ? "latest_sale_date"
            : sql.includes("newest_sale_date")
              ? "newest_sale_date"
              : null;
          console.error("[FR_SQL] failing_query_name", label);
          console.error("[FR_SQL] failing_table", tableName);
          console.error("[FR_SQL] sale_date_column_used", saleDateColumnUsed);
          console.error("[FR_SQL] failing_sql_text", sql);
          throw err;
        }
      };

      // Temporary runtime diagnostics for France (debug-only; do not treat as stable API contract).
      const frParserStarted = !!fullRawAddress;
      const frHadRawInput = !!(addressParam || rawInputAddress || "").trim();
      const frRuntimeDebug: Record<string, unknown> = {
        // Request/parse diagnostics (visible in UI debug panel)
        fr_raw_input: rawInput || null,
        fr_address_param: addressParam || null,
        fr_full_raw_address: fullRawAddress || null,
        fr_parser_started: frParserStarted,
        fr_parsed_house_number: houseNumberParsed || null,
        fr_parsed_street: streetParsed || null,
        fr_parsed_postcode: postcodeParsed || null,
        fr_parsed_city: cityParsed || null,
        fr_ban_query_mode: null as string | null,
        fr_ban_attempt_count: null as number | null,
        fr_raw_postcode_token: frRawPostcodeToken,
        fr_postcode_mismatch_rejections: null as number | null,
        fr_typed_street_normalized: null as string | null,
        fr_typed_street_type: null as string | null,
        fr_typed_street_core: null as string | null,
        fr_ban_candidate_count: null as number | null,
        fr_ban_selected_street_score: null as number | null,
        fr_ban_selected_reason: null as string | null,
        fr_ban_selected_street_type: null as string | null,
        fr_ban_selected_street_core: null as string | null,
        fr_ban_selected_penalties: null as string | null,
        fr_ban_similarity_threshold_passed: null as boolean | null,
        fr_ban_top_candidates_summary: null as string | null,
        fr_source_lookup_postcode: null as string | null,
        fr_source_lookup_city: null as string | null,
        fr_source_lookup_street_raw: null as string | null,
        fr_source_lookup_street_core: null as string | null,
        fr_source_lookup_street_type: null as string | null,
        fr_source_lookup_street: null as string | null,
        fr_source_lookup_house_number: null as string | null,
        fr_source_lookup_exact_count: null as number | null,
        fr_source_lookup_street_count: null as number | null,
        fr_source_lookup_commune_count: null as number | null,
        fr_source_lookup_failed_reason: null as string | null,
        fr_cache_hit: false,
        fr_cache_bypass_reason: null,
        fr_failed_stage: !frParserStarted && frHadRawInput ? "parse_entry" : null,
        // BAN integration diagnostics (France-only)
        ban_match_found: null,
        ban_rows_count: null,
        ban_match_quality: null,
        ban_query_error: null,
        ban_lat: null,
        ban_lon: null,
        ban_city: null,
        ban_postcode: null,
        ban_street: null,
        ban_house_number: null,
        raw_apt_number_param: null,
        raw_aptNumber_param: null,
        request_url_seen_by_api: null,
        submitted_lot: null,
        detect_class: null,
        fr_detect_signals_summary: null as string | null,
        fr_detect_used_lot: null as boolean | null,
        fr_detect_override_reason: null as string | null,
        fr_detect_multi_unit_source: null as "ban" | "source" | "building_intel" | "none" | null,
        fr_detect_ban_strength_used: null as boolean | null,
        fr_should_prompt_lot: null as boolean | null,
        fr_label_safety_override: null as boolean | null,
        fr_confidence_adjustment_reason: null as string | null,
        exact_rows_count: null,
        exact_usable_rows_count: null,
        building_rows_count: null,
        building_usable_rows_count: null,
        street_rows_count: null,
        street_usable_rows_count: null,
        commune_rows_count: null,
        commune_usable_rows_count: null,
        winning_step: null,
        winning_source_label: null,
        has_surface_for_estimate: null,
        chosen_surface_value: null,
        no_data_reason: null,
        submitted_lot_present: null,
        exact_match_reason: null,
        exact_reject_reason: null,
        exact_lot_column_used: null,
        winning_median_price_per_m2: null,
        /** Set when `property_latest_facts` money converter runs (1000 = thousandths of €). */
        property_latest_facts_money_divisor: null,
        exact_level: null as "EXACT_UNIT" | "EXACT_ADDRESS" | "NONE" | null,
        exact_unit_row_count: null as number | null,
        exact_address_row_count: null as number | null,
        building_similar_unit_candidates_count: null as number | null,
        building_similar_unit_after_filters_count: null as number | null,
        exact_house_row_count: null as number | null,
        exact_house_usable_count: null as number | null,
        exact_house_reject_reason: null as string | null,
        building_similar_unit_reject_reason: null as string | null,
      };

      frRuntimeDebug.raw_apt_number_param = searchParams.get("apt_number");
      frRuntimeDebug.raw_aptNumber_param = searchParams.get("aptNumber");
      frRuntimeDebug.request_url_seen_by_api = request.url;
      console.log("[FR_LOT_API] incoming_request", {
        requestUrl: request.url,
        queryString: searchParams.toString(),
        apt_number: searchParams.get("apt_number"),
        aptNumber: searchParams.get("aptNumber"),
      });
      // Normalize the received lot/apartment number for stable matching + runtime debug.
      // (normalizedRequestedLot is computed above from `aptNumber`.)
      frRuntimeDebug.submitted_lot = normalizedRequestedLot ?? null;
      console.log("[FR_DEBUG] lot_value_received_in_api", {
        aptNumberRaw: aptNumber?.trim() || null,
        normalized_submitted_lot: normalizedRequestedLot,
      });

      const frReturn = (payload: Record<string, unknown>, tag: string, status?: number) => {
        frRuntimeDebug.submitted_lot_present = submittedLotPresent;

        if (tag === "prompt_lot_first" && submittedLotPresent) {
          console.error(
            "[FR_RETURN] INVARIANT_VIOLATION prompt_lot_first with submitted_lot_present=true — check client request order"
          );
        }
        if (tag === "valuation_response" && frRuntimeDebug.winning_step == null) {
          console.error(
            "[FR_RETURN] INVARIANT_VIOLATION valuation_response without winning_step — check ladder exit path"
          );
        }

        const pr = payload.property_result as Record<string, unknown> | undefined;
        const frObj = payload.fr as Record<string, unknown> | undefined;
        const prop = frObj?.property as Record<string, unknown> | null | undefined;
        const bs = frObj?.buildingStats as Record<string, unknown> | undefined;
        const exactVal = pr?.exact_value;
        const txVal = prop?.transactionValue;
        const lt = pr?.last_transaction as Record<string, unknown> | undefined;
        const lastSaleAmt = lt?.amount;
        const last_sale_price =
          typeof lastSaleAmt === "number" && Number.isFinite(lastSaleAmt) && lastSaleAmt > 0 ? lastSaleAmt : null;
        const last_sale_date =
          lt?.date != null && String(lt.date).trim().length > 0 ? String(lt.date) : null;

        const ppmProp = prop?.pricePerSqm;
        const ppmBs = bs?.avgPricePerSqm;
        const streetAvgPr = pr?.street_average;
        const ppmFromStreetAvg =
          typeof streetAvgPr === "number" && Number.isFinite(streetAvgPr) && streetAvgPr > 0 ? streetAvgPr : null;
        const wppDbg = frRuntimeDebug.winning_median_price_per_m2;
        const ppmFromWinningDebug =
          typeof wppDbg === "number" && Number.isFinite(wppDbg) && wppDbg > 0 ? wppDbg : null;
        let price_per_m2 =
          typeof ppmProp === "number" && Number.isFinite(ppmProp) && ppmProp > 0
            ? ppmProp
            : typeof ppmBs === "number" && Number.isFinite(ppmBs) && ppmBs > 0
              ? ppmBs
              : ppmFromStreetAvg ?? ppmFromWinningDebug;

        let estimated_value =
          typeof exactVal === "number" && Number.isFinite(exactVal) && exactVal > 0
            ? exactVal
            : typeof txVal === "number" && Number.isFinite(txVal) && txVal > 0
              ? txVal
              : null;

        let winning_source_label = String(
          frRuntimeDebug.winning_source_label ??
            pr?.street_average_message ??
            pr?.exact_value_message ??
            ""
        ).trim();
        let confidence = String((frObj?.confidence as string) ?? "low");

        const banStrengthPassed = frRuntimeDebug.fr_ban_similarity_threshold_passed === true;
        const winningStep = String(frRuntimeDebug.winning_step ?? "");
        const isBuildingLevelStep =
          winningStep === "building_level" ||
          winningStep === "building_similar_unit" ||
          winningStep === "building_profile";
        const labelSaysInBuilding =
          /in this building|similar properties in this building/i.test(winning_source_label);
        const needsLabelSafetyOverride =
          !banStrengthPassed && isBuildingLevelStep && labelSaysInBuilding;
        const needsConfidenceDowngrade =
          !banStrengthPassed && isBuildingLevelStep && /high|very_high/i.test(confidence);

        const adjustmentReasons: string[] = [];
        frRuntimeDebug.fr_label_safety_override = needsLabelSafetyOverride;
        if (needsLabelSafetyOverride) {
          winning_source_label = "Based on similar properties in the area";
          adjustmentReasons.push("weak_ban_label_safe");
        }
        if (needsConfidenceDowngrade) {
          confidence = /very_high/i.test(confidence) ? "medium" : "medium";
          adjustmentReasons.push("weak_ban_confidence_downgraded");
        }
        frRuntimeDebug.fr_confidence_adjustment_reason =
          adjustmentReasons.length > 0 ? adjustmentReasons.join(";") : null;

        let outPayload: Record<string, unknown> = { ...payload };
        if (needsLabelSafetyOverride || needsConfidenceDowngrade) {
          if (outPayload.fr && typeof outPayload.fr === "object") {
            const frUpdate: Record<string, unknown> = { confidence };
            if (needsLabelSafetyOverride) {
              frUpdate.matchExplanation = "Based on similar properties in the area.";
            }
            outPayload.fr = { ...(outPayload.fr as Record<string, unknown>), ...frUpdate };
          }
          if (needsLabelSafetyOverride && outPayload.property_result && typeof outPayload.property_result === "object") {
            const pr = outPayload.property_result as Record<string, unknown>;
            outPayload.property_result = { ...pr, street_average_message: "Based on similar properties in the area." };
          }
        }
        const hasWinningStep = frRuntimeDebug.winning_step != null && String(frRuntimeDebug.winning_step) !== "";

        if (tag === "valuation_response") {
          // No surface: ladder still provides median €/m² on property / street_average — ensure display path sees it.
          if (price_per_m2 == null && ppmFromStreetAvg != null) {
            price_per_m2 = ppmFromStreetAvg;
          }
          if (price_per_m2 == null && ppmFromWinningDebug != null) {
            price_per_m2 = ppmFromWinningDebug;
          }
          if (estimated_value == null && typeof exactVal === "number" && Number.isFinite(exactVal) && exactVal > 0) {
            estimated_value = exactVal;
          }

          if (hasWinningStep && !winning_source_label) {
            winning_source_label = "France valuation";
          }

          const evPos =
            estimated_value != null &&
            typeof estimated_value === "number" &&
            Number.isFinite(estimated_value) &&
            estimated_value > 0;
          const ppmPos =
            price_per_m2 != null &&
            typeof price_per_m2 === "number" &&
            Number.isFinite(price_per_m2) &&
            price_per_m2 > 0;

          const noSurfaceForDisplay = !Boolean(frRuntimeDebug.has_surface_for_estimate);
          let display_value: number | null = evPos ? estimated_value : ppmPos ? price_per_m2 : null;
          let display_value_type: "estimated_total" | "price_per_m2" | null = evPos
            ? "estimated_total"
            : ppmPos
              ? "price_per_m2"
              : null;
          // No surface: UI must still show median €/m² (not blank).
          if (noSurfaceForDisplay && hasWinningStep && ppmPos && !evPos) {
            display_value = price_per_m2;
            display_value_type = "price_per_m2";
          }
          let has_display_value = Boolean(evPos || ppmPos);
          if (noSurfaceForDisplay && hasWinningStep && ppmPos) {
            has_display_value = true;
          }

          if (hasWinningStep && !has_display_value) {
            console.error("[FR_RETURN] winning_step_set_but_no_price_or_estimate", {
              winning_step: frRuntimeDebug.winning_step,
              winning_source_label,
            });
          }

          outPayload.fr_valuation_display = {
            winning_source_label,
            source_label: winning_source_label,
            winning_step: frRuntimeDebug.winning_step,
            confidence,
            estimated_value: evPos ? estimated_value : null,
            price_per_m2: ppmPos ? price_per_m2 : null,
            display_value,
            display_value_type,
            last_sale_price,
            last_sale_date,
            has_display_value,
          };
          if (String(frRuntimeDebug.winning_step) === "exact_unit" || String(frRuntimeDebug.winning_step) === "exact_address") {
            console.log(
              "[FR_PRICE] envelope_fr_valuation_last_sale_price=" +
                String(last_sale_price) +
                " envelope_property_last_transaction_amount=" +
                String(lastSaleAmt)
            );
          }
          // Stable top-level fields for UI/clients (France valuation only).
          outPayload.winning_source_label = winning_source_label;
          outPayload.confidence = confidence;
          outPayload.estimated_value = evPos ? estimated_value : null;
          outPayload.price_per_m2 = ppmPos ? price_per_m2 : null;
          outPayload.display_value = display_value;
          outPayload.display_value_type = display_value_type;
        }

        const estimated_value_present =
          estimated_value != null &&
          typeof estimated_value === "number" &&
          Number.isFinite(estimated_value) &&
          estimated_value > 0;
        const price_per_m2_present =
          price_per_m2 != null &&
          typeof price_per_m2 === "number" &&
          Number.isFinite(price_per_m2) &&
          price_per_m2 > 0;
        const has_display_value = estimated_value_present || price_per_m2_present;

        const has_winner = tag === "valuation_response";
        const winning_step_str =
          frRuntimeDebug.winning_step == null ? "" : String(frRuntimeDebug.winning_step);

        console.log("[FR_LOT_API] response_tag", tag);
        console.log("[FR_RETURN] submitted_lot_present=" + String(submittedLotPresent));
        console.log("[FR_RETURN] has_winner=" + String(has_winner));
        console.log("[FR_RETURN] winning_step=" + winning_step_str);
        console.log("[FR_RETURN] returning_tag=" + tag);
        console.log(
          "[FR_RETURN] has_surface_for_estimate=" + String(Boolean(frRuntimeDebug.has_surface_for_estimate))
        );
        console.log("[FR_RETURN] estimated_value_present=" + String(estimated_value_present));
        console.log("[FR_RETURN] price_per_m2_present=" + String(price_per_m2_present));
        console.log("[FR_RETURN] has_display_value=" + String(has_display_value));
        console.log("[FR_STEP] returning_success");
        console.log("[FR_GOLD] return", { tag, status: status ?? 200, durationMs: Date.now() - frStartTs });
        // Not BAN-specific: every France exit path uses frReturn (valuation, lot prompt, no_data, etc.).
        console.log("[FR_RETURN] response", {
          tag,
          ban_match_found: frRuntimeDebug.ban_match_found,
          ban_rows_count: frRuntimeDebug.ban_rows_count,
        });
        return NextResponse.json(
          { ...outPayload, fr_runtime_debug: frRuntimeDebug },
          status ? { status } : undefined
        );
      };
      const normalizeStreetForDetection = (s: string): string => {
        // Keep this intentionally strict: only strip voie-type prefixes the UI might include.
        // Example: "Rue Adolphe Pajeaud" -> "ADOLPHE PAJEAUD"
        const unified = s
          .replace(/[\u2019\u2018\u02BC\u00B4\u0060]/g, "'")
          .replace(/[’‘]/g, "'")
          .replace(/\s+/g, " ")
          .trim()
          .toUpperCase()
          .normalize("NFD")
          .replace(/\p{M}/gu, "")
          .replace(/[^A-Z0-9 ]+/g, " ")
          .replace(/\s+/g, " ")
          .trim();

        const prefixes = ["RUE", "AVENUE", "AV", "BD", "BOULEVARD", "CHEMIN", "CHE", "ROUTE", "IMPASSE", "IMP", "ALLEE", "ALL", "PLACE", "PL", "SQUARE", "SQ", "SENTE", "COURS", "PROMENADE", "PROM"];
        const prefixRegex = new RegExp(`^(?:${prefixes.join("|")})\\.?\\s+`, "i");
        const cleaned = unified.replace(prefixRegex, "").replace(/\s+/g, " ").trim();
        return cleaned;
      };

      const STREET_TYPES = ["RUE", "AVENUE", "AV", "BD", "BOULEVARD", "CHEMIN", "CHE", "ROUTE", "IMPASSE", "IMP", "ALLEE", "ALL", "PLACE", "PL", "SQUARE", "SQ", "SENTE", "COURS", "PROMENADE", "PROM"];
      const STOPWORDS = ["DE", "DU", "DES", "DE LA", "DE L", "LA", "LE", "LES"];
      const parseStreetForComparison = (raw: string): { type: string; core: string; coreTokens: string[] } => {
        const unified = (raw || "")
          .replace(/[\u2019\u2018\u02BC\u00B4\u0060]/g, "'")
          .replace(/[''']/g, "'")
          .replace(/\s+/g, " ")
          .trim()
          .toUpperCase()
          .normalize("NFD")
          .replace(/\p{M}/gu, "")
          .replace(/[^A-Z0-9 ]+/g, " ")
          .replace(/\s+/g, " ")
          .trim();
        let type = "";
        let rest = unified;
        const typeRegex = new RegExp(`^(${STREET_TYPES.join("|")})\\.?\\s+`, "i");
        const tm = rest.match(typeRegex);
        if (tm) {
          type = tm[1].toUpperCase().replace(/^CHE$/, "CHEMIN").replace(/^IMP$/, "IMPASSE").replace(/^AV$/, "AVENUE").replace(/^BD$/, "BOULEVARD").replace(/^ALL$/, "ALLEE").replace(/^PL$/, "PLACE").replace(/^SQ$/, "SQUARE").replace(/^PROM$/, "PROMENADE");
          rest = rest.slice(tm[0].length).trim();
        }
        for (const sw of STOPWORDS) {
          rest = rest.replace(new RegExp(`\\b${sw}\\b`, "gi"), " ");
        }
        rest = rest.replace(/\s+/g, " ").trim();
        const coreTokens = rest.split(/\s+/).filter(Boolean);
        return { type, core: rest, coreTokens };
      };
      const scoreStreetSimilarity = (
        typedStreetRaw: string,
        candidateStreetRaw: string
      ): { score: number; penalties: string[] } => {
        const typed = parseStreetForComparison(typedStreetRaw || "");
        const cand = parseStreetForComparison(candidateStreetRaw || "");
        const penalties: string[] = [];
        if (typed.coreTokens.length === 0) return { score: 0, penalties: ["no_typed_core"] };
        let baseScore = 0;
        const matchCount = typed.coreTokens.filter((t) => cand.coreTokens.includes(t)).length;
        const extraCount = cand.coreTokens.filter((t) => !typed.coreTokens.includes(t)).length;
        baseScore = matchCount / typed.coreTokens.length;
        if (extraCount > 0) {
          const extraPenalty = extraCount * 0.35;
          baseScore = Math.max(0, baseScore - extraPenalty);
          penalties.push("extra_tokens:" + extraCount);
        }
        let typePenalty = 0;
        if (typed.type && cand.type) {
          if (typed.type !== cand.type) {
            typePenalty = 0.4;
            penalties.push("type_mismatch:" + typed.type + " vs " + cand.type);
          }
        }
        const score = Math.max(0, Math.round((baseScore - typePenalty) * 100) / 100);
        return { score, penalties };
      };

      let streetNormalizedDet = normalizeStreetForDetection(streetNorm);

      // BAN resolution first (FR only) so downstream detection + valuation use deterministic inputs.
      // If BAN lookup fails or returns no row, we fall back to the original parsed values.
      let ban_city: string | null = null;
      let ban_postcode: string | null = null;
      let ban_street: string | null = null;
      let ban_house_number: string | null = null;
      let ban_lat: number | null = null;
      let ban_lon: number | null = null;

      const normalizeForBanText = (s: string): string => {
        const unified = s
          .replace(/[\u2019\u2018\u02BC\u00B4\u0060]/g, "'")
          .replace(/[’‘]/g, "'")
          .replace(/\s+/g, " ")
          .trim()
          .toUpperCase()
          .normalize("NFD")
          .replace(/\p{M}/gu, "")
          .replace(/[^A-Z0-9 ]+/g, " ")
          .replace(/\s+/g, " ")
          .trim();
        return unified;
      };

      const normalizeHouseNumberForBan = (hn: string): string => {
        // BAN house numbers can contain letter suffixes; keep digits + A-Z only.
        return hn
          .replace(/[\u2019\u2018\u02BC\u00B4\u0060]/g, "'")
          .replace(/[’‘]/g, "'")
          .trim()
          .toUpperCase()
          .replace(/\s+/g, "")
          .replace(/[^0-9A-Z]/g, "");
      };

      const banInputPostcode = postcodeNorm.replace(/\s+/g, "").trim();
      const banInputCity = normalizeForBanText(cityNorm);
      const banInputStreetNorm = streetNormalizedDet || normalizeForBanText(streetNorm);
      const banInputHouseNumber = normalizeHouseNumberForBan(houseNumberNorm);

      console.log("[FR_BAN] raw_city=" + (banInputCity || "(empty)"));
      console.log("[FR_BAN] raw_postcode=" + (banInputPostcode || "(empty)"));

      const banQueryAttempts: Array<{ postcode: string; city_norm: string; street_norm: string; house_number_norm: string; query_mode: string }> = [];
      let streetForBan = banInputStreetNorm || banInputCity || "";

      if (!streetForBan && !banInputPostcode && fullRawAddress) {
        const rawPc = fullRawAddress.match(/\b(\d{4,5})\b/)?.[1];
        const beforePc = rawPc ? fullRawAddress.slice(0, fullRawAddress.indexOf(rawPc)).replace(/,+\s*$/, "").trim() : fullRawAddress;
        const rawStreet = beforePc.replace(/^\d+[A-Za-z]?\s*/, "").trim();
        const rawHn = fullRawAddress.match(/^(\d+[A-Za-z]?)\s/)?.[1] || "";
        if ((rawPc || rawStreet) && rawStreet.length >= 2) {
          const rawStreetNorm = normalizeForBanText(rawStreet);
          const rawPrefixRegex = /^(?:RUE|AVENUE|AV|BD|BOULEVARD|CHEMIN|CHE|ROUTE|IMPASSE|IMP|ALLEE|ALL|PLACE|PL|SQUARE|SQ|SENTE|COURS|PROMENADE|PROM)\.?\s+/i;
          const rawStreetCore = rawStreetNorm.replace(rawPrefixRegex, "").replace(/\s+/g, " ").trim() || rawStreetNorm;
          banQueryAttempts.push({
            postcode: rawPc || "",
            city_norm: "",
            street_norm: rawStreetCore || rawStreetNorm,
            house_number_norm: rawHn,
            query_mode: "raw",
          });
        }
      }
      if (streetForBan || banInputPostcode) {
        if (banInputPostcode && banInputCity && streetForBan) {
          banQueryAttempts.push({ postcode: banInputPostcode, city_norm: banInputCity, street_norm: streetForBan, house_number_norm: banInputHouseNumber || "", query_mode: "parsed_full" });
        }
        if (banInputPostcode && streetForBan && !banQueryAttempts.some((a) => a.query_mode === "parsed_postcode_street")) {
          banQueryAttempts.push({ postcode: banInputPostcode, city_norm: "", street_norm: streetForBan, house_number_norm: banInputHouseNumber || "", query_mode: "parsed_postcode_street" });
        }
        if (banInputCity && streetForBan && !banInputPostcode) {
          banQueryAttempts.push({ postcode: "", city_norm: banInputCity, street_norm: streetForBan, house_number_norm: banInputHouseNumber || "", query_mode: "parsed_city_street" });
        }
        if (streetForBan && !banQueryAttempts.some((a) => a.query_mode === "street_only")) {
          banQueryAttempts.push({ postcode: "", city_norm: "", street_norm: streetForBan, house_number_norm: banInputHouseNumber || "", query_mode: "street_only" });
        }
      }

      if (fullRawAddress && banQueryAttempts.length === 0) {
        const rawPc = frRawPostcodeToken || fullRawAddress.match(/\b(\d{5})\b/)?.[1] || fullRawAddress.match(/\b(\d{4,5})\b/)?.[1];
        const beforePc = rawPc ? fullRawAddress.slice(0, fullRawAddress.indexOf(rawPc)).replace(/,+\s*$/, "").trim() : fullRawAddress;
        const rawStreet = beforePc.replace(/^\d+[A-Za-z]?\s*/, "").trim();
        const rawHn = fullRawAddress.match(/^(\d+[A-Za-z]?)\s/)?.[1] || "";
        if (rawStreet.length >= 2 || rawPc) {
          const rawStreetNorm = normalizeForBanText(rawStreet || " ");
          const rawPrefixRegex = /^(?:RUE|AVENUE|AV|BD|BOULEVARD|CHEMIN|CHE|ROUTE|IMPASSE|IMP|ALLEE|ALL|PLACE|PL|SQUARE|SQ|SENTE|COURS|PROMENADE|PROM)\.?\s+/i;
          const rawStreetCore = rawStreetNorm.replace(rawPrefixRegex, "").replace(/\s+/g, " ").trim() || rawStreetNorm || rawStreet;
          banQueryAttempts.push({
            postcode: rawPc || "",
            city_norm: "",
            street_norm: rawStreetCore || rawStreetNorm || rawStreet,
            house_number_norm: rawHn,
            query_mode: "raw",
          });
          console.log("[FR_BAN] raw_attempt_added=true");
        }
      }
      const rawAttemptAdded = banQueryAttempts.some((a) => a.query_mode === "raw");
      if (!rawAttemptAdded && fullRawAddress && banQueryAttempts.length === 0) {
        console.log("[FR_BAN] raw_attempt_added=false");
      }
      console.log("[FR_BAN] ban_attempt_count=" + banQueryAttempts.length);
      frRuntimeDebug.fr_ban_attempt_count = banQueryAttempts.length;
      frRuntimeDebug.fr_ban_query_mode = banQueryAttempts.length > 0 ? banQueryAttempts.map((a) => a.query_mode).join(",") : "(none)";
      console.log("[FR_BAN] raw_query_attempted=" + (banInputPostcode || banInputStreetNorm || banInputCity ? "true" : "false"));
      console.log("[FR_BAN] query_input=" + (fullRawAddress || "(empty)"));

      try {
        console.log("[FR_BAN] before_ban_query", {
          banInputPostcode,
          banInputCity,
          banInputStreetNorm,
          banInputHouseNumber,
        });

        const banLookupQuery = `
          SELECT
            *
          FROM \`streetiq-bigquery.streetiq_gold.france_ban_normalized\`
          WHERE
            (@postcode = "" OR TRIM(CAST(postcode AS STRING)) = TRIM(CAST(@postcode AS STRING)))
            AND (
              street_norm LIKE CONCAT('%', @street_norm, '%')
              OR @street_norm LIKE CONCAT('%', street_norm, '%')
            )
            AND (@city_norm = "" OR LOWER(TRIM(city)) = LOWER(TRIM(@city_norm)))
          ORDER BY
            (TRIM(CAST(postcode AS STRING)) = TRIM(CAST(@postcode AS STRING))) DESC,
            (LOWER(TRIM(city)) = LOWER(TRIM(@city_norm))) DESC,
            (street_norm = @street_norm) DESC,
            (TRIM(CAST(house_number AS STRING)) = TRIM(CAST(@house_number_norm AS STRING))) DESC
          LIMIT 10
        `;

        let banRows: Array<Record<string, unknown>> = [];
        for (let i = 0; i < banQueryAttempts.length; i++) {
          const attempt = banQueryAttempts[i];
          const banParams = {
            postcode: attempt.postcode || "",
            city_norm: attempt.city_norm || "",
            street_norm: attempt.street_norm || "",
            house_number_norm: attempt.house_number_norm || "",
          };
          if (!attempt.street_norm && !attempt.postcode) continue;
          const queryInputComposite = [attempt.postcode, attempt.city_norm, attempt.street_norm].filter(Boolean).join(" ");
          console.log("[FR_BAN] query_mode=" + attempt.query_mode);
          console.log("[FR_BAN] query_input=" + (queryInputComposite || "(empty)"));
          console.log("[FR_PARAMS]", { query: "ban_normalized_lookup_query", attempt: i + 1, ...banParams });
          const [rows] = await queryWithTimeout<[Array<Record<string, unknown>>]>(
            { query: banLookupQuery, params: banParams },
            "ban_normalized_lookup_query"
          );
          const rawRows = (rows ?? []) as Array<Record<string, unknown>>;
          const pickStr = (r: Record<string, unknown>, keys: string[]): string => {
            for (const k of keys) {
              const v = r?.[k];
              if (typeof v === "string" && v.trim()) return v.trim();
            }
            return "";
          };
          let postcodeMismatchCount = 0;
          const filtered = rawRows.filter((r) => {
            const banPc = pickStr(r, ["postcode", "postal_code", "code_postal"]);
            const banCityVal = pickStr(r, ["city_norm", "normalized_city", "city"]);
            if (banInputPostcode && banPc && banInputPostcode.trim() !== banPc.trim()) {
              postcodeMismatchCount++;
              if (process.env.NODE_ENV !== "production") {
                console.log("[FR_BAN] postcode_mismatch_reject", { requested: banInputPostcode, candidate: banPc });
              }
              return false;
            }
            if (banInputCity && banCityVal && !frCityMatches(banInputCity, banCityVal)) return false;
            return true;
          });
          const prevRejections = (frRuntimeDebug.fr_postcode_mismatch_rejections as number) ?? 0;
          frRuntimeDebug.fr_postcode_mismatch_rejections = prevRejections + postcodeMismatchCount;
          if (filtered.length > 0) {
            banRows = filtered;
            break;
          }
        }

        console.log("[FR_BAN] after_ban_query");
        const banRowsCount = banRows?.length ?? 0;
        console.log("[FR_BAN] candidate_count=" + banRowsCount);
        frRuntimeDebug.ban_rows_count = banRowsCount;
        frRuntimeDebug.fr_typed_street_normalized = streetNormalizedDet || null;
        frRuntimeDebug.fr_ban_candidate_count = banRowsCount;

        const pickStrFromRow = (r: Record<string, unknown>, keys: string[]): string => {
          for (const k of keys) {
            const v = r?.[k];
            if (typeof v === "string" && (v as string).trim()) return (v as string).trim();
          }
          return "";
        };
        const typedStreetRaw = streetNorm || "";
        const typedParsed = parseStreetForComparison(typedStreetRaw);
        frRuntimeDebug.fr_typed_street_type = typedParsed.type || null;
        frRuntimeDebug.fr_typed_street_core = typedParsed.core || null;
        const STRONG_STREET_THRESHOLD = 0.65;
        const scoredRows = (banRows ?? []).map((r) => {
          const rowStreet = pickStrFromRow(r, ["street_norm", "normalized_street", "street_norm_clean", "street"]);
          const { score, penalties } = scoreStreetSimilarity(typedStreetRaw, rowStreet);
          return { row: r, score, rowStreet, penalties };
        });
        scoredRows.sort((a, b) => b.score - a.score);
        const best = scoredRows[0];
        const useFullRow = best && (best.score >= STRONG_STREET_THRESHOLD || (banRowsCount === 1 && best.score > 0));
        const banRow: Record<string, unknown> | null =
          best && (useFullRow || (banInputPostcode && best.score >= 0))
            ? (best.row as Record<string, unknown>)
            : null;
        const banRowScore = best?.score ?? null;
        const banSelectionReason =
          !best
            ? "no_candidates"
            : best.score >= 1
              ? "exact_street_match"
              : best.score >= STRONG_STREET_THRESHOLD
                ? "strong_street_similarity"
                : banRowsCount === 1
                  ? "single_candidate"
                  : best.score > 0
                    ? "best_available_weak_street"
                    : "postcode_city_only_fallback";

        frRuntimeDebug.fr_ban_selected_street_score = banRowScore;
        frRuntimeDebug.fr_ban_selected_reason = banSelectionReason;
        frRuntimeDebug.fr_ban_similarity_threshold_passed = best != null && (best.score >= STRONG_STREET_THRESHOLD || (banRowsCount === 1 && best.score > 0));
        if (best) {
          const selParsed = parseStreetForComparison(best.rowStreet);
          frRuntimeDebug.fr_ban_selected_street_type = selParsed.type || null;
          frRuntimeDebug.fr_ban_selected_street_core = selParsed.core || null;
          frRuntimeDebug.fr_ban_selected_penalties = best.penalties?.length ? best.penalties.join(";") : null;
        }
        frRuntimeDebug.fr_ban_top_candidates_summary =
          scoredRows.length > 0
            ? scoredRows
                .slice(0, 5)
                .map((s, i) => `#${i + 1}:${(s.score * 100).toFixed(0)}%:${s.rowStreet || "(empty)"}`)
                .join(" | ")
            : null;

        const selectedMatchLevel = banRow ? "full" : "none";
        console.log("[FR_BAN] selected_match_level=" + selectedMatchLevel);
        console.log("[FR_BAN] street_score=" + String(banRowScore ?? "n/a"));
        console.log("[FR_BAN] selection_reason=" + banSelectionReason);

        if (banRow) {
          const selCity = (banRow.city_norm ?? banRow.city ?? banRow.normalized_city) as string | undefined;
          const selPc = (banRow.postcode ?? banRow.postal_code ?? banRow.code_postal) as string | undefined;
          console.log("[FR_BAN] selected_city=" + String(selCity ?? "(empty)"));
          console.log("[FR_BAN] selected_postcode=" + String(selPc ?? "(empty)"));
          const selReason = banInputPostcode && String(selPc ?? "").trim() === banInputPostcode.trim()
            ? "exact_postcode_match"
            : banInputCity && frCityMatches(banInputCity, String(selCity ?? ""))
              ? "exact_city_match"
              : banSelectionReason;
          console.log("[FR_BAN] selection_reason=" + selReason);
        }
        if (banRow) {
          const pickString = (keys: string[]): string | null => {
            for (const k of keys) {
              const v = banRow?.[k];
              if (typeof v === "string" && v.trim()) return v.trim();
            }
            return null;
          };
          const pickNumber = (keys: string[]): number | null => {
            for (const k of keys) {
              const v = banRow?.[k];
              const n = typeof v === "number" ? v : typeof v === "string" ? Number(v) : NaN;
              if (Number.isFinite(n)) return n;
            }
            return null;
          };

          const banCity = pickString(["city_norm", "normalized_city", "city"]);
          const banPostcode = pickString(["postcode", "postal_code", "postcode_norm", "code_postal"]);
          const banStreetNorm = pickString(["street_norm", "normalized_street", "street_norm_clean"]);
          const banHouse = pickString(["house_number_norm", "house_number"]);
          ban_lat = pickNumber(["lat", "latitude", "ban_lat"]);
          ban_lon = pickNumber(["lon", "lng", "longitude", "ban_lon"]);

          const postcodeExact = banPostcode ? banPostcode.trim() === banInputPostcode.trim() : false;
          const cityExact = banCity ? banCity.trim().toUpperCase() === banInputCity.trim().toUpperCase() : false;
          const streetExact = banStreetNorm ? banStreetNorm.trim() === banInputStreetNorm.trim() : false;
          const houseExact = banHouse ? banHouse.trim().toUpperCase() === banInputHouseNumber.trim().toUpperCase() : false;

          let banQuality: string;
          const matchLevel = postcodeExact && cityExact && streetExact && houseExact ? "full"
            : (postcodeExact && cityExact && streetExact) || (postcodeExact && streetExact) ? "street"
            : postcodeExact || cityExact || streetExact ? "postcode"
            : "none";
          const partialMatch = !houseExact && (banCity || banPostcode || banStreetNorm);
          if (postcodeExact && cityExact && streetExact && houseExact) banQuality = "exact_postcode_city_street_house";
          else if (postcodeExact && cityExact && streetExact && !houseExact) banQuality = "exact_postcode_city_street_same_street_no_house";
          else if (postcodeExact && streetExact && !cityExact) banQuality = "exact_postcode_street_same_street_no_house";
          else if (streetExact && !postcodeExact) banQuality = "street_same_no_exact_postcode_or_city";
          else banQuality = "ban_match_found";

          console.log("[FR_BAN] match_level=" + matchLevel);
          console.log("[FR_BAN] partial_match=" + String(partialMatch));

          // Source of truth: overwrite inputs for the rest of the FR pipeline.
          if (banCity) {
            cityNorm = banCity;
            ban_city = banCity;
          }
          if (banPostcode) {
            postcodeNorm = banPostcode;
            ban_postcode = banPostcode;
          }
          if (banStreetNorm && useFullRow) {
            streetNorm = banStreetNorm;
            ban_street = banStreetNorm;
          } else if (streetNorm && !useFullRow) {
            ban_street = streetNorm;
          }
          if (banHouse && useFullRow) {
            houseNumberNorm = banHouse;
            ban_house_number = banHouse;
          } else if (houseNumberNorm && !useFullRow) {
            ban_house_number = houseNumberNorm;
          }

          // Recompute normalized street used by intelligence_v2 detection.
          streetNormalizedDet = normalizeStreetForDetection(streetNorm);

          frRuntimeDebug.ban_match_found = true;
          frRuntimeDebug.ban_match_quality = banQuality;
          frRuntimeDebug.ban_lat = ban_lat;
          frRuntimeDebug.ban_lon = ban_lon;

          frRuntimeDebug.ban_city = ban_city;
          frRuntimeDebug.ban_postcode = ban_postcode;
          frRuntimeDebug.ban_street = ban_street;
          frRuntimeDebug.ban_house_number = ban_house_number;
          console.log("[FR_BAN] populated_debug_fields", {
            ban_match_found: frRuntimeDebug.ban_match_found,
            ban_rows_count: frRuntimeDebug.ban_rows_count,
            ban_city: frRuntimeDebug.ban_city,
            ban_postcode: frRuntimeDebug.ban_postcode,
            ban_street: frRuntimeDebug.ban_street,
            ban_house_number: frRuntimeDebug.ban_house_number,
            ban_match_quality: frRuntimeDebug.ban_match_quality,
          });
        } else {
          frRuntimeDebug.ban_match_found = false;
          frRuntimeDebug.ban_match_quality = "no_ban_row";
          console.log("[FR_BAN] populated_debug_fields", {
            ban_match_found: frRuntimeDebug.ban_match_found,
            ban_rows_count: frRuntimeDebug.ban_rows_count,
            ban_city: frRuntimeDebug.ban_city,
            ban_postcode: frRuntimeDebug.ban_postcode,
            ban_street: frRuntimeDebug.ban_street,
            ban_house_number: frRuntimeDebug.ban_house_number,
            ban_match_quality: frRuntimeDebug.ban_match_quality,
          });
        }
      } catch (err) {
        console.error("[FR_ERROR] BAN normalized lookup failed", err);
        frRuntimeDebug.ban_query_error = err instanceof Error ? err.message : String(err);
        frRuntimeDebug.ban_match_found = false;
        frRuntimeDebug.ban_match_quality = "ban_normalized_lookup_error";
      }

      // Ensure debug values are always populated even when BAN lookup fails.
      if (frRuntimeDebug.ban_city == null) frRuntimeDebug.ban_city = ban_city;
      if (frRuntimeDebug.ban_postcode == null) frRuntimeDebug.ban_postcode = ban_postcode;
      if (frRuntimeDebug.ban_street == null) frRuntimeDebug.ban_street = ban_street;
      if (frRuntimeDebug.ban_house_number == null) frRuntimeDebug.ban_house_number = ban_house_number;
      if (frRuntimeDebug.ban_lat == null) frRuntimeDebug.ban_lat = ban_lat;
      if (frRuntimeDebug.ban_lon == null) frRuntimeDebug.ban_lon = ban_lon;

      // Diagnostic: final BAN-normalized inputs used by detection + valuation.
      console.log("[FR_DEBUG] normalized_ban_output", {
        ban_match_found: frRuntimeDebug.ban_match_found,
        ban_match_quality: frRuntimeDebug.ban_match_quality,
        ban_city: frRuntimeDebug.ban_city,
        ban_postcode: frRuntimeDebug.ban_postcode,
        ban_street: frRuntimeDebug.ban_street,
        ban_house_number: frRuntimeDebug.ban_house_number,
        ban_lat: frRuntimeDebug.ban_lat,
        ban_lon: frRuntimeDebug.ban_lon,
        cityNorm,
        postcodeNorm,
        streetNorm,
        houseNumberNorm,
      });
      console.log("[FR_FLOW] ban_matched=" + String(Boolean(frRuntimeDebug.ban_match_found)));
      console.log("[FR_STEP] ban_lookup_done");

      console.log("[FR_ADDR] normalized_ban_street=" + String(streetNormalizedDet || ""));
      console.log("[FR_ADDR] normalized_house_number=" + String(houseNumberNorm || ""));

      const getBool = (obj: Record<string, unknown>, keys: string[]): boolean => {
        for (const k of keys) {
          const v = obj?.[k];
          if (typeof v === "boolean") return v;
          if (typeof v === "number") return v === 1;
          if (typeof v === "string") {
            const s = v.trim().toLowerCase();
            if (s === "true" || s === "1" || s === "yes" || s === "y") return true;
            if (s === "false" || s === "0" || s === "no" || s === "n") return false;
          }
        }
        return false;
      };

      const getString = (obj: Record<string, unknown>, keys: string[]): string | null => {
        for (const k of keys) {
          const v = obj?.[k];
          if (typeof v === "string") {
            const s = v.trim();
            if (s) return s;
          }
        }
        return null;
      };

      const getStringArray = (obj: Record<string, unknown>, keys: string[]): string[] => {
        for (const k of keys) {
          const v = obj?.[k];
          if (Array.isArray(v)) {
            const out = v
              .map((x) => (typeof x === "string" ? x : x == null ? "" : String(x)))
              .map((x) => x.trim())
              .filter(Boolean);
            if (out.length > 0) return out;
          }
        }
        return [];
      };

      // Early candidate discovery from property_latest_facts (France source) for apartment evidence
      // Normalize lookup keys consistently between BAN output and France facts.
      const postcodeNormForSource = normalizePostcodeForFranceSource(postcodeNorm);
      const cityNormForSource = normalizeCityForFranceSource(cityNorm);
      const streetParsedForSource = parseStreetForComparison(streetNorm || "");
      const streetNormForSource =
        streetParsedForSource.core && streetParsedForSource.core.length >= 2
          ? streetParsedForSource.core
          : streetNormalizedDet || "";
      const houseNumberNormForSource = houseNumberNorm || "";
      console.log("[FR_SOURCE] normalized_city=" + (cityNormForSource || "(empty)"));
      console.log("[FR_SOURCE] street_core=" + (streetNormForSource || "(empty)"));
      console.log("[FR_SOURCE] street_type=" + (streetParsedForSource.type || "(empty)"));
      console.log("[FR_SOURCE] normalized_postcode=" + (postcodeNormForSource || "(empty)"));
      console.log("[FR_SOURCE] lookup_keys=postcode|street|house|city");

      const frBqCityMatchSql = cityNormForSource
        ? `(LOWER(TRIM(city)) = LOWER(@city_main) OR LOWER(TRIM(city)) LIKE CONCAT(LOWER(@city_main), ' %'))`
        : "TRUE";
      const frBqPostcodeMatchSql = `LPAD(TRIM(CAST(postcode AS STRING)), 5, '0') = LPAD(TRIM(CAST(@postcode AS STRING)), 5, '0')`;
      const frBqStreetMatchSqlEarly = `(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(TRIM(street)), r'[^A-Z0-9 ]+', ' '), r'^(RUE|AVENUE|AV|BD|BOULEVARD|CHEMIN|CHE|ROUTE|IMPASSE|IMP|ALLEE|ALL|PLACE|PL|SQUARE|SQ|SENTE|COURS|PROMENADE|PROM)\\.?\\s+', '')) LIKE CONCAT('%', TRIM(@street_normalized), '%') OR REGEXP_REPLACE(UPPER(TRIM(street)), r'[^A-Z0-9 ]+', ' ') LIKE CONCAT('%', TRIM(@street_normalized), '%')`;
      const normHn = (hn: string) => {
        let s = (hn ?? "").toString().trim().toUpperCase();
        s = s.replace(/\s+BIS\b/gi, "B").replace(/\s+TER\b/gi, "T").replace(/\s+QUATER\b/gi, "Q");
        s = s.replace(/[-–—\s]+/g, "").replace(/[^0-9A-Z]/g, "");
        return s || "";
      };
      const houseNumberNormForEarly = normHn(houseNumberNorm);
      const frBqHouseMatchEarly = houseNumberNorm
        ? `(TRIM(CAST(house_number AS STRING)) = TRIM(CAST(@house_number AS STRING)) OR REGEXP_REPLACE(REGEXP_REPLACE(UPPER(TRIM(CAST(house_number AS STRING))), r'\\s+', ''), r'[^0-9A-Z]', '') = @house_number_norm)`
        : "TRUE";

      let earlyCandidateRows: Array<Record<string, unknown>> = [];
      let apartmentEvidenceFromFacts = false;
      let houseEvidenceFromFacts = false;
      let candidateLotsFromFacts: string[] = [];
      const hasAddressForDiscovery = (cityNorm || postcodeNorm) && streetNorm;
      if (hasAddressForDiscovery) {
        try {
          const earlyQuery = `
            SELECT unit_number, property_type
            FROM \`streetiq-bigquery.streetiq_gold.property_latest_facts\`
            WHERE LOWER(TRIM(country)) = 'fr'
              AND ${frBqPostcodeMatchSql}
              AND ${frBqCityMatchSql}
              AND ${frBqStreetMatchSqlEarly}
              AND ${frBqHouseMatchEarly}
            LIMIT 100
          `;
          const [earlyRows] = await queryWithTimeout<[Array<Record<string, unknown>>]>(
            {
              query: earlyQuery,
              params: {
                city: cityNorm || "",
                city_main: cityNormForSource || cityNorm || "",
                postcode: postcodeNormForSource || "",
                street_normalized: streetNormForSource || "",
                house_number: houseNumberNormForSource || "",
                house_number_norm: houseNumberNormForEarly || "",
              },
            },
            "early_candidate_discovery"
          );
          earlyCandidateRows = (earlyRows ?? []) as Array<Record<string, unknown>>;
          const lots = new Set<string>();
          let appartCount = 0;
          let maisonCount = 0;
          for (const r of earlyCandidateRows) {
            const pt = String((r as any).property_type ?? "").toLowerCase();
            if (pt.includes("appartement")) appartCount++;
            if (pt.includes("maison") || pt.includes("villa") || pt.includes("pavillon") || pt.includes("house")) maisonCount++;
            const un = (r as any).unit_number;
            if (un != null && String(un).trim()) lots.add(String(un).trim().replace(/^0+/, "") || String(un).trim());
          }
          apartmentEvidenceFromFacts = (lots.size >= 2 || appartCount >= 2) && !(maisonCount >= 1 && appartCount === 0);
          houseEvidenceFromFacts = maisonCount >= 1 && (appartCount === 0 || maisonCount > appartCount);
          candidateLotsFromFacts = Array.from(lots).filter(Boolean);
          console.log("[FR_SOURCE] source_path=facts");
          console.log("[FR_SOURCE] candidate_rows_found=" + String(earlyCandidateRows.length));
          console.log("[FR_CLASSIFY] apartment_evidence=" + (apartmentEvidenceFromFacts ? "multi_lot_or_appartement_from_facts" : "none_from_facts"));
          console.log("[FR_CLASSIFY] house_evidence_from_facts=" + (houseEvidenceFromFacts ? "maison_dominates" : "false"));
        } catch (e) {
          console.log("[FR_SOURCE] early_candidate_query_error", (e as Error)?.message);
        }
      }

      const detCityMatchSql = cityNormForSource
        ? `(LOWER(TRIM(city)) = LOWER(@city_main) OR LOWER(TRIM(city)) LIKE CONCAT(LOWER(@city_main), ' %'))`
        : "TRUE";
      const detectionQuery = `
        WITH candidates AS (
          SELECT
            *,
            REGEXP_REPLACE(
              REGEXP_REPLACE(UPPER(TRIM(street_norm)), r'[^A-Z0-9 ]+', ' '),
              r'\\s+',
              ' '
            ) AS street_norm_clean
          FROM \`streetiq-bigquery.streetiq_gold.france_building_intelligence_v2\`
        )
        SELECT
          *
        FROM candidates
        WHERE ${frBqPostcodeMatchSql}
          AND ${detCityMatchSql}
          AND TRIM(CAST(house_number_norm AS STRING)) = TRIM(CAST(@house_number AS STRING))
          AND (
            street_norm_clean LIKE CONCAT('%', @normalizedStreet, '%')
            OR @normalizedStreet LIKE CONCAT('%', street_norm_clean, '%')
          )
        ORDER BY unit_signal_count DESC, row_count DESC
        LIMIT 1
      `;

      console.log("[FR_INIT] about to run first query");
      let detectRows: Array<Record<string, unknown>> = [];
      try {
        console.log("[FR_GOLD] before_intelligence_detection_query");
        const detectParams = {
          city: cityNormForSource || cityNorm || "",
          city_main: cityNormForSource || cityNorm || "",
          postcode: postcodeNormForSource || "",
          normalizedStreet: streetNormForSource || "",
          house_number: houseNumberNormForSource || "",
        };
        console.log("[FR_PARAMS]", { query: "intelligence_detection_query", ...detectParams });
        [detectRows] = await queryWithTimeout<[Array<Record<string, unknown>>]>(
          {
            query: detectionQuery,
            params: detectParams,
          },
          "intelligence_detection_query"
        );
        console.log("[FR_GOLD] after_intelligence_detection_query", { rows: detectRows?.length ?? 0 });
      } catch (err) {
        console.error("[FR_ERROR] query failed", err);
        return new Response(JSON.stringify({ success: false, error: "Query failed" }), { status: 200 });
      }

      // Debug: log exact query params + raw row result (do not change logic).
      console.log("[FR_GOLD] intelligence_detection_query_params", {
        city: cityNorm,
        postcode: postcodeNorm,
        normalized_street: streetNormalizedDet,
        house_number: houseNumberNorm,
      });
      console.log("[FR_GOLD] intelligence_detection_raw_row", {
        row: detectRows?.[0] ?? null,
      });

      const detectRow = (detectRows?.[0] ?? {}) as Record<string, unknown>;
      const isMultiUnitDetected = getBool(detectRow, ["is_multi_unit", "isMultiUnit", "multi_unit", "is_multiunit"]);
      const isHouseLikeDetected = getBool(detectRow, ["is_house_like", "isHouseLike", "house_like", "is_house_like_flag", "houseLike"]);

      const detectedTypeStr = (getString(detectRow, [
        "detected_property_type",
        "property_type",
        "detected_type",
        "type_local",
        "type",
      ]) ?? "") as string;
      const detectedTypeLower = detectedTypeStr.toLowerCase();

      const apartmentFromType =
        detectedTypeLower.includes("appart") || detectedTypeLower.includes("apartment") || detectedTypeLower.includes("multi");
      const houseFromType = detectedTypeLower.includes("maison") || detectedTypeLower.includes("house") || detectedTypeLower.includes("villa");

      const banStreetThresholdPassed = frRuntimeDebug.fr_ban_similarity_threshold_passed === true;
      const banCity = (frRuntimeDebug.ban_city as string)?.trim() || null;
      const banPostcode = (frRuntimeDebug.ban_postcode as string)?.trim() || null;
      const banStreet = (frRuntimeDebug.ban_street as string)?.trim() || null;
      const banHouseNumber = (frRuntimeDebug.ban_house_number as string)?.trim() || null;
      const hasBanAddressMatch = Boolean(banCity && banPostcode && banStreet && banHouseNumber);
      const largeCityDepts = new Set(["75", "13", "69", "59", "33", "31", "44", "34", "67", "83", "84", "06", "74", "38", "35", "64", "17"]);
      const pcDept = (postcodeNorm || "").slice(0, 2);
      const isLargeUrbanCity = largeCityDepts.has(pcDept);
      const hasUrbanApartmentSignals = isLargeUrbanCity && hasBanAddressMatch;
      const allowMultiUnitFromQueries = banStreetThresholdPassed || hasUrbanApartmentSignals;

      const strongHouseSignals = houseEvidenceFromFacts;
      const strongApartmentSignals = allowMultiUnitFromQueries && apartmentEvidenceFromFacts;
      const mediumHouseSignals = isHouseLikeDetected || houseFromType;
      const mediumApartmentSignals = allowMultiUnitFromQueries && (isMultiUnitDetected || apartmentFromType);

      let multiUnitSource: "ban" | "source" | "building_intel" | "none" = "none";

      let detectClass: "apartment" | "house" | "unclear";
      const detectUsedLot = false;
      let detectOverrideReason: string | null = null;

      if (!allowMultiUnitFromQueries && (apartmentEvidenceFromFacts || isMultiUnitDetected)) {
        detectClass = "unclear";
        detectOverrideReason = "ban_weak_match_ignored_multi_unit_evidence";
      } else if (strongHouseSignals && !strongApartmentSignals) {
        detectClass = "house";
        detectOverrideReason = "facts_maison_dominates";
      } else if (strongApartmentSignals && !strongHouseSignals) {
        detectClass = "apartment";
      } else if (strongHouseSignals && strongApartmentSignals) {
        detectClass = "house";
        detectOverrideReason = "conflict_house_wins_over_apartment";
      } else if (mediumHouseSignals && !mediumApartmentSignals) {
        detectClass = "house";
        detectOverrideReason = "intelligence_house_signals";
      } else if (mediumApartmentSignals && !mediumHouseSignals) {
        detectClass = "apartment";
      } else if (mediumHouseSignals && mediumApartmentSignals) {
        detectClass = "house";
        detectOverrideReason = "tie_house_preferred_over_apartment";
      } else {
        detectClass = "unclear";
        detectOverrideReason = "no_strong_or_medium_signals";
      }

      if (strongHouseSignals && detectClass !== "house") {
        detectClass = "house";
        detectOverrideReason = "override_strong_house_ignored_other_signals";
      }

      // Force apartment detection when strong urban signals exist (large city + BAN address match).
      // Do not override house — house logic is unchanged.
      if (detectClass !== "house" && hasUrbanApartmentSignals) {
        detectClass = "apartment";
        detectOverrideReason = "urban_apartment_override_city_street_number";
      }

      if (detectClass === "apartment" && allowMultiUnitFromQueries) {
        multiUnitSource = apartmentEvidenceFromFacts ? "source" : isMultiUnitDetected ? "building_intel" : apartmentFromType ? "building_intel" : "none";
      }

      const shouldPromptLot = detectClass === "apartment" && !submittedLotPresent && allowMultiUnitFromQueries;

      const apartmentEvidenceDesc =
        apartmentEvidenceFromFacts ? "multi_lot_or_appartement_from_facts"
        : isMultiUnitDetected ? "building_intelligence_multi_unit"
        : apartmentFromType ? "detected_type_appartement"
        : "none";
      const multiUnitEvidenceDesc = isMultiUnitDetected ? "true" : "false";
      const signalsSummary = [
        houseEvidenceFromFacts ? "house_from_facts" : null,
        apartmentEvidenceFromFacts ? "apartment_from_facts" : null,
        isMultiUnitDetected ? "multi_unit" : null,
        isHouseLikeDetected ? "house_like" : null,
        apartmentFromType ? "type_appartement" : null,
        houseFromType ? "type_maison" : null,
        submittedLotPresent ? "submitted_lot" : null,
      ].filter(Boolean).join("|") || "none";

      frRuntimeDebug.detect_class = detectClass;
      frRuntimeDebug.fr_detect_signals_summary = signalsSummary;
      frRuntimeDebug.fr_detect_used_lot = detectUsedLot;
      frRuntimeDebug.fr_detect_override_reason = detectOverrideReason;
      frRuntimeDebug.fr_detect_multi_unit_source = multiUnitSource;
      frRuntimeDebug.fr_detect_ban_strength_used = banStreetThresholdPassed;
      frRuntimeDebug.fr_should_prompt_lot = shouldPromptLot;
      console.log("[FR_CLASSIFY] apartment_evidence=" + apartmentEvidenceDesc);
      console.log("[FR_CLASSIFY] multi_unit_evidence=" + multiUnitEvidenceDesc);
      console.log("[FR_CLASSIFY] detect_class=" + detectClass);
      const flowPropertyType = detectClass === "house" ? "house" : detectClass === "apartment" ? "apartment" : "unknown";
      console.log("[FR_FLOW] property_type=" + flowPropertyType);
      console.log("[FR_STEP] apartment_detection_done");

      console.log("[FR_GOLD] intelligence_detection_computed", {
        isMultiUnitDetected,
        isHouseLikeDetected,
        detectedTypeStr,
        detectClass,
      });

      const candidateLotsFromIntelligence = getStringArray(detectRow, [
        "candidate_lots",
        "candidateLots",
        "available_lots",
        "availableLots",
        "lots",
        "candidate_lot",
      ]);
      const candidateLots = candidateLotsFromFacts.length > 0 ? candidateLotsFromFacts : candidateLotsFromIntelligence;

      console.log("[FR_CLASSIFY] should_prompt_lot=" + String(detectClass === "apartment" && !submittedLotPresent));
      console.log("[FR_GOLD] intelligence_apartment_vs_house", {
        detectClass,
        isMultiUnitDetected,
        isHouseLikeDetected,
        detectedTypeStr: detectedTypeStr || null,
        candidateLotsCount: candidateLots.length,
      });

      // Canonical lot tokens used for exact apartment matching fallback.
      // Keeps strict "normalized token" semantics (trim + strip leading zeros).
      const canonicalCandidateLots = candidateLots
        .map((l) => normalizeLot(l))
        .map((l) => (l ? l.replace(/^0+/, "") : ""))
        .filter((l) => Boolean(l));

      const medianNumber = (values: number[]): number | null => {
        const cleaned = values.filter((v) => Number.isFinite(v));
        if (cleaned.length === 0) return null;
        const sorted = [...cleaned].sort((a, b) => a - b);
        const mid = Math.floor(sorted.length / 2);
        if (sorted.length % 2 === 1) return sorted[mid] ?? null;
        const a = sorted[mid - 1];
        const b = sorted[mid];
        if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
        return (a + b) / 2;
      };

      const parseMaybeDecimal = (v: unknown): number | null => {
        if (v === null || v === undefined) return null;
        if (typeof v === "number") return Number.isFinite(v) ? v : null;
        const raw = String(v).trim();
        if (!raw) return null;
        // Handle comma decimals and strip any non-numeric characters except . and -
        const cleaned = raw.replace(",", ".").replace(/[^\d.-]/g, "");
        const n = Number(cleaned);
        return Number.isFinite(n) ? n : null;
      };

      /** Cents → euros for display payloads (uses same parsing as ladder raw reads). */
      const frDvfCentsToEurosFromRow = (raw: unknown): number | null => {
        const n = parseMaybeDecimal(raw);
        if (n == null || !Number.isFinite(n)) return null;
        return n / 100;
      };

      // Temporary strict runtime SQL diagnostics for France ladder.
      const inspectFranceTable = async (
        ladderStep: "EXACT" | "BUILDING" | "STREET" | "COMMUNE",
        tableName: string
      ): Promise<{ columns: string[]; sampleRow: Record<string, unknown> | null }> => {
        const schemaQuery = `
          SELECT column_name
          FROM \`streetiq-bigquery.streetiq_gold.INFORMATION_SCHEMA.COLUMNS\`
          WHERE table_name = @table_name
          ORDER BY ordinal_position
        `;
        const [schemaRows] = await queryWithTimeout<[Array<{ column_name?: string }> ]>(
          {
            query: schemaQuery,
            params: { table_name: tableName || "" },
          },
          `schema_${tableName}`
        );
        const columns = (schemaRows ?? [])
          .map((r) => String(r?.column_name ?? "").trim())
          .filter(Boolean);

        const sampleQuery = `SELECT * FROM \`streetiq-bigquery.streetiq_gold.${tableName}\` LIMIT 1`;
        const [sampleRows] = await queryWithTimeout<[Array<Record<string, unknown>>]>(
          { query: sampleQuery, params: {} },
          `sample_${tableName}`
        );
        const sampleRow = (sampleRows?.[0] ?? null) as Record<string, unknown> | null;

        console.log("[FR_SQL] ladder_step=" + ladderStep);
        console.log("[FR_SQL] table_name=" + `streetiq-bigquery.streetiq_gold.${tableName}`);
        console.log("[FR_SQL] columns_detected=", columns);
        return { columns, sampleRow };
      };

      const exactTable = "property_latest_facts";
      const exactTableInspection = await inspectFranceTable("EXACT", exactTable);
      const pickExistingColumn = (cols: string[], wanted: string[]): string | null => {
        const map = new Map(cols.map((c) => [c.toLowerCase(), c]));
        for (const w of wanted) {
          const hit = map.get(w.toLowerCase());
          if (hit) return hit;
        }
        return null;
      };
      const lotLikeColsFromSchema = exactTableInspection.columns.filter((c) =>
        /\b(lot|unit|local|ident|appart|porte|cadast|dvf|no_porte|no_lot|numero)/i.test(c)
      );
      const exactPrimaryLotColumn = pickExistingColumn(exactTableInspection.columns, [
        "lot_1er",
        "lot1er",
        "unit_number",
        "identifiant_local",
        "local_id",
        "numero_lot",
        "no_lot",
        "numero_de_lot",
        "ref_lot",
      ]);
      frRuntimeDebug.exact_lot_column_used = exactPrimaryLotColumn;
      console.log("[FR_EXACT] lot_column_used=" + String(exactPrimaryLotColumn ?? "none"));
      console.log("[FR_EXACT] lot_like_schema_columns=" + JSON.stringify(lotLikeColsFromSchema));

      // Align with `normalizeStreetForDetection`: strip voie prefixes on the table side so
      // BAN "RUE …" matches facts "…" and vice versa.
      const frBqStreetMatchSql = `(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(UPPER(TRIM(street)), r'[^A-Z0-9 ]+', ' '), r'^(RUE|AVENUE|AV|BD|BOULEVARD|CHEMIN|CHE|ROUTE|IMPASSE|IMP|ALLEE|ALL|PLACE|PL|SQUARE|SQ|SENTE|COURS|PROMENADE|PROM)\\.?\\s+', '')) LIKE CONCAT('%', TRIM(@street_normalized), '%') OR REGEXP_REPLACE(UPPER(TRIM(street)), r'[^A-Z0-9 ]+', ' ') LIKE CONCAT('%', TRIM(@street_normalized), '%'))`;

      // Robust house_number: exact match or normalized (digits+letters, no spaces/punctuation).
      const normalizeHouseNumberForFacts = (hn: string): string => {
        let s = (hn ?? "").toString().trim().toUpperCase();
        s = s.replace(/\s+BIS\b/gi, "B").replace(/\s+TER\b/gi, "T").replace(/\s+QUATER\b/gi, "Q");
        s = s.replace(/[-–—\s]+/g, "").replace(/[^0-9A-Z]/g, "");
        return s || "";
      };
      const houseNumberNormForMatch = normalizeHouseNumberForFacts(houseNumberNorm);
      const frBqHouseNumberMatchSql = `(
        TRIM(CAST(house_number AS STRING)) = TRIM(CAST(@house_number AS STRING))
        OR REGEXP_REPLACE(REGEXP_REPLACE(UPPER(TRIM(CAST(house_number AS STRING))), r'\\s+', ''), r'[^0-9A-Z]', '') = @house_number_norm
      )`;
      const extractHouseNumberNumeric = (hn: string): number | null => {
        const m = /^\d+/.exec(String(hn ?? "").replace(/\s+/g, ""));
        return m ? parseInt(m[0], 10) : null;
      };
      const houseNumberNumericTarget = houseNumberNorm ? extractHouseNumberNumeric(houseNumberNorm) : null;

      const exactQuery = `
        SELECT
          *
        FROM \`streetiq-bigquery.streetiq_gold.${exactTable}\`
        WHERE LOWER(TRIM(country)) = LOWER(TRIM(@country))
          AND ${frBqPostcodeMatchSql}
          AND ${frBqCityMatchSql}
          AND ${frBqStreetMatchSql}
          AND ${frBqHouseNumberMatchSql}
        LIMIT 50
      `;
      const shouldPromptLotFirst = shouldPromptLot;
      console.log("[FR_LOT_API] normalizedRequestedLot", normalizedRequestedLot);
      console.log("[FR_LOT_API] submittedLotPresent", submittedLotPresent);
      console.log("[FR_LOT_API] shouldPromptLotFirst", shouldPromptLotFirst);
      console.log("[FR_FLOW] submitted_lot=" + String(normalizedRequestedLot ?? ""));
      console.log("[FR_FLOW] should_prompt_lot=" + String(shouldPromptLotFirst));
      console.log("[FR_FLOW] continue_to_valuation=" + String(!shouldPromptLotFirst));
      if (shouldPromptLotFirst) {
        if (submittedLotPresent) {
          console.error(
            "[FR_RETURN] blocked_prompt_lot_first_submitted_lot_present=true — continuing to valuation ladder"
          );
        } else {
          // Defer lot prompt: run ladder first to try building-level fallback (req 4).
          // If building profile returns a result, we return it. If not, return prompt before no_data (req 3).
          console.log("[FR_FLOW] apartment_no_lot_defer_prompt_until_after_building_fallback");
        }
      }

      console.log("[FR_FLOW] ladder_step_started=EXACT");
      console.log("[FR_STEP] exact_lookup_start");
      console.log("[FR_GOLD] before_exact_query");
      const exactParams = {
        country: country || "",
        city: cityNormForSource || cityNorm || "",
        city_main: cityNormForSource || cityNorm || "",
        postcode: postcodeNormForSource || "",
        street: streetNorm || "",
        street_normalized: streetNormForSource || "",
        house_number: houseNumberNormForSource || "",
        house_number_norm: houseNumberNormForMatch || "",
      };
      frRuntimeDebug.fr_source_lookup_postcode = postcodeNormForSource || null;
      frRuntimeDebug.fr_source_lookup_city = cityNormForSource || null;
      frRuntimeDebug.fr_source_lookup_street_raw = streetNorm || null;
      frRuntimeDebug.fr_source_lookup_street_core = streetNormForSource || null;
      frRuntimeDebug.fr_source_lookup_street_type = streetParsedForSource.type || null;
      frRuntimeDebug.fr_source_lookup_street = streetNormForSource || null;
      frRuntimeDebug.fr_source_lookup_house_number = houseNumberNormForSource || null;
      console.log("[FR_PARAMS]", { query: "exact_query", ...exactParams });
      const [exactRows] = await queryWithTimeout<[Array<Record<string, unknown>>]>(
        {
          query: exactQuery,
          params: exactParams,
        },
        "exact_query"
      );
      const exactRowsCount = (exactRows as any[])?.length ?? 0;
      frRuntimeDebug.fr_source_lookup_exact_count = exactRowsCount;
      if (exactRowsCount > 0) {
        frRuntimeDebug.fr_source_lookup_failed_reason = null;
      } else if (postcodeNormForSource || cityNormForSource || streetNormForSource) {
        frRuntimeDebug.fr_source_lookup_failed_reason =
          !streetNormForSource ? "no_street_for_lookup" : !postcodeNormForSource && !cityNormForSource ? "no_postcode_or_city" : "no_matching_rows_in_property_latest_facts";
      }
      console.log("[FR_SOURCE] source_rows_exact=" + exactRowsCount);
      console.log("[FR_GOLD] after_exact_query", { rows: exactRowsCount });
      console.log("[FR_SQL] query_ok=true");
      console.log("[FR_SQL] rows_count=", (exactRows as any[])?.length ?? 0);
      console.log("[FR_SQL] columns_detected=", Object.keys(exactTableInspection.sampleRow ?? {}));

      const rawExactHouseNumberRowCount = (exactRows as Array<Record<string, unknown>>).length;

      const normalizeLotToken = (v: unknown): string | null => {
        if (v === null || v === undefined) return null;
        const raw = typeof v === "number" ? String(v) : typeof v === "string" ? v : String(v);
        const trimmed = raw.trim();
        if (!trimmed) return null;
        const n = normalizeLot(trimmed).toUpperCase().replace(/\s+/g, " ").trim();
        if (!n) return null;
        return n.replace(/^0+/, "") || n;
      };

      const exactLotToken = normalizedRequestedLot;

      const extractLotTokensFromRow = (row: Record<string, unknown>): string[] => {
        const tokens: string[] = [];
        const pushIf = (val: unknown) => {
          const t = normalizeLotToken(val);
          if (t) tokens.push(t);
        };

        // unit_number
        pushIf((row as any).unit_number);

        // local_id / Identifiant local
        pushIf((row as any).local_id);
        pushIf((row as any).identifiant_local);
        pushIf((row as any)["Identifiant local"]);

        // lot1..lot5
        for (let i = 1; i <= 5; i++) {
          pushIf((row as any)[`lot${i}`]);
          pushIf((row as any)[`lot_${i}`]);
        }

        // candidate_lots / available_lots if present
        for (const k of ["candidate_lots", "available_lots", "lots"]) {
          const v = (row as any)[k];
          if (Array.isArray(v)) for (const item of v) pushIf(item);
          else pushIf(v);
        }

        // DVF / gold-table lot columns discovered at runtime (no guessed names in SQL).
        for (const col of lotLikeColsFromSchema) {
          pushIf((row as any)[col]);
        }

        return tokens;
      };

      /** Facts row has a non-null unit_number (only then do we enforce submitted lot vs row). */
      const primaryUnitNumberRaw = (r: Record<string, unknown>): string | null => {
        const un = (r as any).unit_number;
        if (un === null || un === undefined) return null;
        const s = typeof un === "number" ? String(un) : String(un).trim();
        return s === "" ? null : s;
      };

      const rowUsableForExact = (r: Record<string, unknown>): boolean => {
        const surf = parseMaybeDecimal((r as any).surface_m2);
        const ppm2 = parseMaybeDecimal((r as any).price_per_m2);
        return surf != null && surf > 0 && ppm2 != null && ppm2 > 0;
      };

      /** True only when `unit_number` normalizes to the submitted lot (strict unit-level evidence). */
      const unitTokenMatchesSubmittedLot = (r: Record<string, unknown>): boolean => {
        const un = primaryUnitNumberRaw(r);
        if (un == null) return false;
        const t = normalizeLotToken(un);
        return t != null && t === exactLotToken;
      };

      /** Single building-level aggregate: no unit_number and no resolvable lot tokens on the row. */
      const isAddressOnlyAggregateRow = (r: Record<string, unknown>): boolean =>
        primaryUnitNumberRaw(r) == null && extractLotTokensFromRow(r).length === 0;

      const sortExactCandidates = (rows: Array<Record<string, unknown>>): Record<string, unknown> | undefined => {
        if (rows.length === 0) return undefined;
        const sorted = [...rows].sort((a, b) => {
          const pa = parseMaybeDecimal((a as any).price_per_m2) ?? 0;
          const pb = parseMaybeDecimal((b as any).price_per_m2) ?? 0;
          if (pb !== pa) return pb - pa;
          const da = String((a as any).last_sale_date ?? "");
          const db = String((b as any).last_sale_date ?? "");
          return db.localeCompare(da);
        });
        return sorted[0];
      };

      const isHouseLikePropertyType = (r: Record<string, unknown>): boolean => {
        const pt = String((r as any).property_type ?? "").trim().toLowerCase();
        const hasUnit = primaryUnitNumberRaw(r) != null;
        if (hasUnit) return false;
        if (/maison|house|maison individuelle|villa|pavillon|maisonette/i.test(pt)) return true;
        if (!pt || pt === "local") return true;
        if (/appartement|appart/i.test(pt)) return false;
        return true;
      };

      const rowUsableForExactHouse = (r: Record<string, unknown>): boolean => {
        const surf = parseMaybeDecimal((r as any).surface_m2);
        const ppm2 = parseMaybeDecimal((r as any).price_per_m2);
        const lastSale = frPropertyLatestFactsMoneyToEuros((r as any).last_sale_price);
        if (surf != null && surf > 0 && ppm2 != null && ppm2 > 0) return true;
        if (lastSale != null && lastSale > 0) return true;
        return false;
      };

      if (detectClass === "house" && rawExactHouseNumberRowCount > 0) {
        const firstExactRow = (exactRows as Array<Record<string, unknown>>)[0];
        const factStreetRaw = firstExactRow ? String((firstExactRow as any).street ?? "") : "";
        const normalizedFactStreet = factStreetRaw ? normalizeStreetForDetection(factStreetRaw) : "";
        const matchedHouseNumber = firstExactRow ? String((firstExactRow as any).house_number ?? "") : houseNumberNorm;

        console.log("[FR_HOUSE] normalized_ban_street=" + String(streetNormalizedDet || ""));
        console.log("[FR_HOUSE] normalized_fact_street=" + String(normalizedFactStreet || ""));
        console.log("[FR_HOUSE] matched_house_number=" + String(matchedHouseNumber || ""));

        const exactHouseRows = (exactRows as Array<Record<string, unknown>>).filter(isHouseLikePropertyType);
        const exactHouseUsable = exactHouseRows.filter(rowUsableForExactHouse);
        frRuntimeDebug.exact_house_row_count = exactHouseRows.length;
        frRuntimeDebug.exact_house_usable_count = exactHouseUsable.length;

        console.log("[FR_HOUSE] row_found=" + String(exactHouseRows.length > 0));
        console.log("[FR_HOUSE] usable=" + String(exactHouseUsable.length > 0));
        console.log("[FR_HOUSE] exact_house_row_count=" + String(exactHouseRows.length));
        console.log("[FR_HOUSE] exact_house_usable_count=" + String(exactHouseUsable.length));

        const houseBest =
          exactHouseUsable.length > 0
            ? sortExactCandidates(exactHouseUsable)
            : exactHouseRows.length > 0
              ? sortExactCandidates(exactHouseRows)
              : undefined;
        if (houseBest) {
            const surface = parseMaybeDecimal((houseBest as any).surface_m2) ?? 0;
            const pricePerM2Euro = frPropertyLatestFactsMoneyToEuros((houseBest as any).price_per_m2) ?? 0;
            const lastSaleEuro = frPropertyLatestFactsMoneyToEuros((houseBest as any).last_sale_price) ?? 0;
            const estimated =
              Number.isFinite(surface) && surface > 0 && Number.isFinite(pricePerM2Euro) && pricePerM2Euro > 0
                ? Math.round(surface * pricePerM2Euro)
                : lastSaleEuro != null && lastSaleEuro > 0
                  ? lastSaleEuro
                  : null;
            frRuntimeDebug.property_latest_facts_money_divisor = 1000;
            frRuntimeDebug.winning_step = "exact_house";
            frRuntimeDebug.winning_source_label = "Exact house match";
            frRuntimeDebug.has_surface_for_estimate = surface != null && surface > 0;
            frRuntimeDebug.chosen_surface_value = surface;
            frRuntimeDebug.winning_median_price_per_m2 =
              Number.isFinite(pricePerM2Euro) && pricePerM2Euro > 0 ? pricePerM2Euro : null;
            return frReturn(
              {
                address: { city: cityNorm, street: streetNorm, house_number: houseNumberNorm },
                data_source: "properties_france",
                fr_detect: detectClass,
                property_result: {
                  exact_value: estimated,
                  exact_value_message: null,
                  value_level: "property-level",
                  last_transaction: {
                    amount: lastSaleEuro,
                    date: (houseBest as any).last_sale_date ?? null,
                    message: lastSaleEuro > 0 ? undefined : "No recent transaction available",
                  },
                  street_average: null,
                  street_average_message: "Exact house match",
                  livability_rating: "FAIR",
                },
                fr: emptyFranceResponse({
                  success: true,
                  resultType: "exact_house" as any,
                  confidence: "very_high" as any,
                  requestedLot: null,
                  normalizedLot: null,
                  property: {
                    transactionDate: (houseBest as any).last_sale_date ?? null,
                    transactionValue: lastSaleEuro > 0 ? lastSaleEuro : null,
                    pricePerSqm: Number.isFinite(pricePerM2Euro) && pricePerM2Euro > 0 ? pricePerM2Euro : null,
                    surfaceArea: Number.isFinite(surface) && surface > 0 ? surface : null,
                    rooms: null,
                    propertyType: String((houseBest as any).property_type ?? "Maison"),
                    building: `${houseNumberNorm} ${streetNorm}`.trim() || null,
                    postalCode: postcodeNorm || null,
                    commune: cityNorm || null,
                  },
                  buildingStats: null,
                  comparables: [],
                  matchExplanation: "Exact house match",
                }),
              },
              "valuation_response"
            );
        } else {
          const exactHouseRejectReason =
            exactHouseRows.length === 0 ? "no_house_type_rows_at_address" : "house_rows_missing_surface_or_ppm2";
          frRuntimeDebug.exact_house_reject_reason = exactHouseRejectReason;
          console.log("[FR_HOUSE] reject_reason=" + exactHouseRejectReason);
        }
      }

      if (detectClass === "house" && rawExactHouseNumberRowCount === 0) {
        console.log("[FR_HOUSE] normalized_ban_street=" + String(streetNormalizedDet || ""));
        console.log("[FR_HOUSE] normalized_fact_street=(no_rows)");
        console.log("[FR_HOUSE] matched_house_number=" + String(houseNumberNorm || ""));
        console.log("[FR_HOUSE] row_found=false");
        console.log("[FR_HOUSE] usable=false");
        frRuntimeDebug.exact_house_reject_reason = "no_exact_rows_for_address";
        console.log("[FR_HOUSE] reject_reason=no_exact_rows_for_address");
      }

      let exactTier: "EXACT_UNIT" | "EXACT_ADDRESS" | "NONE" = "NONE";
      let exactMatchingRows: Array<Record<string, unknown>> = [];
      let usableUnitRows: Array<Record<string, unknown>> = [];
      let usableAddressRows: Array<Record<string, unknown>> = [];

      if (!exactLotToken) {
        exactMatchingRows = [];
        frRuntimeDebug.exact_unit_row_count = 0;
        frRuntimeDebug.exact_address_row_count = 0;
      } else {
        const unitCandidates = (exactRows as Array<Record<string, unknown>>).filter((r) =>
          unitTokenMatchesSubmittedLot(r)
        );
        const addressCandidates = (exactRows as Array<Record<string, unknown>>).filter((r) =>
          isAddressOnlyAggregateRow(r)
        );
        exactMatchingRows = [...unitCandidates, ...addressCandidates];
        usableUnitRows = unitCandidates.filter(rowUsableForExact);
        usableAddressRows = addressCandidates.filter(rowUsableForExact);
        frRuntimeDebug.exact_unit_row_count = unitCandidates.length;
        frRuntimeDebug.exact_address_row_count = addressCandidates.length;
      }

      const exactApartmentRowsCount = exactMatchingRows.length;
      const exactUsableRowsCount = usableUnitRows.length + usableAddressRows.length;

      console.log("[FR_DEBUG] exact_apartment_query_counts", {
        submittedLot: aptNumber?.trim() || null,
        normalizedLot: normalizedRequestedLot,
        exactApartmentRowsCount,
        exactUsableRowsCount,
        exact_unit_row_count: frRuntimeDebug.exact_unit_row_count,
        exact_address_row_count: frRuntimeDebug.exact_address_row_count,
      });
      console.log("[FR_STEP] exact_lookup_done");

      frRuntimeDebug.exact_rows_count = exactApartmentRowsCount;
      frRuntimeDebug.exact_usable_rows_count = exactUsableRowsCount;

      let exactMatchReason: string;
      if (!exactLotToken) {
        exactMatchReason = "no_submitted_lot_token_skipped_apartment_row_filter";
      } else if (rawExactHouseNumberRowCount === 0) {
        exactMatchReason = "no_rows_for_ban_normalized_address_in_property_latest_facts";
      } else if (exactApartmentRowsCount === 0) {
        const sample = (exactRows as Array<Record<string, unknown>>)[0];
        const sampleTokens = sample ? extractLotTokensFromRow(sample) : [];
        console.log("[FR_EXACT] sample_row_lot_tokens=" + JSON.stringify(sampleTokens));
        exactMatchReason =
          "address_rows_exist_but_no_unit_or_address_aggregate_row_for_token_" + String(exactLotToken);
      } else if (exactUsableRowsCount === 0) {
        exactMatchReason = "exact_tier_rows_matched_but_missing_usable_surface_m2_or_price_per_m2";
      } else {
        exactMatchReason = "usable_exact_rows_available";
      }
      frRuntimeDebug.exact_match_reason = exactMatchReason;
      console.log("[FR_EXACT] exact_match_reason=" + exactMatchReason);
      console.log(
        "[FR_EXACT] raw_address_rows_count=" +
          String(rawExactHouseNumberRowCount) +
          " filtered_lot_rows=" +
          String(exactApartmentRowsCount)
      );

      let exactBest: Record<string, unknown> | undefined;
      let exactUnitMatch = false;
      if (usableUnitRows.length > 0) {
        exactBest = sortExactCandidates(usableUnitRows);
        exactTier = "EXACT_UNIT";
        exactUnitMatch = true;
      } else if (exactLotToken && usableAddressRows.length > 0) {
        exactBest = sortExactCandidates(usableAddressRows);
        exactTier = "EXACT_ADDRESS";
        exactUnitMatch = false;
      } else {
        exactBest = undefined;
        exactTier = "NONE";
        exactUnitMatch = false;
      }

      frRuntimeDebug.exact_level = exactTier;

      const usableExactRows =
        exactTier === "EXACT_UNIT" ? usableUnitRows : exactTier === "EXACT_ADDRESS" ? usableAddressRows : [];

      console.log("[FR_EXACT] submitted_lot=" + String(exactLotToken ?? ""));
      console.log("[FR_EXACT] unit_match=" + String(exactUnitMatch));
      console.log("[FR_EXACT] exact_level=" + exactTier);

      const logRowForFrExact =
        exactBest ??
        (usableExactRows.length > 0 ? usableExactRows[0] : null) ??
        (exactMatchingRows.length > 0 ? exactMatchingRows[0] : null) ??
        ((exactRows as Array<Record<string, unknown>>)[0] ?? null);
      const factStreetRawForLog = logRowForFrExact ? String((logRowForFrExact as any).street ?? "") : "";
      const normalizedFactStreet = factStreetRawForLog ? normalizeStreetForDetection(factStreetRawForLog) : "";
      console.log("[FR_ADDR] normalized_ban_street=" + String(streetNormalizedDet || ""));
      console.log("[FR_ADDR] normalized_fact_street=" + String(normalizedFactStreet || "—"));
      console.log("[FR_ADDR] normalized_house_number=" + String(houseNumberNorm || ""));
      console.log("[FR_EXACT] normalized_ban_street=" + String(streetNormalizedDet || ""));
      console.log("[FR_EXACT] normalized_fact_street=" + String(normalizedFactStreet || "—"));
      console.log(
        "[FR_EXACT] matched_house_number=" +
          (logRowForFrExact ? String((logRowForFrExact as any).house_number ?? "") : "—")
      );
      {
        const rowForUnitLog = exactBest ?? logRowForFrExact;
        const un = rowForUnitLog ? (rowForUnitLog as any).unit_number : undefined;
        const unLog =
          !rowForUnitLog
            ? "—"
            : un === null || un === undefined || String(un).trim() === ""
              ? "null"
              : String(un);
        console.log("[FR_EXACT] matched_unit_number=" + unLog);
      }

      const FR_LABEL_BUILDING_SIMILAR_UNIT = "Based on similar apartments in this building";

      const frBqHouseNumberNearbySql = houseNumberNumericTarget != null
        ? `(
            COALESCE(SAFE_CAST(REGEXP_EXTRACT(TRIM(CAST(house_number AS STRING)), r'^(\\d+)') AS INT64), 999999) BETWEEN @house_number_numeric_target - 2 AND @house_number_numeric_target + 2
          )`
        : "TRUE";

      type BuildingProfile = {
        building_id: string;
        median_price_per_m2: number;
        avg_price_per_m2: number;
        min_price_per_m2: number;
        max_price_per_m2: number;
        transaction_count: number;
        surface_min: number | null;
        surface_max: number | null;
        surface_median: number | null;
        apartment_count: number;
        house_count: number;
      };

      const tryFranceBuildingSimilarUnit = async (p: {
        medianSurfaceM2ForFallback: number | null;
        /** Prefer surfaces closest to this (input > exact row > building median). */
        chosenSurfaceValueForRanking: number | null;
        /** Building profile for low-candidate fallback and ranking. */
        buildingProfile?: BuildingProfile | null;
      }): Promise<ReturnType<typeof frReturn> | null> => {
        try {
          if (!houseNumberNorm || !postcodeNorm) {
            const rr = !houseNumberNorm ? "missing_house_number" : "missing_postcode";
            frRuntimeDebug.building_similar_unit_reject_reason = rr;
            console.log("[FR_BUILDING] reject_reason=" + rr);
            return null;
          }
          console.log("[FR_FLOW] ladder_step_started=BUILDING_SIMILAR_UNIT");
          console.log("[FR_STEP] building_similar_unit_lookup_start");
          const similarUnitQuery = `
          SELECT
            surface_m2,
            price_per_m2,
            last_sale_price,
            last_sale_date,
            property_type,
            house_number
          FROM \`streetiq-bigquery.streetiq_gold.property_latest_facts\`
          WHERE LOWER(TRIM(country)) = LOWER(TRIM(@country))
            AND LOWER(TRIM(city)) = LOWER(TRIM(@city))
            AND ${frBqPostcodeMatchSql}
            AND ${frBqStreetMatchSql}
            AND ${frBqHouseNumberMatchSql}
            AND LOWER(TRIM(CAST(property_type AS STRING))) = 'appartement'
          LIMIT 300
        `;
          const similarParams = {
            country: country || "",
            city: cityNormForSource || cityNorm || "",
            postcode: postcodeNormForSource || "",
            street: streetNorm || "",
            street_normalized: streetNormForSource || "",
            house_number: houseNumberNormForSource || "",
            house_number_norm: houseNumberNormForMatch || "",
          };
          let [similarRowsRaw] = await queryWithTimeout<[Array<Record<string, unknown>>]>(
            { query: similarUnitQuery, params: similarParams },
            "building_similar_unit_query"
          );
          let similarRows = similarRowsRaw ?? [];
          let rawCount = similarRows.length;

          if (rawCount < 2 && houseNumberNumericTarget != null) {
            const nearbyUnitQuery = `
            SELECT
              surface_m2,
              price_per_m2,
              last_sale_price,
              last_sale_date,
              property_type,
              house_number
            FROM \`streetiq-bigquery.streetiq_gold.property_latest_facts\`
            WHERE LOWER(TRIM(country)) = LOWER(TRIM(@country))
              AND LOWER(TRIM(city)) = LOWER(TRIM(@city))
              AND ${frBqPostcodeMatchSql}
              AND ${frBqStreetMatchSql}
              AND ${frBqHouseNumberNearbySql}
              AND LOWER(TRIM(CAST(property_type AS STRING))) = 'appartement'
            LIMIT 300
            `;
            const nearbyParams = {
              ...similarParams,
              house_number_numeric_target: houseNumberNumericTarget,
            };
            const [nearbyRows] = await queryWithTimeout<[Array<Record<string, unknown>>]>(
              { query: nearbyUnitQuery, params: nearbyParams },
              "building_similar_unit_nearby_query"
            );
            similarRows = nearbyRows ?? [];
            rawCount = similarRows.length;
            if (rawCount > 0) console.log("[FR_BUILDING] expanded_with_nearby_house_numbers rows=" + String(rawCount));
          }
          frRuntimeDebug.building_similar_unit_candidates_count = rawCount;
          console.log("[FR_GOLD] after_building_similar_unit_query", { rows: rawCount });
          console.log("[FR_BUILDING] candidates_count=" + String(rawCount));

          type SimilarEnriched = {
            raw: Record<string, unknown>;
            surf: number;
            ppmEuro: number;
            lastSaleEuro: number;
            dateStr: string | null;
            houseDistance: number;
          };

          const enriched: SimilarEnriched[] = [];
          const allowRelaxedFiveYearWindow = rawCount <= 4;
          for (const row of similarRows) {
            const surf = parseMaybeDecimal((row as any).surface_m2);
            const ppmRaw = parseMaybeDecimal((row as any).price_per_m2);
            const ppmEuro = frPropertyLatestFactsMoneyToEuros((row as any).price_per_m2) ?? 0;
            const lastSaleEuro = frPropertyLatestFactsMoneyToEuros((row as any).last_sale_price) ?? 0;
            const dateRaw = (row as any).last_sale_date;
            const dateStr =
              dateRaw === null || dateRaw === undefined
                ? null
                : String(dateRaw).trim() === ""
                  ? null
                  : String(dateRaw);
            const rowHn = (row as any).house_number;
            const rowNum = extractHouseNumberNumeric(String(rowHn ?? ""));
            const houseDistance =
              houseNumberNumericTarget != null && rowNum != null
                ? Math.abs(rowNum - houseNumberNumericTarget)
                : 0;

            if (surf == null || surf <= 0) continue;
            if (ppmRaw == null || ppmRaw <= 0 || ppmEuro <= 0) continue;
            if (!allowRelaxedFiveYearWindow && !frSaleDateWithinFiveYears(dateRaw)) continue;
            enriched.push({ raw: row, surf, ppmEuro, lastSaleEuro, dateStr, houseDistance });
          }

          const afterFilters = enriched.length;
          frRuntimeDebug.building_similar_unit_after_filters_count = afterFilters;
          console.log("[FR_BUILDING] after_filters_count=" + String(afterFilters));

          if (afterFilters === 0) {
            frRuntimeDebug.building_similar_unit_reject_reason = "no_candidates_after_quality_filters";
            console.log("[FR_BUILDING] reject_reason=no_candidates_after_quality_filters");
            return null;
          }

          let trimmed = enriched;
          if (enriched.length >= 5) {
            trimmed = frTrimFractionExtremes(enriched, (x) => x.ppmEuro, 0.1);
          }
          if (trimmed.length === 0) {
            console.log("[FR_BUILDING] reject_reason=outlier_trim_removed_all_fallback_to_untrimmed");
            trimmed = enriched;
          }

          const medSurfCohort = medianNumber(trimmed.map((x) => x.surf));
          const targetSurface =
            p.chosenSurfaceValueForRanking ??
            (medSurfCohort != null && Number.isFinite(medSurfCohort) ? medSurfCohort : null);

          const cohortMedian = medianNumber(trimmed.map((x) => x.ppmEuro).filter((n) => n > 0 && Number.isFinite(n))) ?? 0;
          const medianPpmEuro =
            p.buildingProfile && rawCount <= 2 && p.buildingProfile.transaction_count > 0
              ? p.buildingProfile.median_price_per_m2
              : cohortMedian;
          if (p.buildingProfile && rawCount <= 2) {
            console.log("[FR_BUILDING_PROFILE] used_in=building (low candidates, using building median)");
          }

          const sorted = [...trimmed].sort((a, b) => {
            if (targetSurface != null && Number.isFinite(targetSurface)) {
              const da = Math.abs(a.surf - targetSurface);
              const db = Math.abs(b.surf - targetSurface);
              if (da !== db) return da - db;
            }
            if (a.houseDistance !== b.houseDistance) return a.houseDistance - b.houseDistance;
            const ta = a.dateStr ? new Date(a.dateStr).getTime() : 0;
            const tb = b.dateStr ? new Date(b.dateStr).getTime() : 0;
            if (tb !== ta) return tb - ta;
            if (medianPpmEuro > 0) {
              const ma = Math.abs(a.ppmEuro - medianPpmEuro);
              const mb = Math.abs(b.ppmEuro - medianPpmEuro);
              if (ma !== mb) return ma - mb;
            }
            if (a.ppmEuro !== b.ppmEuro) return a.ppmEuro - b.ppmEuro;
            return a.surf - b.surf;
          });

          const best = sorted[0];
          if (!best) {
            frRuntimeDebug.building_similar_unit_reject_reason = "no_best_after_sort";
            console.log("[FR_BUILDING] reject_reason=no_best_after_sort");
            return null;
          }

          const estSurf = validInputSurfaceM2 ?? p.medianSurfaceM2ForFallback ?? best.surf;
          const ppmForEstimate =
            p.buildingProfile && rawCount <= 2 && p.buildingProfile.transaction_count > 0
              ? p.buildingProfile.median_price_per_m2
              : p.chosenSurfaceValueForRanking != null
                ? best.ppmEuro
                : medianPpmEuro > 0
                  ? medianPpmEuro
                  : best.ppmEuro;
          const estimated =
            Number.isFinite(estSurf) && estSurf > 0 && ppmForEstimate > 0 ? Math.round(estSurf * ppmForEstimate) : null;

          const selectedSurfaceDiff =
            targetSurface != null && Number.isFinite(targetSurface)
              ? Math.abs(best.surf - targetSurface)
              : medSurfCohort != null && Number.isFinite(medSurfCohort)
                ? Math.abs(best.surf - medSurfCohort)
                : null;

          const recencyTs = best.dateStr ? new Date(best.dateStr).getTime() : 0;
          const rankingScore = (selectedSurfaceDiff ?? 0) * 1000 + (recencyTs > 0 ? 1e15 - recencyTs : 0);

          console.log("[FR_SURFACE] chosen_surface_value=" + String(targetSurface ?? "null"));
          console.log("[FR_SURFACE] candidate_surface=" + String(best.surf));
          console.log("[FR_SURFACE] surface_diff=" + String(selectedSurfaceDiff ?? "null"));
          console.log("[FR_SURFACE] ranking_score=" + String(rankingScore));

          console.log("[FR_BUILDING] selected_surface=" + String(estSurf));
          console.log("[FR_BUILDING] selected_surface_diff=" + String(selectedSurfaceDiff ?? "null"));
          console.log("[FR_BUILDING] selected_house_distance=" + String(best.houseDistance));
          console.log("[FR_BUILDING] selected_sale_date=" + String(best.dateStr ?? "null"));
          console.log("[FR_BUILDING] selected_price_per_m2=" + String(best.ppmEuro));

          frRuntimeDebug.building_similar_unit_reject_reason = null;
          frRuntimeDebug.winning_step = "building_similar_unit";
          frRuntimeDebug.winning_source_label = FR_LABEL_BUILDING_SIMILAR_UNIT;
          frRuntimeDebug.has_surface_for_estimate = estSurf != null && estSurf > 0;
          frRuntimeDebug.chosen_surface_value = estSurf;
          frRuntimeDebug.winning_median_price_per_m2 = ppmForEstimate;
          frRuntimeDebug.property_latest_facts_money_divisor = 1000;

          console.log("[FR_DEBUG] winning_valuation_step", {
            winningValuationStep: "building_similar_unit",
            winningSourceLabel: FR_LABEL_BUILDING_SIMILAR_UNIT,
          });

          return frReturn(
            {
              address: { city: cityNorm, street: streetNorm, house_number: houseNumberNorm },
              data_source: "properties_france",
              fr_detect: detectClass,
              property_result: {
                exact_value: estimated,
                exact_value_message:
                  estimated == null
                    ? `${FR_LABEL_BUILDING_SIMILAR_UNIT} — total estimate needs surface where available.`
                    : null,
                value_level: "building-level",
                last_transaction: {
                  amount: best.lastSaleEuro,
                  date: best.dateStr,
                  message:
                    best.lastSaleEuro > 0 ? undefined : "No recorded sale amount for selected comparable row",
                },
                street_average: ppmForEstimate > 0 ? ppmForEstimate : null,
                street_average_message: FR_LABEL_BUILDING_SIMILAR_UNIT,
                livability_rating: "FAIR",
              },
              fr: emptyFranceResponse({
                success: true,
                resultType: "building_similar_unit" as any,
                confidence: "high",
                requestedLot: requestedLotNorm,
                normalizedLot: normalizedRequestedLot,
                property: {
                  transactionDate: best.dateStr,
                  transactionValue: best.lastSaleEuro > 0 ? best.lastSaleEuro : null,
                  pricePerSqm: ppmForEstimate > 0 ? ppmForEstimate : best.ppmEuro,
                  surfaceArea: Number.isFinite(estSurf) && estSurf > 0 ? estSurf : null,
                  rooms: null,
                  propertyType: "Appartement",
                  building: `${houseNumberNorm} ${streetNorm}`.trim() || null,
                  postalCode: postcodeNorm || null,
                  commune: cityNorm || null,
                },
                buildingStats: {
                  transactionCount: trimmed.length,
                  avgPricePerSqm: ppmForEstimate > 0 ? ppmForEstimate : best.ppmEuro,
                  avgTransactionValue: estimated,
                },
                comparables: [],
                matchExplanation: FR_LABEL_BUILDING_SIMILAR_UNIT,
              }),
            },
            "valuation_response"
          );
        } catch (err) {
          frRuntimeDebug.building_similar_unit_reject_reason = "building_similar_unit_query_failed";
          console.error("[FR_BUILDING] building_similar_unit_query_failed", err);
          return null;
        } finally {
          console.log("[FR_STEP] building_similar_unit_lookup_done");
        }
      };

      const buildingSimilarBase =
        detectClass === "apartment" && Boolean(exactLotToken) && Boolean(houseNumberNorm) && Boolean(postcodeNorm);

      /** Same-building address aggregate (no unit_number): prefer Appartement cohort over exact_address when it wins. */
      const buildingSimilarAfterExactAddressNoUnit =
        buildingSimilarBase &&
        exactTier === "EXACT_ADDRESS" &&
        exactBest != null &&
        primaryUnitNumberRaw(exactBest) == null;

      /** No exact lot/unit row: still try same-address Appartement rows before generic BUILDING median / STREET. */
      const buildingSimilarAfterExactNone =
        buildingSimilarBase && exactTier === "NONE" && rawExactHouseNumberRowCount > 0;

      let exactAddressHadUsableEstimate = false;

      if (exactBest) {
        const surface = parseMaybeDecimal((exactBest as any).surface_m2) ?? 0;
        const rawLastSalePrice = (exactBest as any).last_sale_price;
        const rawPricePerM2Plf = (exactBest as any).price_per_m2;
        const nLspProbe = frParseNumericLoose(rawLastSalePrice);
        const pricePerM2Euro = frPropertyLatestFactsMoneyToEuros(rawPricePerM2Plf) ?? 0;
        const lastSaleEuro = frPropertyLatestFactsMoneyToEuros(rawLastSalePrice) ?? 0;
        frRuntimeDebug.property_latest_facts_money_divisor = 1000;
        console.log("[FR_PRICE] raw_last_sale_price=" + String(rawLastSalePrice));
        console.log("[FR_PRICE] raw_price_per_m2_property_latest_facts=" + String(rawPricePerM2Plf));
        if (nLspProbe != null && Number.isFinite(nLspProbe)) {
          console.log("[FR_PRICE] probe_last_sale_euros_if_centimes_div100=" + String(nLspProbe / 100));
          console.log("[FR_PRICE] probe_last_sale_euros_if_plf_thousandths_div1000=" + String(nLspProbe / 1000));
        }
        console.log("[FR_PRICE] api_last_sale_price=" + String(lastSaleEuro));
        console.log("[FR_PRICE] api_price_per_m2_euro=" + String(pricePerM2Euro));
        const estimated =
          Number.isFinite(surface) && surface > 0 && Number.isFinite(pricePerM2Euro) && pricePerM2Euro > 0
            ? Math.round(surface * pricePerM2Euro)
            : null;
        const hasEstimated = estimated != null;
        exactAddressHadUsableEstimate = hasEstimated;

        if (hasEstimated && buildingSimilarAfterExactAddressNoUnit) {
          const chosenSurfaceValueForRanking =
            validInputSurfaceM2 ?? (Number.isFinite(surface) && surface > 0 ? surface : null);
          const buildingSimilarWin = await tryFranceBuildingSimilarUnit({
            medianSurfaceM2ForFallback: null,
            chosenSurfaceValueForRanking,
          });
          if (buildingSimilarWin) return buildingSimilarWin;
        }

        if (hasEstimated) {
          console.log("[FR_PRICE] api_estimated_value_exact_surface_x_ppm=" + String(estimated));
          const isExactUnitTier = exactTier === "EXACT_UNIT";
          const winningStep = isExactUnitTier ? "exact_unit" : "exact_address";
          const winningSourceLabel = isExactUnitTier
            ? detectClass === "apartment"
              ? "Exact apartment"
              : "Exact property"
            : "Exact address match";
          const frResultType = isExactUnitTier ? "exact_apartment" : "exact_address";
          const frConfidence = isExactUnitTier ? "medium_high" : "high";
          frRuntimeDebug.exact_reject_reason = "";
          console.log("[FR_EXACT] exact_reject_reason=");
          console.log("[FR_DEBUG] winning_valuation_step", {
            winningValuationStep: winningStep,
            winningSourceLabel,
            exact_level: exactTier,
          });
          frRuntimeDebug.winning_step = winningStep;
          frRuntimeDebug.winning_source_label = winningSourceLabel;
          frRuntimeDebug.has_surface_for_estimate = surface != null && surface > 0;
          frRuntimeDebug.chosen_surface_value = surface;
          frRuntimeDebug.winning_median_price_per_m2 =
            Number.isFinite(pricePerM2Euro) && pricePerM2Euro > 0 ? pricePerM2Euro : null;
          return frReturn({
            address: { city: cityNorm, street: streetNorm, house_number: houseNumberNorm },
            data_source: "properties_france",
            fr_detect: detectClass,
            property_result: {
              exact_value: estimated,
              exact_value_message: null,
              value_level: "property-level",
              last_transaction: {
                amount: lastSaleEuro,
                date: (exactBest as any).last_sale_date ?? null,
                message: lastSaleEuro > 0 ? undefined : "No recent transaction available",
              },
              street_average: null,
              street_average_message: winningSourceLabel,
              livability_rating: "FAIR",
            },
            fr: emptyFranceResponse({
              success: true,
              resultType: frResultType as any,
              confidence: frConfidence as any,
              requestedLot: requestedLotNorm,
              normalizedLot: normalizedRequestedLot,
              property: {
                transactionDate: (exactBest as any).last_sale_date ?? null,
                transactionValue: lastSaleEuro > 0 ? lastSaleEuro : null,
                pricePerSqm: Number.isFinite(pricePerM2Euro) && pricePerM2Euro > 0 ? pricePerM2Euro : null,
                surfaceArea: Number.isFinite(surface) && surface > 0 ? surface : null,
                rooms: null,
                propertyType: null,
                building: `${houseNumberNorm} ${streetNorm}`.trim() || null,
                postalCode: postcodeNorm || null,
                commune: cityNorm || null,
              },
              buildingStats: null,
              comparables: [],
              matchExplanation: isExactUnitTier
                ? detectClass === "apartment"
                  ? "Exact apartment match"
                  : "Exact property match"
                : "Exact address match (building-level record; unit not matched in data)",
            }),
          }, "valuation_response");
        }
        // If the row exists but we couldn't compute an estimated value, continue the ladder.
      }

      {
        let frExactRejectReason: string;
        if (exactBest) {
          frExactRejectReason = "matched_row_but_could_not_compute_total_estimate";
        } else if (!exactLotToken) {
          frExactRejectReason = "skipped_apartment_exact_no_submitted_lot_token";
        } else if (rawExactHouseNumberRowCount === 0) {
          frExactRejectReason = "no_property_latest_facts_rows_for_address_key";
        } else if (exactApartmentRowsCount === 0) {
          frExactRejectReason = "lot_or_unit_filter_removed_all_rows";
        } else if (exactUsableRowsCount === 0) {
          frExactRejectReason = "no_usable_surface_m2_or_price_per_m2_after_lot_filter";
        } else {
          frExactRejectReason = "no_usable_exact_best_after_sort";
        }
        frRuntimeDebug.exact_reject_reason = frExactRejectReason;
        console.log("[FR_EXACT] exact_reject_reason=" + frExactRejectReason);
      }

      let buildingProfile: BuildingProfile | null = null;
      if (houseNumberNorm && streetNorm && cityNorm && postcodeNorm) {
        const buildingId = [cityNorm, postcodeNorm, streetNormalizedDet || streetNorm, houseNumberNorm]
          .filter(Boolean)
          .join("|")
          .toLowerCase();
        try {
          const profileQuery = `
            SELECT price_per_m2, surface_m2, property_type
            FROM \`streetiq-bigquery.streetiq_gold.property_latest_facts\`
            WHERE LOWER(TRIM(country)) = LOWER(TRIM(@country))
              AND LOWER(TRIM(city)) = LOWER(TRIM(@city))
              AND ${frBqPostcodeMatchSql}
              AND ${frBqStreetMatchSql}
              AND ${frBqHouseNumberMatchSql}
            LIMIT 200
          `;
          const [profileRows] = await queryWithTimeout<
            [Array<{ price_per_m2?: unknown; surface_m2?: unknown; property_type?: unknown }>]
          >(
            {
              query: profileQuery,
              params: {
                country: country || "",
                city: cityNormForSource || cityNorm || "",
                postcode: postcodeNormForSource || "",
                street: streetNorm || "",
                street_normalized: streetNormForSource || "",
                house_number: houseNumberNormForSource || "",
                house_number_norm: houseNumberNormForMatch || "",
              },
            },
            "building_profile_query"
          );
          const rows = (profileRows ?? []) as Array<{ price_per_m2?: unknown; surface_m2?: unknown; property_type?: unknown }>;
          const withPpm = rows
            .map((r: { price_per_m2?: unknown; surface_m2?: unknown; property_type?: unknown }) => ({
              ppm: frPropertyLatestFactsMoneyToEuros(r.price_per_m2) as number | null,
              surf: parseMaybeDecimal(r.surface_m2) as number | null,
              pt: String(r.property_type ?? "").trim().toLowerCase(),
            }))
            .filter((x): x is { ppm: number; surf: number | null; pt: string } => x.ppm != null && x.ppm > 0);
          const apartmentCount = withPpm.filter((x) => x.pt.includes("appartement")).length;
          const houseCount = withPpm.filter((x) =>
            x.pt.includes("maison") || x.pt.includes("villa") || x.pt.includes("pavillon") || x.pt === "local" || x.pt === ""
          ).length;
          if (withPpm.length >= 1) {
            const ppmValues = withPpm.map((x) => x.ppm);
            const surfValues = withPpm.map((x) => x.surf).filter((v): v is number => v != null && v > 0);
            buildingProfile = {
              building_id: buildingId,
              median_price_per_m2: medianNumber(ppmValues) ?? ppmValues[0]!,
              avg_price_per_m2: ppmValues.reduce((a, b) => a + b, 0) / ppmValues.length,
              min_price_per_m2: Math.min(...ppmValues),
              max_price_per_m2: Math.max(...ppmValues),
              transaction_count: withPpm.length,
              surface_min: surfValues.length > 0 ? Math.min(...surfValues) : null,
              surface_max: surfValues.length > 0 ? Math.max(...surfValues) : null,
              surface_median: surfValues.length > 0 ? medianNumber(surfValues) : null,
              apartment_count: apartmentCount,
              house_count: houseCount,
            };
            console.log("[FR_BUILDING_PROFILE] building_id=" + buildingId);
            console.log("[FR_BUILDING_PROFILE] median_price_per_m2=" + String(buildingProfile.median_price_per_m2));
            console.log("[FR_BUILDING_PROFILE] transaction_count=" + String(buildingProfile.transaction_count));
          }
        } catch (e) {
          console.log("[FR_BUILDING_PROFILE] query_error", (e as Error)?.message);
        }
      }

      if (buildingSimilarAfterExactNone) {
        const buildingSimilarNoneWin = await tryFranceBuildingSimilarUnit({
          medianSurfaceM2ForFallback: null,
          chosenSurfaceValueForRanking: validInputSurfaceM2,
          buildingProfile,
        });
        if (buildingSimilarNoneWin) return buildingSimilarNoneWin;
      }

      // Same building fallback (when unit/lot was provided but exact unit match is missing):
      // Uses property_latest_facts with the same address key but without unit_number filtering.
      const MIN_SAME_BUILDING_USABLE_ROWS = 2;
      let medianSurfaceM2ForFallback: number | null = null;
      let sameBuildingRowsCount = 0;
      let sameBuildingUsableRowsCount = 0;
      if (houseNumberNorm) {
        if (rawExactHouseNumberRowCount === 0) {
          console.log(
            "[FR_FLOW] ladder_step_skipped=BUILDING reason=no_property_latest_facts_rows_for_this_house_number (same filter as EXACT)"
          );
          frRuntimeDebug.building_rows_count = 0;
          frRuntimeDebug.building_usable_rows_count = 0;
          console.log("[FR_STEP] building_lookup_done");
        } else {
        console.log("[FR_FLOW] ladder_step_started=BUILDING");
        const buildingTable = "property_latest_facts";
        const buildingTableInspection = await inspectFranceTable("BUILDING", buildingTable);
        console.log("[FR_STEP] building_lookup_start");
        const buildingQuery = `
          SELECT
            surface_m2,
            price_per_m2,
            last_sale_price,
            last_sale_date
          FROM \`streetiq-bigquery.streetiq_gold.${buildingTable}\`
          WHERE LOWER(TRIM(country)) = LOWER(TRIM(@country))
            AND LOWER(TRIM(city)) = LOWER(TRIM(@city))
            AND ${frBqPostcodeMatchSql}
            AND ${frBqStreetMatchSql}
            AND ${frBqHouseNumberMatchSql}
          LIMIT 50
        `;
        console.log("[FR_GOLD] before_building_query");
        const buildingParams = {
          country: country || "",
          city: cityNormForSource || cityNorm || "",
          postcode: postcodeNormForSource || "",
          street: streetNorm || "",
          street_normalized: streetNormForSource || "",
          house_number: houseNumberNormForSource || "",
          house_number_norm: houseNumberNormForMatch || "",
        };
        console.log("[FR_PARAMS]", { query: "building_same_address_query", ...buildingParams });
        const [buildingRows] = await queryWithTimeout<
          Array<{ surface_m2?: number; price_per_m2?: number; last_sale_price?: number; last_sale_date?: string | null }>
        >(
          {
            query: buildingQuery,
            params: buildingParams,
          },
          "building_same_address_query"
        );
        console.log("[FR_GOLD] after_building_query", { rows: (buildingRows as any[])?.length ?? 0 });
        console.log("[FR_SQL] query_ok=true");
        console.log("[FR_SQL] rows_count=", (buildingRows as any[])?.length ?? 0);
        console.log("[FR_SQL] columns_detected=", Object.keys(buildingTableInspection.sampleRow ?? {}));

        const sameBuildingRows = buildingRows as Array<{ surface_m2?: number; price_per_m2?: number; last_sale_price?: number; last_sale_date?: string | null }>;
        sameBuildingRowsCount = sameBuildingRows.length;
        const usablePriceRows = sameBuildingRows.filter((r) => {
          const surf = parseMaybeDecimal(r.surface_m2);
          const ppm2 = parseMaybeDecimal(r.price_per_m2);
          return surf != null && surf > 0 && ppm2 != null && ppm2 > 0;
        });
        sameBuildingUsableRowsCount = usablePriceRows.length;

        const surfacesForMedian = sameBuildingRows
          .map((r) => parseMaybeDecimal(r.surface_m2))
          .filter((v): v is number => v != null && v > 0);
        medianSurfaceM2ForFallback = surfacesForMedian.length > 0 ? medianNumber(surfacesForMedian) : null;

        console.log("[FR_DEBUG] same_building_matching", {
          submittedLot: aptNumber?.trim() || null,
          normalizedLot: normalizedRequestedLot,
          sameBuildingRowsCount,
          sameBuildingUsableRowsCount,
          sameBuildingMinUsableThreshold: MIN_SAME_BUILDING_USABLE_ROWS,
          medianSurfaceM2ForFallback,
        });

        frRuntimeDebug.building_rows_count = sameBuildingRowsCount;
        frRuntimeDebug.building_usable_rows_count = sameBuildingUsableRowsCount;
        console.log("[FR_STEP] building_lookup_done");

        if (sameBuildingUsableRowsCount >= MIN_SAME_BUILDING_USABLE_ROWS && medianSurfaceM2ForFallback != null) {
          const pricePerM2Values = usablePriceRows.map((r) => parseMaybeDecimal(r.price_per_m2)).filter((v): v is number => v != null && v > 0);
          const medianPricePerM2 = medianNumber(pricePerM2Values);
          if (medianPricePerM2 != null && medianPricePerM2 > 0) {
            frRuntimeDebug.property_latest_facts_money_divisor = 1000;
            const medianPricePerM2Euro = frPropertyLatestFactsMoneyToEuros(medianPricePerM2) ?? 0;
            const estimated = Math.round(medianSurfaceM2ForFallback * medianPricePerM2Euro);
            console.log("[FR_DEBUG] winning_valuation_step", {
              winningValuationStep: "building_level",
              winningSourceLabel: "Similar properties in this building",
            });
            frRuntimeDebug.winning_step = "building_level";
            frRuntimeDebug.winning_source_label = "Similar properties in this building";
            frRuntimeDebug.has_surface_for_estimate = medianSurfaceM2ForFallback != null;
            frRuntimeDebug.chosen_surface_value = medianSurfaceM2ForFallback;
            frRuntimeDebug.winning_median_price_per_m2 = medianPricePerM2Euro;
            return frReturn(
              {
                address: { city: cityNorm, street: streetNorm, house_number: houseNumberNorm },
                data_source: "properties_france",
                fr_detect: detectClass,
                property_result: {
                  exact_value: estimated,
                  exact_value_message: null,
                  value_level: "building-level",
                  last_transaction: {
                    amount: 0,
                    date: null,
                    message: "No exact recent transaction available",
                  },
                  street_average: null,
                  street_average_message: "Similar properties in this building",
                  livability_rating: "FAIR",
                },
                fr: emptyFranceResponse({
                  success: true,
                  resultType: "building_level",
                  confidence: "high",
                  requestedLot: requestedLotNorm,
                  normalizedLot: normalizedRequestedLot,
                  property: null,
                  buildingStats: { transactionCount: sameBuildingUsableRowsCount, avgPricePerSqm: medianPricePerM2Euro, avgTransactionValue: null },
                  comparables: [],
                  matchExplanation: "Similar properties in this building (median from same-address rows).",
                }),
              },
              "valuation_response"
            );
          }
        }
        // If building-level estimate can't be computed, continue the ladder to street/commune fallback.
        }
      }

      // Apartment + EXACT_ADDRESS (no unit_number): second chance after BUILDING median when exact row had no total estimate.
      if (
        buildingSimilarAfterExactAddressNoUnit &&
        exactBest &&
        !exactAddressHadUsableEstimate &&
        houseNumberNorm &&
        postcodeNorm
      ) {
        const exactRowSurface = parseMaybeDecimal((exactBest as any).surface_m2);
        const chosenSurfaceValueForRanking =
          validInputSurfaceM2 ??
          (exactRowSurface != null && exactRowSurface > 0 ? exactRowSurface : null) ??
          medianSurfaceM2ForFallback;
        const buildingSimilarLate = await tryFranceBuildingSimilarUnit({
          medianSurfaceM2ForFallback,
          chosenSurfaceValueForRanking,
          buildingProfile,
        });
        if (buildingSimilarLate) return buildingSimilarLate;
      }

      if (!houseNumberNorm) console.log("[FR_STEP] building_lookup_done");

      // Same-street house fallback: when exact_house failed but house-like rows exist on same street.
      const fallbackSourceSameStreetHouse = "Similar houses on same street";
      if (
        detectClass === "house" &&
        streetNorm &&
        postcodeNorm &&
        cityNorm
      ) {
        const sameStreetHouseQuery = `
          SELECT price_per_m2, last_sale_date, house_number
          FROM \`streetiq-bigquery.streetiq_gold.property_latest_facts\`
          WHERE LOWER(TRIM(country)) = LOWER(TRIM(@country))
            AND LOWER(TRIM(city)) = LOWER(TRIM(@city))
            AND ${frBqPostcodeMatchSql}
            AND ${frBqStreetMatchSql}
            AND (LOWER(TRIM(CAST(property_type AS STRING))) LIKE '%maison%'
                 OR LOWER(TRIM(CAST(property_type AS STRING))) LIKE '%villa%'
                 OR LOWER(TRIM(CAST(property_type AS STRING))) LIKE '%pavillon%'
                 OR LOWER(TRIM(CAST(property_type AS STRING))) = 'local'
                 OR TRIM(CAST(property_type AS STRING)) = '')
          LIMIT 100
        `;
          try {
          const [sameStreetRows] = await queryWithTimeout<[Array<{ price_per_m2?: unknown; last_sale_date?: string | null; house_number?: unknown }>]>(
            {
              query: sameStreetHouseQuery,
              params: {
                country: country || "",
                city: cityNormForSource || cityNorm || "",
                postcode: postcodeNormForSource || "",
                street: streetNorm || "",
                street_normalized: streetNormForSource || "",
              },
            },
            "same_street_house_fallback"
          );
          const withPpmAndDist = (sameStreetRows ?? []).map((r) => {
            const ppm = frPropertyLatestFactsMoneyToEuros(r.price_per_m2);
            const hn = r.house_number;
            const dist =
              houseNumberNumericTarget != null && hn != null
                ? Math.abs((extractHouseNumberNumeric(String(hn)) ?? 999999) - houseNumberNumericTarget)
                : 0;
            return { ppm, dist };
          }).filter((x): x is { ppm: number; dist: number } => x.ppm != null && x.ppm > 0);
          if (withPpmAndDist.length >= 1) {
            withPpmAndDist.sort((a, b) => a.dist - b.dist);
            let pool = withPpmAndDist.filter((x) => x.dist <= 0);
            if (pool.length < 2) pool = withPpmAndDist.filter((x) => x.dist <= 1);
            if (pool.length < 2) pool = withPpmAndDist.filter((x) => x.dist <= 2);
            if (pool.length < 2) pool = withPpmAndDist.filter((x) => x.dist <= 5);
            if (pool.length < 2) pool = withPpmAndDist;
            const ppmValues = pool.map((x) => x.ppm);
            const trimmedPpm = ppmValues.length >= 5 ? frTrimFractionExtremes(ppmValues.map((p) => ({ p })), (x) => x.p, 0.1).map((x) => x.p) : ppmValues;
            const medianPpm = medianNumber(trimmedPpm);
            if (medianPpm != null && medianPpm > 0) {
              const surfEst = validInputSurfaceM2 ?? medianSurfaceM2ForFallback;
              const surfaceForEst = surfEst ?? null;
              const estimated = surfaceForEst != null && surfaceForEst > 0 ? Math.round(surfaceForEst * medianPpm) : null;
              frRuntimeDebug.winning_median_price_per_m2 = medianPpm;
              frRuntimeDebug.winning_step = "street_fallback";
              frRuntimeDebug.winning_source_label = fallbackSourceSameStreetHouse;
              frRuntimeDebug.has_surface_for_estimate = surfaceForEst != null;
              frRuntimeDebug.chosen_surface_value = surfaceForEst;
              console.log("[FR_FLOW] valuation_ladder_complete tag=valuation_response branch=SAME_STREET_HOUSE_FROM_FACTS");
              return frReturn(
                {
                  address: { city: cityNorm, street: streetNorm, house_number: houseNumberNorm },
                  data_source: "properties_france",
                  fr_detect: "house",
                  property_result: {
                    exact_value: estimated,
                    exact_value_message: estimated == null ? "Surface needed for total estimate" : null,
                    value_level: "street-level",
                    last_transaction: { amount: 0, date: null, message: "No exact recent transaction available" },
                    street_average: medianPpm,
                    street_average_message: fallbackSourceSameStreetHouse,
                    livability_rating: "FAIR",
                  },
                  fr: emptyFranceResponse({
                    success: true,
                    resultType: "nearby_comparable",
                    confidence: "medium",
                    requestedLot: null,
                    normalizedLot: null,
                    property: {
                      transactionDate: null,
                      transactionValue: estimated,
                      pricePerSqm: medianPpm,
                      surfaceArea: surfaceForEst ?? null,
                      rooms: null,
                      propertyType: "Maison",
                      building: `${houseNumberNorm} ${streetNorm}`.trim() || null,
                      postalCode: postcodeNorm || null,
                      commune: cityNorm || null,
                    },
                    buildingStats: null,
                    comparables: [],
                    matchExplanation: fallbackSourceSameStreetHouse,
                  }),
                },
                "valuation_response"
              );
            }
          }
        } catch (e) {
          console.log("[FR_FLOW] same_street_house_fallback_error", (e as Error)?.message);
        }
      }

      // Use the gold-table driven apartment-vs-house decision for the valuation ladder.
      const frDetectToUse: "apartment" | "house" | "unclear" = detectClass;

      // Runtime schema inspection (no guessing): detect real sale-date column per fallback table.
      const detectSaleDateColumn = (columns: string[]): "latest_sale_date" | "newest_sale_date" | null => {
        if (columns.includes("latest_sale_date")) return "latest_sale_date";
        if (columns.includes("newest_sale_date")) return "newest_sale_date";
        return null;
      };

      const streetTable = "property_area_fallback";
      const communeTable = "france_commune_property_stats";
      const streetTableInspection = await inspectFranceTable("STREET", streetTable);
      const communeTableInspection = await inspectFranceTable("COMMUNE", communeTable);
      const streetSaleDateColumn = detectSaleDateColumn(streetTableInspection.columns);
      const communeSaleDateColumn = detectSaleDateColumn(communeTableInspection.columns);
      console.log("[FR_SQL] failing_query_name", "fallback_street_query");
      console.log("[FR_SQL] failing_table", `streetiq-bigquery.streetiq_gold.${streetTable}`);
      console.log("[FR_SQL] sale_date_column_used", streetSaleDateColumn);
      console.log("[FR_SQL] failing_query_name", "fallback_commune_stats_query");
      console.log("[FR_SQL] failing_table", `streetiq-bigquery.streetiq_gold.${communeTable}`);
      console.log("[FR_SQL] sale_date_column_used", communeSaleDateColumn);

      const fallbackStreetQuery = `
        SELECT
          avg_price_per_m2,
          ${streetSaleDateColumn ? streetSaleDateColumn : "NULL AS sale_date"}
        FROM \`streetiq-bigquery.streetiq_gold.${streetTable}\`
        WHERE LOWER(TRIM(country)) = LOWER(TRIM(@country))
          AND LOWER(TRIM(city)) = LOWER(TRIM(@city))
          AND TRIM(CAST(postcode AS STRING)) = TRIM(CAST(@postcode AS STRING))
          AND ${frBqStreetMatchSql}
          AND (@property_type = "" OR LOWER(TRIM(property_type)) = LOWER(TRIM(@property_type)))
        LIMIT 20
      `;

      const fallbackCommuneStatsQuery = `
        SELECT
          avg_price_per_m2,
          ${communeSaleDateColumn ? communeSaleDateColumn : "NULL AS sale_date"}
        FROM \`streetiq-bigquery.streetiq_gold.${communeTable}\`
        WHERE LOWER(TRIM(country)) = LOWER(TRIM(@country))
          AND LOWER(TRIM(city)) = LOWER(TRIM(@city))
          AND ${frBqPostcodeMatchSql}
        LIMIT 20
      `;

      // Building profile fallback: prefer over street when available
      const tryBuildingProfileFallback = (): ReturnType<typeof frReturn> | null => {
        if (!buildingProfile || buildingProfile.transaction_count < 1) return null;
        const forHouses = detectClass === "house" && buildingProfile.house_count > 0;
        const forApartments =
          (detectClass === "apartment" || (detectClass === "unclear" && buildingProfile.apartment_count > 0)) &&
          (buildingProfile.apartment_count > 0 || buildingProfile.transaction_count > 0);
        if (!forHouses && !forApartments) return null;
        const medianPpm = buildingProfile.median_price_per_m2;
        const surfaceForEst = validInputSurfaceM2 ?? buildingProfile.surface_median ?? medianSurfaceM2ForFallback;
        const estimated = surfaceForEst != null && surfaceForEst > 0 ? Math.round(surfaceForEst * medianPpm) : null;
        const fallbackSource = "Similar properties in this building";
        console.log("[FR_BUILDING_PROFILE] used_in=fallback");
        frRuntimeDebug.winning_step = "building_profile";
        frRuntimeDebug.winning_source_label = fallbackSource;
        frRuntimeDebug.winning_median_price_per_m2 = medianPpm;
        return frReturn(
          {
            address: { city: cityNorm, street: streetNorm, house_number: houseNumberNorm },
            data_source: "properties_france",
            fr_detect: frDetectToUse,
            property_result: {
              exact_value: estimated,
              exact_value_message: estimated == null ? "Surface needed for total estimate" : null,
              value_level: "building-level",
              last_transaction: { amount: 0, date: null, message: "No exact recent transaction available" },
              street_average: medianPpm,
              street_average_message: fallbackSource,
              livability_rating: "FAIR",
            },
            fr: emptyFranceResponse({
              success: true,
              resultType: "building_level",
              confidence: "medium",
              requestedLot: requestedLotNorm,
              normalizedLot: normalizedRequestedLot,
              property: {
                transactionDate: null,
                transactionValue: estimated,
                pricePerSqm: medianPpm,
                surfaceArea: surfaceForEst ?? null,
                rooms: null,
                propertyType: detectClass === "house" ? "Maison" : "Appartement",
                building: `${houseNumberNorm} ${streetNorm}`.trim() || null,
                postalCode: postcodeNorm || null,
                commune: cityNorm || null,
              },
              buildingStats: {
                transactionCount: buildingProfile.transaction_count,
                avgPricePerSqm: medianPpm,
                avgTransactionValue: estimated,
              },
              comparables: [],
              matchExplanation: fallbackSource,
            }),
          },
          "valuation_response"
        );
      };

      const buildingProfileWin = tryBuildingProfileFallback();
      if (buildingProfileWin) return buildingProfileWin;

      console.log("[FR_FLOW] ladder_step_started=STREET");
      console.log("[FR_STEP] street_lookup_start");
      console.log("[FR_GOLD] before_fallback_query", { level: "same_street" });
      const streetParams = {
        country: country || "",
        city: cityNormForSource || cityNorm || "",
        postcode: postcodeNormForSource || "",
        street: streetNorm || "",
        street_normalized: streetNormForSource || "",
        property_type: propertyType || "",
      };
      console.log("[FR_PARAMS]", { query: "fallback_street_query", ...streetParams });
      let [fallbackStreetRows] = await queryWithTimeout<[Array<{ avg_price_per_m2?: number; newest_sale_date?: string | null; latest_sale_date?: string | null; sale_date?: string | null }> ]>(
        {
          query: fallbackStreetQuery,
          params: streetParams,
        },
        "fallback_street_query"
      );
      if ((fallbackStreetRows ?? []).length === 0 && propertyType) {
        const streetParamsNoType = { ...streetParams, property_type: "" };
        console.log("[FR_PARAMS]", { query: "fallback_street_query_retry_no_property_type", ...streetParamsNoType });
        const [retryRows] = await queryWithTimeout<[Array<{ avg_price_per_m2?: number; newest_sale_date?: string | null; latest_sale_date?: string | null; sale_date?: string | null }> ]>(
          { query: fallbackStreetQuery, params: streetParamsNoType },
          "fallback_street_query_retry"
        );
        fallbackStreetRows = retryRows;
        if ((fallbackStreetRows ?? []).length > 0) console.log("[FR_FLOW] street_retry_without_property_type_rows=" + String((fallbackStreetRows ?? []).length));
      }
      console.log("[FR_GOLD] after_fallback_query", { level: "same_street", rows: (fallbackStreetRows as any[])?.length ?? 0 });
      console.log("[FR_SQL] query_ok=true");
      console.log("[FR_SQL] rows_count=", (fallbackStreetRows as any[])?.length ?? 0);
      console.log("[FR_SQL] columns_detected=", Object.keys(streetTableInspection.sampleRow ?? {}));
      const fallbackSourceStreet = "Similar properties on same street";
      const fallbackSourceCommune = "Similar properties in same commune";

      let streetRows = fallbackStreetRows as Array<{ avg_price_per_m2?: number; newest_sale_date?: string | null; latest_sale_date?: string | null; sale_date?: string | null }>;
      let streetFallbackRowsCount = streetRows.length;
      let streetUsableAvgRows = streetRows.filter((r) => {
        const v = parseMaybeDecimal(r.avg_price_per_m2);
        return v != null && v > 0;
      });
      let streetUsableAvgRowsCount = streetUsableAvgRows.length;
      let streetClosestHouseDistance: number | null = null;

      if (streetFallbackRowsCount === 0 && streetNorm && postcodeNorm && cityNorm) {
        try {
          const factsStreetQuery = `
            SELECT price_per_m2, house_number
            FROM \`streetiq-bigquery.streetiq_gold.property_latest_facts\`
            WHERE LOWER(TRIM(country)) = LOWER(TRIM(@country))
              AND LOWER(TRIM(city)) = LOWER(TRIM(@city))
              AND ${frBqPostcodeMatchSql}
              AND ${frBqStreetMatchSql}
            LIMIT 200
          `;
          const [factsRows] = await queryWithTimeout<[Array<{ price_per_m2?: unknown; house_number?: unknown }>]>(
            {
              query: factsStreetQuery,
              params: {
                country: country || "",
                city: cityNormForSource || cityNorm || "",
                postcode: postcodeNormForSource || "",
                street: streetNorm || "",
                street_normalized: streetNormForSource || "",
              },
            },
            "street_from_facts_fallback"
          );
          const withPpm = (factsRows ?? []).map((r) => {
            const ppm = frPropertyLatestFactsMoneyToEuros(r.price_per_m2);
            const hn = r.house_number;
            const dist =
              houseNumberNumericTarget != null && hn != null
                ? Math.abs((extractHouseNumberNumeric(String(hn)) ?? 999999) - houseNumberNumericTarget)
                : 0;
            return { ppm, dist };
          }).filter((x): x is { ppm: number; dist: number } => x.ppm != null && x.ppm > 0);
          if (withPpm.length >= 1) {
            withPpm.sort((a, b) => a.dist - b.dist);
            const closestDist = withPpm[0]?.dist ?? 0;
            let pool = withPpm.filter((x) => x.dist <= 0);
            if (pool.length < 3) pool = withPpm.filter((x) => x.dist <= 1);
            if (pool.length < 3) pool = withPpm.filter((x) => x.dist <= 2);
            if (pool.length < 3) pool = withPpm.filter((x) => x.dist <= 5);
            if (pool.length < 3) pool = withPpm;
            const ppmPool = pool.map((x) => x.ppm);
            const trimmedPpm =
              ppmPool.length >= 5
                ? frTrimFractionExtremes(ppmPool.map((p) => ({ p })), (x) => x.p, 0.1).map((x) => x.p)
                : ppmPool;
            const medianPpmEuro = medianNumber(trimmedPpm) ?? ppmPool[0] ?? 0;
            streetRows = [{ avg_price_per_m2: medianPpmEuro * 100 }];
            streetFallbackRowsCount = 1;
            streetUsableAvgRows = streetRows;
            streetUsableAvgRowsCount = 1;
            console.log("[FR_FLOW] street_from_facts_fallback rows=" + String(withPpm.length) + " pool=" + String(pool.length) + " median_ppm=" + String(medianPpmEuro));
            streetClosestHouseDistance = closestDist;
          }
        } catch (e) {
          console.log("[FR_FLOW] street_from_facts_fallback_error", (e as Error)?.message);
        }
      }

      const surfaceForEstimation = validInputSurfaceM2 ?? medianSurfaceM2ForFallback;

      console.log("[FR_DEBUG] street_fallback_matching", {
        submittedLot: aptNumber?.trim() || null,
        normalizedLot: normalizedRequestedLot,
        streetFallbackRowsCount,
        streetUsableAvgRowsCount,
        surfaceForEstimation,
      });
      console.log("[FR_STEP] street_lookup_done");

      frRuntimeDebug.street_rows_count = streetFallbackRowsCount;
      frRuntimeDebug.street_usable_rows_count = streetUsableAvgRowsCount;
      frRuntimeDebug.fr_source_lookup_street_count = streetFallbackRowsCount;

      const tryStreetFallback = () => {
        if (streetUsableAvgRowsCount <= 0) return null;
        // No outlier filtering for street when rows < 5 – allow small samples
        const avgPriceValues = streetUsableAvgRows
          .map((r) => parseMaybeDecimal(r.avg_price_per_m2))
          .filter((v): v is number => v != null && v > 0);
        const medianAvgPricePerM2 = medianNumber(avgPriceValues);
        if (medianAvgPricePerM2 == null || !Number.isFinite(medianAvgPricePerM2) || medianAvgPricePerM2 <= 0) return null;
        const medianAvgPricePerM2Euro = medianAvgPricePerM2 / 100;
        const estimated =
          surfaceForEstimation != null ? Math.round(surfaceForEstimation * medianAvgPricePerM2Euro) : null;
        return {
          avgPricePerM2: medianAvgPricePerM2Euro,
          estimated,
          newestSaleDate:
            streetSaleDateColumn === "latest_sale_date"
              ? (streetUsableAvgRows[0]?.latest_sale_date ?? null)
              : streetSaleDateColumn === "newest_sale_date"
                ? (streetUsableAvgRows[0]?.newest_sale_date ?? null)
                : (streetUsableAvgRows[0]?.sale_date ?? null),
        };
      };

      const streetEstimate = tryStreetFallback();
      const streetFallbackDecision = streetEstimate ? "street" : "commune";
      console.log("[FR_STREET] rows_found=" + String(streetFallbackRowsCount));
      console.log("[FR_STREET] rows_used=" + String(streetUsableAvgRowsCount));
      if (streetEstimate) {
        console.log("[FR_STREET] median_price_per_m2=" + String(streetEstimate.avgPricePerM2));
        if (streetClosestHouseDistance != null) console.log("[FR_STREET] closest_house_distance=" + String(streetClosestHouseDistance));
      }
      if (!streetEstimate) {
        console.log("[FR_STREET] rejected_reason=" + (streetUsableAvgRowsCount <= 0 ? "no_usable_rows" : "median_invalid"));
      }
      console.log("[FR_STREET] fallback_decision=" + streetFallbackDecision);

      if (streetEstimate) {
        const streetConfidence = streetUsableAvgRowsCount <= 2 ? "low" : "medium";
        console.log("[FR_DEBUG] winning_valuation_step", {
          winningValuationStep: "street_fallback",
          winningSourceLabel: fallbackSourceStreet,
        });
        if (streetEstimate.estimated != null) {
          frRuntimeDebug.winning_median_price_per_m2 = streetEstimate.avgPricePerM2;
          frRuntimeDebug.winning_step = "street_fallback";
          frRuntimeDebug.winning_source_label = fallbackSourceStreet;
          frRuntimeDebug.has_surface_for_estimate = surfaceForEstimation != null;
          frRuntimeDebug.chosen_surface_value = surfaceForEstimation;
          console.log(
            "[FR_FLOW] valuation_ladder_complete tag=valuation_response branch=STREET_numeric (EXACT+BUILDING+STREET ran)"
          );
          return frReturn({
            address: { city: cityNorm, street: streetNorm, house_number: houseNumberNorm },
            data_source: "properties_france",
            fr_detect: frDetectToUse,
            property_result: {
              exact_value: streetEstimate.estimated,
              exact_value_message: null,
              value_level: "street-level",
              last_transaction: {
                amount: 0,
                date: null,
                message: "No exact recent transaction available",
              },
              // Median €/m² for UI when headline uses price/m² (same numeric scale as exact_value uses total €).
              street_average: streetEstimate.avgPricePerM2,
              street_average_message: fallbackSourceStreet,
              livability_rating: "FAIR",
            },
            fr: emptyFranceResponse({
              // success=true so the UI does not go to "No reliable data found"
              success: true,
              resultType: "nearby_comparable",
              confidence: streetConfidence as "low" | "medium",
              requestedLot: requestedLotNorm,
              normalizedLot: normalizedRequestedLot,
              property: {
                // Estimated market value must not be treated as an exact recent transaction.
                transactionDate: null,
                transactionValue: streetEstimate.estimated,
                pricePerSqm: streetEstimate.avgPricePerM2,
                surfaceArea: surfaceForEstimation ?? null,
                rooms: null,
                propertyType: propertyType,
                building: `${houseNumberNorm} ${streetNorm}`.trim() || null,
                postalCode: postcodeNorm || null,
                commune: cityNorm || null,
              },
              buildingStats: null,
              comparables: [],
              matchExplanation:
                surfaceForEstimation == null
                  ? `${fallbackSourceStreet} — comparable pricing available, but exact estimate could not be computed (missing surface).`
                  : fallbackSourceStreet,
            }),
          }, "valuation_response");
        }
        // Street has valid price_per_m2 but no surface for total estimate: prefer street over commune.
        const streetOnlyMessage = `${fallbackSourceStreet} — comparable pricing available, but exact estimate could not be computed (missing surface).`;
        frRuntimeDebug.winning_median_price_per_m2 = streetEstimate.avgPricePerM2;
        frRuntimeDebug.winning_step = "street_fallback";
        frRuntimeDebug.winning_source_label = fallbackSourceStreet;
        frRuntimeDebug.has_surface_for_estimate = false;
        frRuntimeDebug.chosen_surface_value = null;
        console.log("[FR_FLOW] valuation_ladder_complete tag=valuation_response branch=STREET_price_only (prefer street over commune)");
        return frReturn(
          {
            address: { city: cityNorm, street: streetNorm, house_number: houseNumberNorm },
            data_source: "properties_france",
            fr_detect: frDetectToUse,
            property_result: {
              exact_value: null,
              exact_value_message: null,
              value_level: "street-level",
              last_transaction: {
                amount: 0,
                date: null,
                message: "No exact recent transaction available",
              },
              street_average: streetEstimate.avgPricePerM2,
              street_average_message: streetOnlyMessage,
              livability_rating: "FAIR",
            },
            fr: emptyFranceResponse({
              success: true,
              resultType: "nearby_comparable",
              confidence: streetConfidence as "low" | "medium",
              requestedLot: requestedLotNorm,
              normalizedLot: normalizedRequestedLot,
              property: {
                transactionDate: null,
                transactionValue: null,
                pricePerSqm: streetEstimate.avgPricePerM2,
                surfaceArea: null,
                rooms: null,
                propertyType: propertyType,
                building: `${houseNumberNorm} ${streetNorm}`.trim() || null,
                postalCode: postcodeNorm || null,
                commune: cityNorm || null,
              },
              buildingStats: null,
              comparables: [],
              matchExplanation: streetOnlyMessage,
            }),
          },
          "valuation_response"
        );
      }

      // Street fallback wasn't usable; try commune fallback next.
      console.log("[FR_FLOW] ladder_step_started=COMMUNE");
      console.log("[FR_STEP] commune_lookup_start");
      console.log("[FR_GOLD] before_fallback_query", { level: "commune_stats" });
      const communeParams = {
        country: country || "",
        city: cityNormForSource || cityNorm || "",
        postcode: postcodeNormForSource || "",
      };
      console.log("[FR_PARAMS]", { query: "fallback_commune_stats_query", ...communeParams });
      const [fallbackCommuneRows] = await queryWithTimeout<[Array<{ avg_price_per_m2?: number; newest_sale_date?: string | null; latest_sale_date?: string | null; sale_date?: string | null }> ]>(
        {
          query: fallbackCommuneStatsQuery,
          params: communeParams,
        },
        "fallback_commune_stats_query"
      );
      console.log("[FR_GOLD] after_fallback_query", { level: "commune_stats", rows: (fallbackCommuneRows as any[])?.length ?? 0 });
      console.log("[FR_SQL] query_ok=true");
      console.log("[FR_SQL] rows_count=", (fallbackCommuneRows as any[])?.length ?? 0);
      console.log("[FR_SQL] columns_detected=", Object.keys(communeTableInspection.sampleRow ?? {}));

      let communeRows = fallbackCommuneRows as Array<{ avg_price_per_m2?: number; newest_sale_date?: string | null; latest_sale_date?: string | null; sale_date?: string | null }>;
      let communeFallbackRowsCount = communeRows.length;
      let communeUsableAvgRows = communeRows.filter((r) => {
        const v = parseMaybeDecimal(r.avg_price_per_m2);
        return v != null && v > 0;
      });
      let communeUsableAvgRowsCount = communeUsableAvgRows.length;

      if (communeFallbackRowsCount === 0 && postcodeNorm && cityNorm) {
        try {
          const factsCommuneQuery = `
            SELECT price_per_m2
            FROM \`streetiq-bigquery.streetiq_gold.property_latest_facts\`
            WHERE LOWER(TRIM(country)) = LOWER(TRIM(@country))
              AND LOWER(TRIM(city)) = LOWER(TRIM(@city))
              AND ${frBqPostcodeMatchSql}
            LIMIT 300
          `;
          const [factsCommuneRows] = await queryWithTimeout<[Array<{ price_per_m2?: unknown }>]>(
            {
              query: factsCommuneQuery,
              params: { country: country || "", city: cityNormForSource || cityNorm || "", postcode: postcodeNormForSource || "" },
            },
            "commune_from_facts_fallback"
          );
          const communePpmEuros = (factsCommuneRows ?? [])
            .map((r) => frPropertyLatestFactsMoneyToEuros(r.price_per_m2))
            .filter((v): v is number => v != null && v > 0);
          if (communePpmEuros.length >= 1) {
            const medianCommunePpm = medianNumber(communePpmEuros) ?? 0;
            communeRows = [{ avg_price_per_m2: medianCommunePpm * 100 }];
            communeFallbackRowsCount = 1;
            communeUsableAvgRows = communeRows;
            communeUsableAvgRowsCount = 1;
            console.log("[FR_FLOW] commune_from_facts_fallback rows=" + String(communePpmEuros.length) + " median_ppm=" + String(medianCommunePpm));
          }
        } catch (e) {
          console.log("[FR_FLOW] commune_from_facts_fallback_error", (e as Error)?.message);
        }
      }

      console.log("[FR_DEBUG] commune_fallback_matching", {
        submittedLot: aptNumber?.trim() || null,
        normalizedLot: normalizedRequestedLot,
        communeFallbackRowsCount,
        communeUsableAvgRowsCount,
        surfaceForEstimation,
      });
      console.log("[FR_STEP] commune_lookup_done");

      frRuntimeDebug.commune_rows_count = communeFallbackRowsCount;
      frRuntimeDebug.commune_usable_rows_count = communeUsableAvgRowsCount;
      frRuntimeDebug.fr_source_lookup_commune_count = communeFallbackRowsCount;

      if (communeUsableAvgRowsCount > 0) {
        const avgPriceValues = communeUsableAvgRows
          .map((r) => parseMaybeDecimal(r.avg_price_per_m2))
          .filter((v): v is number => v != null && v > 0);
        const medianAvgPricePerM2 = medianNumber(avgPriceValues);
        if (medianAvgPricePerM2 != null && Number.isFinite(medianAvgPricePerM2) && medianAvgPricePerM2 > 0) {
          const medianAvgPricePerM2Euro = medianAvgPricePerM2 / 100;
          const estimated =
            surfaceForEstimation != null ? Math.round(surfaceForEstimation * medianAvgPricePerM2Euro) : null;
          // Avoid forcing "No reliable building value available" when we do not have surface.
          const frResultType = estimated == null ? "nearby_comparable" : "building_level";
          const frConfidence = "low";
          console.log("[FR_DEBUG] winning_valuation_step", {
            winningValuationStep: "commune_fallback",
            winningSourceLabel: fallbackSourceCommune,
          });
          frRuntimeDebug.winning_step = "commune_fallback";
          frRuntimeDebug.winning_source_label = fallbackSourceCommune;
          frRuntimeDebug.has_surface_for_estimate = surfaceForEstimation != null;
          frRuntimeDebug.chosen_surface_value = surfaceForEstimation;
          frRuntimeDebug.winning_median_price_per_m2 = medianAvgPricePerM2Euro;
          console.log(
            "[FR_FLOW] valuation_ladder_complete tag=valuation_response branch=COMMUNE (EXACT+BUILDING+STREET+COMMUNE ran)"
          );
          return frReturn({
            address: { city: cityNorm, street: streetNorm, house_number: houseNumberNorm },
            data_source: "properties_france",
            fr_detect: frDetectToUse,
            property_result: {
              exact_value: estimated,
              exact_value_message: null,
              value_level: "street-level",
              last_transaction: {
                amount: 0,
                date: null,
                message: "No exact recent transaction available",
              },
              street_average: medianAvgPricePerM2Euro,
            street_average_message:
              surfaceForEstimation == null
                ? `${fallbackSourceCommune} — comparable pricing available, but exact estimate could not be computed (missing surface).`
                : fallbackSourceCommune,
              livability_rating: "FAIR",
            },
            fr: emptyFranceResponse({
              success: true,
              resultType: frResultType as any,
              confidence: frConfidence as any,
              requestedLot: requestedLotNorm,
              normalizedLot: normalizedRequestedLot,
              property: {
                transactionDate: null,
                transactionValue: estimated,
                pricePerSqm: medianAvgPricePerM2Euro,
                surfaceArea: surfaceForEstimation ?? null,
                rooms: null,
                propertyType: propertyType,
                building: `${houseNumberNorm} ${streetNorm}`.trim() || null,
                postalCode: postcodeNorm || null,
                commune: cityNorm || null,
              },
              buildingStats: null,
              comparables: [],
              matchExplanation:
                surfaceForEstimation == null
                  ? `${fallbackSourceCommune} — comparable pricing available, but exact estimate could not be computed (missing surface).`
                  : fallbackSourceCommune,
            }),
          }, "valuation_response");
        }
      }

      // NEARBY fallback: same postcode, nearby streets/addresses — before no_data
      const tryNearbyFallback = async (): Promise<ReturnType<typeof frReturn> | null> => {
        if (!postcodeNorm || !cityNorm) return null;
        const isHouse = detectClass === "house";
        const isApartment = detectClass === "apartment";
        const tryBoth = detectClass === "unclear";

        const houseLikeFilter = `(
          LOWER(TRIM(CAST(property_type AS STRING))) LIKE '%maison%'
          OR LOWER(TRIM(CAST(property_type AS STRING))) LIKE '%villa%'
          OR LOWER(TRIM(CAST(property_type AS STRING))) LIKE '%pavillon%'
          OR LOWER(TRIM(CAST(property_type AS STRING))) = 'local'
          OR TRIM(CAST(property_type AS STRING)) = ''
        )`;
        const apartmentFilter = `LOWER(TRIM(CAST(property_type AS STRING))) = 'appartement'`;

        const runNearbyQuery = async (filter: string) => {
          const q = `
            SELECT price_per_m2, surface_m2, street, house_number, last_sale_date, city
            FROM \`streetiq-bigquery.streetiq_gold.property_latest_facts\`
            WHERE LOWER(TRIM(country)) = LOWER(TRIM(@country))
              AND ${frBqPostcodeMatchSql}
              AND ${filter}
            LIMIT 400
          `;
          return queryWithTimeout<[
            Array<{ price_per_m2?: unknown; street?: unknown; house_number?: unknown; last_sale_date?: unknown; city?: unknown }>
          ]>({ query: q, params: { country: country || "", postcode: postcodeNormForSource } }, "nearby_fallback_query");
        };

        try {
          let usedHouseFilter = isHouse || tryBoth;
          let [nearbyRows] = await runNearbyQuery(usedHouseFilter ? houseLikeFilter : apartmentFilter);
          if (tryBoth && (nearbyRows ?? []).length === 0) {
            [nearbyRows] = await runNearbyQuery(apartmentFilter);
            usedHouseFilter = false;
          }

          const rows = (nearbyRows ?? []) as Array<{
            price_per_m2?: unknown;
            street?: unknown;
            house_number?: unknown;
            last_sale_date?: unknown;
            city?: unknown;
          }>;
          const withPpm = rows
            .map((r) => {
              const ppm = frPropertyLatestFactsMoneyToEuros(r.price_per_m2);
              const streetRaw = String(r.street ?? "").trim().toUpperCase().normalize("NFD").replace(/\p{M}/gu, "").replace(/[^A-Z0-9 ]/g, " ").replace(/\s+/g, " ").trim();
              const streetNormRow = streetRaw.replace(/^(RUE|AVENUE|AV|BD|BOULEVARD|CHEMIN|CHE|ROUTE|IMPASSE|IMP|ALLEE|ALL|PLACE|PL|SQUARE|SQ|SENTE|COURS|PROMENADE|PROM)\.?\s+/i, "").trim();
              const streetMatch = streetNormForSource && (streetNormRow.includes(streetNormForSource) || streetNormForSource.includes(streetNormRow) || streetNormRow === streetNormForSource);
              const cityMatch = cityNorm && frCityMatches(cityNorm, String(r.city ?? "").trim());
              const hnRaw = r.house_number;
              const hnNum = extractHouseNumberNumeric(String(hnRaw ?? ""));
              const houseDist = houseNumberNumericTarget != null && hnNum != null ? Math.abs(hnNum - houseNumberNumericTarget) : 999;
              const dateStr = r.last_sale_date != null ? String(r.last_sale_date).trim() || null : null;
              return { ppm, streetMatch, cityMatch, houseDist, dateStr };
            })
            .filter((x): x is { ppm: number; streetMatch: boolean; cityMatch: boolean; houseDist: number; dateStr: string | null } => x.ppm != null && x.ppm > 0);

          if (withPpm.length < 1) {
            console.log("[FR_NEARBY] nearby_rows_found=0");
            return null;
          }

          withPpm.sort((a, b) => {
            if (a.streetMatch !== b.streetMatch) return a.streetMatch ? -1 : 1;
            if (a.houseDist !== b.houseDist) return a.houseDist - b.houseDist;
            const ta = a.dateStr ? new Date(a.dateStr).getTime() : 0;
            const tb = b.dateStr ? new Date(b.dateStr).getTime() : 0;
            return tb - ta;
          });

          const pool = withPpm.slice(0, Math.min(50, withPpm.length));
          const ppmValues = pool.map((x) => x.ppm);
          const ppmForMedian = ppmValues.length >= 5 ? frTrimFractionExtremes(ppmValues.map((p) => ({ p })), (x) => x.p, 0.1).map((x) => x.p) : ppmValues;
          const medianPpm = medianNumber(ppmForMedian) ?? ppmValues[0];
          if (medianPpm == null || medianPpm <= 0) return null;

          const sameStreetCount = withPpm.filter((x) => x.streetMatch).length;
          const nearbyScope =
            sameStreetCount >= pool.length ? "same_street" : sameStreetCount > 0 ? "nearby_street" : "same_postcode";

          const surfaceForEst = validInputSurfaceM2 ?? medianSurfaceM2ForFallback;
          const estimated = surfaceForEst != null && surfaceForEst > 0 ? Math.round(surfaceForEst * medianPpm) : null;

          const labelHouse = "Based on nearby similar houses";
          const labelApt = "Based on nearby similar apartments";
          const label = (tryBoth ? usedHouseFilter : isHouse) ? labelHouse : labelApt;
          const propType = (tryBoth ? usedHouseFilter : isHouse) ? "Maison" : "Appartement";
          const conf: "low" | "medium" = pool.length >= 5 ? "medium" : "low";

          console.log("[FR_NEARBY] detect_class=" + String(detectClass));
          console.log("[FR_NEARBY] nearby_rows_found=" + String(withPpm.length));
          console.log("[FR_NEARBY] nearby_rows_used=" + String(pool.length));
          console.log("[FR_NEARBY] nearby_scope=" + nearbyScope);
          console.log("[FR_NEARBY] selected_price_per_m2=" + String(medianPpm));
          console.log("[FR_NEARBY] selected_reason=" + (nearbyScope === "same_street" ? "same_street" : nearbyScope === "nearby_street" ? "nearby_street_with_same_street" : "same_postcode_only"));

          frRuntimeDebug.winning_step = "nearby_fallback";
          frRuntimeDebug.winning_source_label = label;
          frRuntimeDebug.winning_median_price_per_m2 = medianPpm;

          return frReturn(
            {
              address: { city: cityNorm, street: streetNorm, house_number: houseNumberNorm },
              data_source: "properties_france",
              fr_detect: frDetectToUse,
              property_result: {
                exact_value: estimated,
                exact_value_message: estimated == null ? "Surface needed for total estimate" : null,
                value_level: "street-level",
                last_transaction: { amount: 0, date: null, message: "No exact recent transaction available" },
                street_average: medianPpm,
                street_average_message: label,
                livability_rating: "FAIR",
              },
              fr: emptyFranceResponse({
                success: true,
                resultType: "nearby_comparable",
                confidence: conf,
                requestedLot: requestedLotNorm,
                normalizedLot: normalizedRequestedLot,
                property: {
                  transactionDate: null,
                  transactionValue: estimated,
                  pricePerSqm: medianPpm,
                  surfaceArea: surfaceForEst ?? null,
                  rooms: null,
                  propertyType: propType,
                  building: `${houseNumberNorm} ${streetNorm}`.trim() || null,
                  postalCode: postcodeNorm || null,
                  commune: cityNorm || null,
                },
                buildingStats: null,
                comparables: [],
                matchExplanation: label,
              }),
            },
            "valuation_response"
          );
        } catch (e) {
          console.log("[FR_NEARBY] query_error", (e as Error)?.message);
          return null;
        }
      };

      const nearbyWin = await tryNearbyFallback();
      if (nearbyWin) return nearbyWin;

      const noDataReasonParts: string[] = [];
      if (exactApartmentRowsCount <= 0) noDataReasonParts.push("no_exact_lot_rows");
      if (exactApartmentRowsCount > 0 && exactUsableRowsCount <= 0) noDataReasonParts.push("exact_lot_rows_not_usable");
      if (sameBuildingUsableRowsCount < MIN_SAME_BUILDING_USABLE_ROWS) noDataReasonParts.push("same_building_insufficient_usable_rows");
      if (streetUsableAvgRowsCount <= 0) noDataReasonParts.push("no_usable_street_avg_price");
      if (communeUsableAvgRowsCount <= 0) noDataReasonParts.push("no_usable_commune_avg_price");

      const exactHouseCandidates = Number(frRuntimeDebug.exact_house_row_count) || 0;
      console.log("[FR_LADDER] exact=" + String(exactHouseCandidates || exactApartmentRowsCount) + " building=" + String(sameBuildingUsableRowsCount) + " street=" + String(streetUsableAvgRowsCount) + " commune=" + String(communeUsableAvgRowsCount));
      const buildingCandidates = frRuntimeDebug.building_similar_unit_candidates_count ?? sameBuildingRowsCount;
      const streetCandidates = streetFallbackRowsCount;
      const communeCandidates = communeFallbackRowsCount;
      const noReliableReason = noDataReasonParts.join(" | ") || "unknown";
      const rejectReason = frRuntimeDebug.exact_house_reject_reason ?? frRuntimeDebug.exact_reject_reason ?? frRuntimeDebug.building_similar_unit_reject_reason ?? "";
      const addrStr = [houseNumberNorm, streetNorm, cityNorm, postcodeNorm].filter(Boolean).join(", ");
      console.log("[FR_COVERAGE] address=" + addrStr);
      console.log("[FR_COVERAGE] detect_class=" + String(frDetectToUse));
      console.log("[FR_COVERAGE] exact_house_candidates=" + String(exactHouseCandidates));
      console.log("[FR_COVERAGE] building_candidates=" + String(buildingCandidates));
      console.log("[FR_COVERAGE] street_candidates=" + String(streetCandidates));
      console.log("[FR_COVERAGE] commune_candidates=" + String(communeCandidates));
      console.log("[FR_COVERAGE] no_reliable_reason=" + noReliableReason);
      console.log("[FR_COVERAGE] reject_reason=" + String(rejectReason));

      console.log("[FR_DEBUG] no_data_why", {
        exactApartmentRowsCount,
        exactUsableRowsCount,
        sameBuildingUsableRowsCount,
        streetFallbackRowsCount,
        streetUsableAvgRowsCount,
        communeFallbackRowsCount: communeRows?.length ?? 0,
        communeUsableAvgRowsCount,
        surfaceForEstimation,
        winningValuationStep: null,
        winningSourceLabel: null,
        noDataReason: noDataReasonParts.join(" | ") || "unknown",
      });

      const anyUsableRowSignal =
        exactUsableRowsCount > 0 ||
        sameBuildingUsableRowsCount > 0 ||
        streetUsableAvgRowsCount > 0 ||
        communeUsableAvgRowsCount > 0;
      if (anyUsableRowSignal) {
        console.error("[FR_FLOW] unexpected_no_data_despite_usable_rows", {
          exactUsableRowsCount,
          sameBuildingUsableRowsCount,
          streetUsableAvgRowsCount,
          communeUsableAvgRowsCount,
        });
      }

      const banMatchedForTerminal = Boolean(frRuntimeDebug.ban_match_found);
      const noValuationWinnerTerminal = !anyUsableRowSignal;
      const terminalNoDataTag: "fallback_match" | "no_data" =
        banMatchedForTerminal && noValuationWinnerTerminal ? "fallback_match" : "no_data";

      if (terminalNoDataTag === "fallback_match") {
        frRuntimeDebug.winning_step = null;
        frRuntimeDebug.winning_source_label = null;
      } else {
        frRuntimeDebug.winning_step = "no_data";
        frRuntimeDebug.winning_source_label = "No reliable data found";
        const failedStage =
          !cityNorm && !streetNorm && !postcodeNorm ? "parse"
          : !frRuntimeDebug.ban_match_found ? "ban_selection"
          : exactHouseCandidates <= 0 && exactApartmentRowsCount <= 0 && sameBuildingUsableRowsCount <= 0 ? "source_lookup"
          : "valuation";
        frRuntimeDebug.fr_failed_stage = failedStage;
        console.log("[FR_FAIL] no_data_reason=" + (noDataReasonParts.join(" | ") || "unknown"));
        console.log("[FR_FAIL] failed_stage=" + failedStage);
      }
      frRuntimeDebug.has_surface_for_estimate = surfaceForEstimation != null;
      frRuntimeDebug.chosen_surface_value = surfaceForEstimation;
      frRuntimeDebug.no_data_reason = noDataReasonParts.join(" | ") || "unknown";

      // Req 3: If apartment + no lot and we'd return no_data, prompt for lot instead.
      if (detectClass === "apartment" && !submittedLotPresent) {
        console.log("[FR_GOLD] apartment_lot_prompt_triggered_at_no_data");
        return frReturn(
          {
            address: { city: cityNorm, street: streetNorm, house_number: houseNumberNorm },
            data_source: "properties_france",
            multiple_units: true,
            prompt_for_apartment: true,
            available_lots: candidateLots,
            property_result: {
              exact_value: null,
              exact_value_message: "Enter apartment/lot for a more precise result.",
              value_level: "building-level",
              last_transaction: { amount: 0, date: null, message: "No recent transaction available" },
              street_average: null,
              street_average_message: "No reliable data found",
              livability_rating: "FAIR",
            },
            fr_detect: detectClass,
            fr: emptyFranceResponse({
              success: true,
              resultType: "building_level",
              confidence: "medium",
              requestedLot: requestedLotNorm,
              normalizedLot: normalizedRequestedLot,
              property: null,
              buildingStats: {
                transactionCount: Math.max(candidateLots.length, isMultiUnitDetected ? 2 : 1),
                avgPricePerSqm: null,
                avgTransactionValue: null,
              },
              comparables: [],
              matchExplanation: "Apartment-like building detected. Enter lot/apartment for precise valuation.",
            }),
          },
          "prompt_lot_first"
        );
      }

      return frReturn({
        address: { city: cityNorm, street: streetNorm, house_number: houseNumberNorm },
        data_source: "properties_france",
        fr_detect: frDetectToUse,
        property_result: {
          exact_value: null,
          exact_value_message: "No reliable data found",
          value_level: "no_match",
          last_transaction: { amount: 0, date: null, message: "No recent transaction available" },
          street_average: null,
          street_average_message: "No reliable data found",
          livability_rating: "FAIR",
        },
        fr: emptyFranceResponse({
          success: false,
          resultType: "no_result",
          confidence: "low",
          requestedLot: requestedLotNorm,
          normalizedLot: normalizedRequestedLot,
          property: null,
          buildingStats: null,
          comparables: [],
          matchExplanation: "No reliable data found",
        }),
      }, terminalNoDataTag);
    } catch (err) {
      console.log("[FR_STEP] returning_error");
      console.error("[FR_FATAL]", err);
      console.log("[FR_GOLD] catch_error", { message: err instanceof Error ? err.message : "Unknown error" });
      const fatalMessage = err instanceof Error ? err.message : "Unknown error";
      const fatalStackFirstLine =
        err instanceof Error && typeof err.stack === "string"
          ? (err.stack.split("\n")[0] ?? null)
          : null;
      const catchRawInput = (searchParams.get("address") || searchParams.get("rawInputAddress") || "").trim();
      const payload = {
        message: "Failed to fetch France property value",
        error: fatalMessage,
        fr_detect: "unclear",
        fr_runtime_debug: {
          fatal_error_message: fatalMessage,
          fatal_error_stack_first_line: fatalStackFirstLine,
          request_url_seen_by_api: request.url,
          raw_apt_number_param: searchParams.get("apt_number"),
          raw_aptNumber_param: searchParams.get("aptNumber"),
          submitted_lot: (() => {
            const l = normalizeLot(aptNumber) || null;
            return l ? (l.replace(/^0+/, "") || l) : null;
          })(),
          ban_city: null,
          ban_postcode: null,
          ban_street: null,
          ban_house_number: null,
          fr_raw_input: catchRawInput || null,
          fr_address_param: searchParams.get("address") || null,
          fr_full_raw_address: searchParams.get("rawInputAddress") || searchParams.get("address") || catchRawInput || null,
          fr_parser_started: false,
          fr_parsed_house_number: null,
          fr_parsed_street: null,
          fr_parsed_postcode: null,
          fr_parsed_city: null,
          fr_ban_query_mode: "(none)",
          fr_ban_attempt_count: 0,
          fr_raw_postcode_token: null,
          fr_postcode_mismatch_rejections: 0,
          fr_typed_street_normalized: null,
          fr_ban_candidate_count: 0,
          fr_ban_selected_street_score: null,
          fr_ban_selected_reason: "fatal_error",
          fr_ban_top_candidates_summary: null,
          fr_cache_hit: false,
          fr_cache_bypass_reason: null,
          fr_failed_stage: "fatal_error",
        },
        fr: emptyFranceResponse({
          success: false,
          resultType: "no_result",
          confidence: "low",
          requestedLot: normalizeLot(aptNumber) || null,
          normalizedLot: (() => {
            const l = normalizeLot(aptNumber) || null;
            return l ? (l.replace(/^0+/, "") || l) : null;
          })(),
          property: null,
          buildingStats: null,
          comparables: [],
          matchExplanation: "No reliable data found",
        }),
      };
      console.log("[FR_GOLD] return", { tag: "error", status: 500 });
      return NextResponse.json(payload, { status: 500 });
    }

    const frRequestStartedAt = Date.now();
    const frErrorCacheKey = `fr_err:${cacheKey}${aptNumber ? `|apt:${aptNumber}` : ""}`;
    const frCached = FR_ERROR_CACHE.get(frErrorCacheKey);
    const frCachedData = frCached?.data;
    const frCachedTs = frCached?.ts;
    const frCachedAge = frCachedTs == null ? Number.POSITIVE_INFINITY : Date.now() - frCachedTs!;
    if (frCachedData && frCachedAge < FR_ERROR_CACHE_TTL_MS) {
      return NextResponse.json(frCachedData);
    }

    const frEmptyResponse = (): Record<string, unknown> => ({
      message: "No government data for this address",
      data_source: "properties_france",
      address: { city: city.trim(), street: street.trim(), house_number: houseNumber.trim() },
      property_result: {
        exact_value: null,
        exact_value_message: "No government data for this address",
        value_level: "no_match",
        last_transaction: { amount: 0, date: null, message: "No DVF data" },
        street_average: null,
        street_average_message: "No data for this address",
        livability_rating: "FAIR",
      },
    });
    const cacheAndReturn = (data: Record<string, unknown>) => {
      FR_ERROR_CACHE.set(frErrorCacheKey, { data, ts: Date.now() });
      return NextResponse.json(data);
    };

    try {
      if (!isBigQueryConfigured()) {
        console.log("BIGQUERY_FETCH_FAILED: [France] Missing BigQuery credentials");
        return NextResponse.json({
          message: "No government data for this address",
          data_source: "properties_france",
          address: { city: city.trim(), street: street.trim(), house_number: houseNumber.trim() },
          property_result: {
            exact_value: null,
            exact_value_message: "No government data for this address",
            value_level: "no_match",
            last_transaction: { amount: 0, date: null, message: "No DVF data" },
            street_average: null,
            street_average_message: "No data for this address",
            livability_rating: "FAIR",
          },
        });
      }

      const fullAddress = (addressParam && addressParam.trim()) || [houseNumber, street, city].filter(Boolean).join(", ");
      const parsedFR = parseFRAddressFromFullString(fullAddress);
      let houseNumTrim = (parsedFR.houseNumber || houseNumber || "").trim();
      if (!houseNumTrim) {
        const matchedHouseNum = fullAddress.match(/\b(\d{1,3})(?!\d)\b/)?.[1] ?? "";
        if (matchedHouseNum !== "") {
          houseNumTrim = matchedHouseNum;
        }
      }
      const aptTrimmed = normalizeLot(aptNumber);

      let codePostal = (parsedFR.postcode || postcode || zip || "").trim();
      if (!codePostal) {
        const fiveDigit = fullAddress.match(/\b(\d{5})\b/)?.[1] ?? "";
        const fourDigit = fullAddress.match(/\b(\d{4})\b/)?.[1] ?? "";
        if (fiveDigit) codePostal = fiveDigit;
        else if (fourDigit) codePostal = "0" + fourDigit;
      }
      if (!codePostal && /anglais|promenade|prom\s/i.test((parsedFR.street || street || ""))) {
        codePostal = "06000";
      }
      codePostal = codePostal.trim();
      if (codePostal.length === 4 && !codePostal.startsWith("0")) {
        codePostal = "0" + codePostal;
      }

      if (!codePostal) {
        const legacy = {
          message: "Postcode required for France search (e.g. Anatole France, 10000 Troyes).",
          data_source: "properties_france",
          address: { city: city.trim(), street: street.trim(), house_number: houseNumber.trim() },
          property_result: {
            exact_value: null,
            exact_value_message: "Postcode required",
            value_level: "no_match",
            last_transaction: { amount: 0, date: null, message: "No DVF data" },
            street_average: null,
            street_average_message: "Include postcode (e.g. 10000)",
            livability_rating: "FAIR",
          },
        };
        const requestedLot = aptNumber ? normalizeLot(aptNumber) || null : null;
        const normalizedLot = requestedLot ? (String(requestedLot).replace(/^0+/, "") || String(requestedLot)) : null;
        const fr: FrancePropertyResponse = emptyFranceResponse({
          success: false,
          resultType: "no_result",
          confidence: "low",
          requestedLot,
          normalizedLot,
          debug: {
            searchedAddress: fullAddress,
            normalizedAddress: undefined,
            requestedLot,
            normalizedLot,
            selectedResultType: "no_result",
            usedFallback: false,
            failureReason: "postcode_required",
            queryDurationMs: Date.now() - frRequestStartedAt,
          },
        });
        return NextResponse.json({ ...legacy, fr });
      }

      const voie = (parsedFR.street || street || "").trim() || null;
      const commune = (parsedFR.city || city || "").trim() || null;

      const result = await getFrancePropertyResult(
        codePostal,
        houseNumTrim || null,
        voie,
        commune,
        aptTrimmed || null,
        null
      );

      const livabilityRating = mapLivabilityToRating(result.livabilityStandard);
      const requestedLot = aptTrimmed || null;
      const exactLotRowCount = result.exactLotRowCount ?? 0;
      const exactLotMatched = !!(requestedLot && exactLotRowCount > 0);
      console.log("[property-value] France lot decision:", JSON.stringify({
        requestedLot: requestedLot ?? "(none)",
        exactLotMatched,
        exactLotRowCount,
        matchStage: result.matchStage,
        rowsAtStage: result.rowsAtStage,
        serviceResultLevel: result.resultLevel,
      }));

      if (result.multipleUnits) {
        const scaledBuildingSales = mapFranceBuildingSalesPricesToEuros(result.buildingSales ?? []);
        const averageBuildingEuro = frDvfMoneyCentsToEuros(result.averageBuildingValue) ?? 0;
        const payload = {
          address: { city: city.trim(), street: street.trim(), house_number: houseNumber.trim() },
          data_source: "properties_france",
          multiple_units: true,
          prompt_for_apartment: true,
          result_level: "building",
          match_stage: result.matchStage,
          rows_at_stage: result.rowsAtStage,
          average_building_value: averageBuildingEuro,
          unit_count: result.unitCount ?? 0,
          building_sales: scaledBuildingSales,
          available_lots: result.availableLots ?? [],
          property_result: {
            exact_value: null,
            exact_value_message: "Multiple units found. Please enter apartment number to see exact value.",
            value_level: "building-level",
            last_transaction: { amount: 0, date: null, message: "No DVF data" },
            street_average: frDvfMoneyCentsToEuros(result.averageBuildingValue),
            street_average_message: result.unitCount != null ? (result.unitCount === 1 ? "1 unit in this building" : `Average of ${result.unitCount} units in this building`) : null,
            livability_rating: livabilityRating,
          },
        };
        console.log("[property-value] France final payload (multiple_units):", JSON.stringify({ result_level: payload.result_level, multiple_units: payload.multiple_units, match_stage: payload.match_stage, rows_at_stage: payload.rows_at_stage, average_building_value: payload.average_building_value, building_sales_count: payload.building_sales?.length ?? 0 }));
        const requestedLot = aptTrimmed || null;
        const normalizedLot = requestedLot ? (String(requestedLot).replace(/^0+/, "") || String(requestedLot)) : null;
        const fr: FrancePropertyResponse = emptyFranceResponse({
          success: true,
          resultType: "building_level",
          confidence: "medium",
          matchedAddress: `${houseNumTrim || ""} ${(voie ?? "").trim()}`.trim() || null,
          normalizedAddress: undefined,
          requestedLot,
          normalizedLot,
          property: null,
          buildingStats: {
            transactionCount: Array.isArray(payload.building_sales) ? payload.building_sales.length : 0,
            avgPricePerSqm: null,
            avgTransactionValue: typeof payload.average_building_value === "number" ? payload.average_building_value : null,
          },
          comparables: (payload.building_sales ?? []).map((c: any) => ({
            date: c?.date ?? null,
            type: String(c?.type ?? ""),
            price: Number(c?.price ?? 0) || 0,
            surface: c?.surface ?? null,
            lot_number: c?.lot_number ?? null,
          })),
          debug: {
            searchedAddress: fullAddress,
            normalizedAddress: undefined,
            requestedLot,
            normalizedLot,
            exactLotRowCount: exactLotRowCount,
            buildingRowCount: Array.isArray(payload.building_sales) ? payload.building_sales.length : 0,
            comparableRowCount: 0,
            selectedResultType: "building_level",
            usedFallback: true,
            failureReason: null,
            queryDurationMs: Date.now() - frRequestStartedAt,
          },
        });
        return NextResponse.json({ ...payload, fr });
      }

      // If user requested a lot and we have exact lot rows, return ONLY apartment-specific data.
      // Do not label a building-level payload as exact_property.
      if (exactLotMatched) {
        const lastTx = result.lastTransaction;
        const exactValue = frDvfMoneyCentsToEuros(result.currentValue ?? lastTx?.value ?? null);
        const lastTxAmountEuro = frDvfMoneyCentsToEuros(lastTx?.value) ?? 0;
        const payload = {
          address: { city: city.trim(), street: street.trim(), house_number: houseNumber.trim() },
          data_source: "properties_france",
          multiple_units: false,
          prompt_for_apartment: false,
          apartment_not_matched: false,
          result_level: "exact_property" as const,
          lot_number: result.lotNumber,
          surface_reelle_bati: result.surfaceReelleBati,
          date_mutation: lastTx?.date ?? null,
          match_stage: result.matchStage,
          rows_at_stage: exactLotRowCount,
          exact_lot_row_count: exactLotRowCount,
          property_result: {
            exact_value: exactValue,
            exact_value_message: exactValue == null ? "No recorded transaction for this apartment" : null,
            value_level: "property-level" as const,
            last_transaction: {
              amount: lastTxAmountEuro,
              date: lastTx?.date ?? null,
              message: lastTxAmountEuro > 0 ? undefined : "No recorded transaction",
            },
            street_average: frDvfMoneyCentsToEuros(result.areaAverageValue),
            street_average_message: result.areaAverageValue == null ? "No DVF data for this area" : null,
            livability_rating: livabilityRating,
          },
        };
        console.log("[property-value] France final payload (exact_property):", JSON.stringify({
          requestedLot: requestedLot ?? "(none)",
          exactLotRowCount,
          result_level: payload.result_level,
          value_level: payload.property_result.value_level,
        }));
        const normalizedLot = requestedLot ? (String(requestedLot).replace(/^0+/, "") || String(requestedLot)) : null;
        const fr: FrancePropertyResponse = emptyFranceResponse({
          success: true,
          resultType: "exact_apartment",
          confidence: "high",
          matchedAddress: `${houseNumTrim || ""} ${(voie ?? "").trim()}`.trim() || null,
          normalizedAddress: undefined,
          requestedLot,
          normalizedLot,
          property: {
            transactionDate: payload.date_mutation ?? null,
            transactionValue: payload.property_result?.last_transaction?.amount ?? null,
            pricePerSqm:
              payload.surface_reelle_bati && payload.property_result?.last_transaction?.amount
                ? Math.round((payload.property_result.last_transaction.amount / (payload.surface_reelle_bati ?? 1)) * 100) / 100
                : null,
            surfaceArea: payload.surface_reelle_bati ?? null,
            rooms: null,
            propertyType: payload.property_result?.value_level ? null : null,
            building: `${houseNumTrim || ""} ${(voie ?? "").trim()}`.trim() || null,
            postalCode: codePostal ?? null,
            commune: commune ?? null,
          },
          buildingStats: null,
          comparables: [],
          debug: {
            searchedAddress: fullAddress,
            normalizedAddress: undefined,
            requestedLot,
            normalizedLot,
            exactLotRowCount,
            buildingRowCount: Array.isArray(result.buildingSales) ? result.buildingSales.length : 0,
            comparableRowCount: 0,
            selectedResultType: "exact_apartment",
            usedFallback: false,
            failureReason: null,
            queryDurationMs: Date.now() - frRequestStartedAt,
          },
        });
        return NextResponse.json({ ...payload, fr });
      }

      if (result.apartmentNotMatched || requestedLot) {
        const buildingVal =
          frDvfMoneyCentsToEuros(result.averageBuildingValue ?? result.currentValue ?? null) ?? 0;
        const payload = {
          address: { city: city.trim(), street: street.trim(), house_number: houseNumber.trim() },
          data_source: "properties_france",
          multiple_units: false,
          prompt_for_apartment: true,
          apartment_not_matched: true,
          average_building_value: buildingVal,
          building_sales: mapFranceBuildingSalesPricesToEuros(result.buildingSales ?? []),
          available_lots: result.availableLots ?? [],
          match_stage: result.matchStage,
          result_level: "building",
          rows_at_stage: result.rowsAtStage,
          exact_lot_row_count: exactLotRowCount,
          property_result: {
            exact_value: buildingVal,
            exact_value_message: `Apartment/lot ${aptTrimmed} was not found in DVF data. Showing building-level estimate.`,
            value_level: "building-level",
            last_transaction: { amount: 0, date: null, message: "No recorded transaction for this apartment" },
            street_average: frDvfMoneyCentsToEuros(result.areaAverageValue),
            street_average_message: null,
            livability_rating: livabilityRating,
          },
        };
        console.log("[FR_DEBUG] final France payload", JSON.stringify({
          path: "apartment_not_matched",
          rawParams: {
            city: city.trim(),
            street: street.trim(),
            houseNumber: houseNumber.trim(),
            postcode: (postcode || zip || "").trim(),
            aptNumber: aptNumber?.trim() || "",
            countryCode: (countryCode ?? "").toUpperCase(),
          },
          legacy: {
            requestedLot: requestedLot ?? "(none)",
            exactLotRowCount,
            result_level: payload.result_level,
            value_level: payload.property_result.value_level,
            match_stage: payload.match_stage,
            rows_at_stage: payload.rows_at_stage,
            building_sales_count: payload.building_sales?.length ?? 0,
            available_lots_count: payload.available_lots?.length ?? 0,
          },
        }));
        const normalizedLot = requestedLot ? (String(requestedLot).replace(/^0+/, "") || String(requestedLot)) : null;
        const hasBuilding = (payload.building_sales?.length ?? 0) > 0 || (payload.average_building_value ?? 0) > 0;
        const sim = result.similarApartment ?? null;
        const nearby = result.nearbyComparable ?? null;
        const reliableSameBuildingCount = result.debug?.reliableCandidateCountSameBuilding ?? (sim ? 2 : 0);
        const reliableNearbyCount = result.debug?.reliableCandidateCountNearby ?? (nearby ? 2 : 0);

        const buildingSalesCount = Array.isArray(payload.building_sales) ? payload.building_sales.length : 0;
        const buildingHasDatedSale = Array.isArray(payload.building_sales)
          ? payload.building_sales.some((s: any) => typeof s?.date === "string" && s.date.trim())
          : false;
        const buildingHasSurface = Array.isArray(payload.building_sales)
          ? payload.building_sales.some((s: any) => typeof s?.surface === "number" && s.surface > 0)
          : false;
        const buildingAvg = typeof payload.average_building_value === "number" ? payload.average_building_value : 0;
        const buildingStrong =
          hasBuilding &&
          buildingSalesCount >= 2 &&
          buildingAvg > 0 &&
          buildingHasDatedSale &&
          buildingHasSurface;
        const buildingRejectedAsWeak = hasBuilding && !buildingStrong;

        const nearbyStrong =
          typeof nearby?.value === "number" &&
          (nearby?.value ?? 0) > 0 &&
          typeof nearby?.date === "string" &&
          (nearby?.date ?? "").trim().length > 0 &&
          typeof nearby?.surface === "number" &&
          (nearby?.surface ?? 0) > 0;

        const hasReliableComps = reliableSameBuildingCount >= 2 || reliableNearbyCount >= 2;
        const hasAnyTxData = !!sim || nearbyStrong || hasBuilding;
        const searchedLot = !!aptTrimmed;
        const noReliableComparableData = searchedLot && !sim && !nearbyStrong && !hasReliableComps;

        const resultType: FrancePropertyResponse["resultType"] =
          // Critical bug fix: if there's no matching building/apartment/nearby transaction data at all, it's a true no_result.
          !hasAnyTxData
            ? "no_result"
            : noReliableComparableData
              ? "no_reliable_data"
              : sim
                ? "similar_apartment_same_building"
                : buildingStrong
                  ? "building_level"
                  : (buildingRejectedAsWeak && nearbyStrong)
                    ? "nearby_comparable"
                    : hasBuilding
                      ? "building_level"
                      : nearbyStrong
                        ? "nearby_comparable"
                        : "no_result";
        const confidence: FrancePropertyResponse["confidence"] =
          resultType === "similar_apartment_same_building"
            ? "medium_high"
            : resultType === "building_level"
              ? "medium"
              : resultType === "nearby_comparable"
                ? (buildingRejectedAsWeak ? "low_medium" : "low_medium")
                : "low";

        const frProperty =
          resultType === "similar_apartment_same_building" && sim
            ? {
                transactionDate: sim?.date ?? null,
                transactionValue: frDvfMoneyCentsToEuros(sim?.value) ?? null,
                pricePerSqm: (() => {
                  const s = sim!;
                  if (s.surface == null || s.value == null) return null;
                  const v = frDvfMoneyCentsToEuros(s.value) ?? 0;
                  return v > 0 ? Math.round((v / (s.surface ?? 1)) * 100) / 100 : null;
                })(),
                surfaceArea: sim?.surface ?? null,
                rooms: null,
                propertyType: sim?.typeLocal ?? null,
                building: `${houseNumTrim || ""} ${(voie ?? "").trim()}`.trim() || null,
                postalCode: codePostal ?? null,
                commune: commune ?? null,
              }
            : resultType === "nearby_comparable" && nearby
              ? {
                  transactionDate: nearby?.date ?? null,
                  transactionValue: frDvfMoneyCentsToEuros(nearby?.value) ?? null,
                  pricePerSqm: (() => {
                    const n = nearby!;
                    if (n.surface == null || n.value == null) return null;
                    const v = frDvfMoneyCentsToEuros(n.value) ?? 0;
                    return v > 0 ? Math.round((v / (n.surface ?? 1)) * 100) / 100 : null;
                  })(),
                  surfaceArea: nearby?.surface ?? null,
                  rooms: null,
                  propertyType: nearby?.typeLocal ?? null,
                  building: null,
                  postalCode: codePostal ?? null,
                  commune: commune ?? null,
                }
              : hasBuilding
                ? {
                    transactionDate: null,
                    transactionValue: typeof payload.average_building_value === "number" ? payload.average_building_value : null,
                    pricePerSqm: null,
                    surfaceArea: null,
                    rooms: null,
                    propertyType: null,
                    building: `${houseNumTrim || ""} ${(voie ?? "").trim()}`.trim() || null,
                    postalCode: codePostal ?? null,
                    commune: commune ?? null,
                  }
                : null;

        // If result is explicitly "no_result" / "no_reliable_data", never return any price fields.
        const isNoData = resultType === "no_result" || resultType === "no_reliable_data";

        // Sanity check: extremely high price/m² is suspicious (e.g. bad valeur_fonciere/surface pairing).
        let finalResultType: FrancePropertyResponse["resultType"] = resultType;
        let finalConfidence: FrancePropertyResponse["confidence"] = confidence;
        const suspiciousPricePerSqm = (frProperty?.pricePerSqm ?? 0) > 50000;
        // Policy:
        // - exact_apartment: strict suppression to no_result when suspicious
        // - similar_apartment_same_building / nearby_comparable: warning mode (do NOT suppress)
        // NOTE: this branch is in the apartment-not-matched handler, so resultType here can never be "exact_apartment".
        const suppressForSuspiciousExact = false;
        const warnForSuspiciousNonExact =
          suspiciousPricePerSqm &&
          (resultType === "similar_apartment_same_building" || resultType === "nearby_comparable");
        if (suppressForSuspiciousExact) {
          finalResultType = "no_result";
          finalConfidence = "low";
        } else if (warnForSuspiciousNonExact) {
          finalConfidence = "low";
        }

        const fr: FrancePropertyResponse = emptyFranceResponse({
          success: finalResultType !== "no_result" && finalResultType !== "no_reliable_data",
          resultType: finalResultType,
          confidence: finalConfidence,
          matchedAddress: `${houseNumTrim || ""} ${(voie ?? "").trim()}`.trim() || null,
          normalizedAddress: undefined,
          requestedLot,
          normalizedLot,
          matchedLot: resultType === "similar_apartment_same_building" ? sim?.lotNumber ?? null : null,
          fallbackSource:
            resultType === "similar_apartment_same_building"
              ? "similar_same_building"
              : resultType === "building_level"
                ? "building_level"
                : resultType === "nearby_comparable"
                  ? "nearby_comparable"
                  : "none",
          fallbackReason: buildingRejectedAsWeak && resultType === "nearby_comparable"
            ? "Building data too weak — using nearby comparable"
            : "exact_lot_not_found",
          comparableAddress: resultType === "nearby_comparable" ? nearby?.address ?? null : null,
          comparableDistanceMeters: null,
          comparableScope: resultType === "nearby_comparable" ? nearby?.scope : undefined,
          selectedNearbyStrategy: resultType === "nearby_comparable" ? nearby?.selectedNearbyStrategy : undefined,
          matchExplanation:
            suppressForSuspiciousExact
              ? "Result suppressed due to suspiciously high price per m²."
              : warnForSuspiciousNonExact
                ? "This result was found, but the price per m² is unusually high and should be treated with caution."
              : resultType === "similar_apartment_same_building"
              ? `No exact apartment match for lot ${requestedLot ?? ""} — showing the most similar apartment in this building.`
              : resultType === "building_level"
                ? `No exact apartment match for lot ${requestedLot ?? ""} — showing building transaction data.`
                : resultType === "nearby_comparable"
                  ? (buildingRejectedAsWeak
                      ? `Building data was insufficient for this address — showing a nearby comparable instead.`
                      : `No exact apartment match for lot ${requestedLot ?? ""} — showing a similar nearby apartment.`)
                  : null,
          property: (suppressForSuspiciousExact || isNoData) ? null : frProperty,
          buildingStats: hasBuilding
            ? {
                transactionCount: Array.isArray(payload.building_sales) ? payload.building_sales.length : 0,
                avgPricePerSqm: null,
                avgTransactionValue: typeof payload.average_building_value === "number" ? payload.average_building_value : null,
              }
            : null,
          comparables: (payload.building_sales ?? []).map((c: any) => ({
            date: c?.date ?? null,
            type: String(c?.type ?? ""),
            price: Number(c?.price ?? 0) || 0,
            surface: c?.surface ?? null,
            lot_number: c?.lot_number ?? null,
          })),
          debug: {
            searchedAddress: fullAddress,
            normalizedAddress: undefined,
            requestedLot,
            normalizedLot,
            matchedLot: resultType === "similar_apartment_same_building" ? sim?.lotNumber ?? null : null,
            fallbackSource:
              resultType === "similar_apartment_same_building"
                ? "similar_same_building"
                : resultType === "building_level"
                  ? "building_level"
                  : resultType === "nearby_comparable"
                    ? "nearby_comparable"
                    : "none",
            fallbackReason: suppressForSuspiciousExact
              ? "suspicious_price_per_m2_suppressed"
              : warnForSuspiciousNonExact
                ? "suspicious_price_per_m2_warning"
              : buildingRejectedAsWeak && resultType === "nearby_comparable"
                ? "Building data too weak — using nearby comparable"
                : "exact_lot_not_found",
            comparableAddress: resultType === "nearby_comparable" ? nearby?.address ?? null : null,
            comparableDistanceMeters: null,
            comparableScope: resultType === "nearby_comparable" ? nearby?.scope : undefined,
            selectedNearbyStrategy: resultType === "nearby_comparable" ? nearby?.selectedNearbyStrategy : undefined,
            nearbyStageCounts: result.debug?.nearbyStageCounts,
            matchExplanation:
              resultType === "similar_apartment_same_building"
                ? `No exact apartment match for lot ${requestedLot ?? ""} — showing the most similar apartment in this building.`
                : resultType === "building_level"
                  ? `No exact apartment match for lot ${requestedLot ?? ""} — showing building transaction data.`
                  : resultType === "nearby_comparable"
                    ? (buildingRejectedAsWeak
                        ? `Building data too weak — using nearby comparable`
                        : `No exact apartment match for lot ${requestedLot ?? ""} — showing a similar nearby apartment.`)
                    : null,
            similarityScore: result.debug?.similarityScore ?? null,
            ...(suppressForSuspiciousExact || warnForSuspiciousNonExact
              ? {
                  suspiciousPricePerSqm: true,
                  suspiciousPricePerSqmValue: frProperty?.pricePerSqm ?? null,
                  suspiciousPolicy: suppressForSuspiciousExact ? "suppressed" : "warning",
                }
              : {}),
            candidateCountSameBuilding: result.debug?.candidateCountSameBuilding,
            candidateCountNearby: result.debug?.candidateCountNearby,
            ...(typeof reliableSameBuildingCount === "number" ? { reliableCandidateCountSameBuilding: reliableSameBuildingCount } : {}),
            ...(typeof reliableNearbyCount === "number" ? { reliableCandidateCountNearby: reliableNearbyCount } : {}),
            buildingRejectedAsWeak,
            ...(result.debug?.nearbyFilterStats ? { nearbyFilterStats: result.debug?.nearbyFilterStats as any } : {}),
            exactLotRowCount,
            buildingRowCount: Array.isArray(payload.building_sales) ? payload.building_sales.length : 0,
            comparableRowCount: resultType === "nearby_comparable" ? 1 : 0,
            selectedResultType: resultType,
            usedFallback: true,
            failureReason: resultType === "no_result" ? "no_trustworthy_fallback" : "apartment_not_matched",
            queryDurationMs: Date.now() - frRequestStartedAt,
          },
        });
        console.log("[FR_DEBUG] final France payload", JSON.stringify({
          path: "apartment_not_matched",
          rawParams: {
            city: city.trim(),
            street: street.trim(),
            houseNumber: houseNumber.trim(),
            postcode: codePostal ?? (postcode || zip || "").trim(),
            aptNumber: aptTrimmed ?? "",
            countryCode: (countryCode ?? "").toUpperCase(),
          },
          fr: {
            resultType: fr.resultType,
            success: fr.success,
            confidence: fr.confidence,
            suspiciousPolicy: fr.debug?.suspiciousPolicy ?? null,
            fallbackReason: fr.debug?.fallbackReason ?? null,
          },
        }));
        return NextResponse.json({ ...payload, fr });
      }

      const lastTx = result.lastTransaction;
      const exactValue = frDvfMoneyCentsToEuros(result.currentValue ?? lastTx?.value ?? null);
      const lastTxAmountEuroDefault = frDvfMoneyCentsToEuros(lastTx?.value) ?? 0;
      const streetAvgDisplay = frDvfMoneyCentsToEuros(result.areaAverageValue);
      const streetAvgDisplayValue = streetAvgDisplay ?? 0;
      const isBuildingLevel = result.resultLevel === "building";
      const isAreaFallback = result.resultLevel === "commune_fallback" && streetAvgDisplayValue > 0;
      const hasRows = (result.rowsAtStage ?? 0) > 0;
      const matchStageHighEnough = (result.matchStage ?? 0) >= 3;
      const isExactProperty = result.resultLevel === "exact_property";
      const coerceToBuilding = hasRows && matchStageHighEnough && !isExactProperty;
      const hasExactValue = (exactValue ?? 0) > 0;
      const valueLevel = coerceToBuilding
        ? "building-level"
        : isBuildingLevel
          ? "building-level"
          : hasExactValue
            ? "property-level"
            : isAreaFallback
              ? "area-level"
              : "no_match";
      const exactValueMessage = exactValue == null
        ? (isAreaFallback ? "No exact match for this address. Showing postcode/area-level data." : "No DVF data for this address")
        : null;

      const averageBuildingEuroCoerced =
        frDvfMoneyCentsToEuros(
          result.averageBuildingValue ?? result.currentValue ?? result.areaAverageValue ?? null
        ) ?? 0;

      const payload = {
        address: { city: city.trim(), street: street.trim(), house_number: houseNumber.trim() },
        data_source: "properties_france",
        multiple_units: coerceToBuilding,
        result_level: coerceToBuilding ? "building" : result.resultLevel,
        ...(coerceToBuilding ? { average_building_value: averageBuildingEuroCoerced } : {}),
        lot_number: result.lotNumber,
        surface_reelle_bati: result.surfaceReelleBati,
        date_mutation: lastTx?.date ?? null,
        building_sales: mapFranceBuildingSalesPricesToEuros(result.buildingSales ?? []),
        match_stage: result.matchStage,
        rows_at_stage: result.rowsAtStage,
        property_result: {
          exact_value: exactValue,
          exact_value_message: exactValueMessage,
          value_level: valueLevel,
          last_transaction: {
            amount: lastTxAmountEuroDefault,
            date: lastTx?.date ?? null,
            message: lastTxAmountEuroDefault > 0 ? undefined : "No recorded transaction",
          },
          street_average: streetAvgDisplay,
          street_average_message: streetAvgDisplay == null ? "No DVF data for this area" : (isAreaFallback ? "Postcode/area average" : null),
          livability_rating: livabilityRating,
        },
      };
      console.log("[property-value] France final payload (single/building/area):", JSON.stringify({ result_level: payload.result_level, value_level: payload.property_result.value_level, multiple_units: payload.multiple_units, match_stage: payload.match_stage, rows_at_stage: payload.rows_at_stage, building_sales_count: payload.building_sales?.length ?? 0 }));
      const normalizedLot = requestedLot ? (String(requestedLot).replace(/^0+/, "") || String(requestedLot)) : null;
      const fr: FrancePropertyResponse = emptyFranceResponse({
        success: valueLevel !== "no_match" || coerceToBuilding,
        resultType: payload.result_level === "exact_property" ? "exact_apartment" : payload.result_level === "building" ? "building_level" : "no_result",
        confidence: payload.result_level === "exact_property" ? "high" : payload.result_level === "building" ? "medium" : "low",
        matchedAddress: `${houseNumTrim || ""} ${(voie ?? "").trim()}`.trim() || null,
        normalizedAddress: undefined,
        requestedLot,
        normalizedLot,
        property:
          payload.result_level === "exact_property"
            ? {
                transactionDate: payload.date_mutation ?? null,
                transactionValue: payload.property_result?.last_transaction?.amount ?? null,
                pricePerSqm:
                  (payload.surface_reelle_bati ?? 0) > 0 && payload.property_result?.last_transaction?.amount
                    ? Math.round((payload.property_result.last_transaction.amount / (payload.surface_reelle_bati ?? 1)) * 100) / 100
                    : null,
                surfaceArea: payload.surface_reelle_bati ?? null,
                rooms: null,
                propertyType: null,
                building: `${houseNumTrim || ""} ${(voie ?? "").trim()}`.trim() || null,
                postalCode: codePostal ?? null,
                commune: commune ?? null,
              }
            : null,
        buildingStats:
          payload.result_level === "building" || coerceToBuilding
            ? {
                transactionCount: Array.isArray(payload.building_sales) ? payload.building_sales.length : 0,
                avgPricePerSqm: null,
                avgTransactionValue: typeof (payload as any).average_building_value === "number" ? (payload as any).average_building_value : null,
              }
            : null,
        comparables: (payload.building_sales ?? []).map((c: any) => ({
          date: c?.date ?? null,
          type: String(c?.type ?? ""),
          price: Number(c?.price ?? 0) || 0,
          surface: c?.surface ?? null,
          lot_number: c?.lot_number ?? null,
        })),
        debug: {
          searchedAddress: fullAddress,
          normalizedAddress: undefined,
          requestedLot,
          normalizedLot,
          exactLotRowCount,
          buildingRowCount: Array.isArray(payload.building_sales) ? payload.building_sales.length : 0,
          comparableRowCount: 0,
          selectedResultType: payload.result_level === "exact_property" ? "exact_apartment" : payload.result_level === "building" ? "building_level" : "no_result",
          usedFallback: payload.result_level !== "exact_property",
          failureReason: valueLevel === "no_match" ? "no_match" : null,
          queryDurationMs: Date.now() - frRequestStartedAt,
        },
      });
      return NextResponse.json({ ...payload, fr });
    } catch (err) {
      const searchTerm = [houseNumber, street, city].filter(Boolean).join(", ");
      const e = err as Error & { code?: number; errors?: unknown[]; response?: { data?: unknown }; stack?: string };
      console.error("[BigQuery ERROR] France fetch failed:", searchTerm, {
        message: e?.message,
        code: e?.code,
        errors: e?.errors,
        responseData: e?.response?.data,
        stack: e?.stack,
      });
      // If user is searching a specific lot and that request fails (transient auth/quota/etc),
      // retry once at building-level (no lot) so the UI never collapses to "No data for this area"
      // for an address that can be matched to a building.
      try {
        const aptTrimmed = normalizeLot(aptNumber);
        if (aptTrimmed) {
          console.log("[property-value] France retry without lot due to failure:", { aptTrimmed });
          const fullAddress = (addressParam && addressParam.trim()) || [houseNumber, street, city].filter(Boolean).join(", ");
          const parsedFR = parseFRAddressFromFullString(fullAddress);
          const voie = (parsedFR.street || street || "").trim() || null;
          const commune = (parsedFR.city || city || "").trim() || null;
          let codePostal = (parsedFR.postcode || postcode || zip || "").trim();
          if (!codePostal) {
            const fiveDigit = fullAddress.match(/\b(\d{5})\b/)?.[1] ?? "";
            const fourDigit = fullAddress.match(/\b(\d{4})\b/)?.[1] ?? "";
            if (fiveDigit) codePostal = fiveDigit;
            else if (fourDigit) codePostal = "0" + fourDigit;
          }
          codePostal = codePostal.trim();
          if (codePostal.length === 4 && !codePostal.startsWith("0")) codePostal = "0" + codePostal;

          const houseNum = (parsedFR.houseNumber || houseNumber || "").trim() || null;
          if (codePostal && voie) {
            const buildingResult = await getFrancePropertyResult(codePostal, houseNum, voie, commune, null, null);
            if (buildingResult.multipleUnits || (buildingResult.rowsAtStage ?? 0) > 0) {
              const livabilityRating = mapLivabilityToRating(buildingResult.livabilityStandard);
              const retryBuildingValueEuro = frDvfMoneyCentsToEuros(
                buildingResult.averageBuildingValue ?? buildingResult.currentValue ?? null
              );
              const payload = {
                address: { city: city.trim(), street: street.trim(), house_number: houseNumber.trim() },
                data_source: "properties_france",
                multiple_units: true,
                prompt_for_apartment: true,
                apartment_not_matched: true,
                result_level: "building",
                average_building_value: retryBuildingValueEuro ?? 0,
                unit_count: buildingResult.unitCount ?? 0,
                building_sales: mapFranceBuildingSalesPricesToEuros(buildingResult.buildingSales ?? []),
                available_lots: buildingResult.availableLots ?? [],
                match_stage: buildingResult.matchStage,
                rows_at_stage: buildingResult.rowsAtStage,
                property_result: {
                  exact_value: retryBuildingValueEuro,
                  exact_value_message: `Apartment/lot ${aptTrimmed} was not found in DVF data. Showing building-level estimate.`,
                  value_level: "building-level" as const,
                  last_transaction: { amount: 0, date: null, message: "No recorded transaction for this apartment" },
                  street_average: frDvfMoneyCentsToEuros(buildingResult.areaAverageValue),
                  street_average_message: null,
                  livability_rating: livabilityRating,
                },
              };
              console.log("[FR_DEBUG] final France payload", JSON.stringify({
                path: "retry_building_fallback",
                rawParams: {
                  city: city.trim(),
                  street: street.trim(),
                  houseNumber: houseNumber.trim(),
                  postcode: codePostal ?? (postcode || zip || "").trim(),
                  aptNumber: aptTrimmed ?? "",
                  countryCode: (countryCode ?? "").toUpperCase(),
                },
                legacy: {
                  result_level: payload.result_level,
                  value_level: payload.property_result.value_level,
                  match_stage: payload.match_stage,
                  rows_at_stage: payload.rows_at_stage,
                  building_sales_count: payload.building_sales?.length ?? 0,
                },
              }));
              const requestedLot = aptTrimmed || null;
              const normalizedLot = requestedLot ? (String(requestedLot).replace(/^0+/, "") || String(requestedLot)) : null;
              const fr: FrancePropertyResponse = emptyFranceResponse({
                success: true,
                resultType: "building_level",
                confidence: "medium",
                matchedAddress: `${houseNum || ""} ${(voie ?? "").trim()}`.trim() || null,
                normalizedAddress: undefined,
                requestedLot,
                normalizedLot,
                property: null,
                buildingStats: {
                  transactionCount: Array.isArray(payload.building_sales) ? payload.building_sales.length : 0,
                  avgPricePerSqm: null,
                  avgTransactionValue: typeof payload.average_building_value === "number" ? payload.average_building_value : null,
                },
                comparables: (payload.building_sales ?? []).map((c: any) => ({
                  date: c?.date ?? null,
                  type: String(c?.type ?? ""),
                  price: Number(c?.price ?? 0) || 0,
                  surface: c?.surface ?? null,
                  lot_number: c?.lot_number ?? null,
                })),
                debug: {
                  searchedAddress: fullAddress,
                  normalizedAddress: undefined,
                  requestedLot,
                  normalizedLot,
                  exactLotRowCount: buildingResult.exactLotRowCount ?? 0,
                  buildingRowCount: Array.isArray(payload.building_sales) ? payload.building_sales.length : 0,
                  comparableRowCount: 0,
                  selectedResultType: "building_level",
                  usedFallback: true,
                  failureReason: "lot_request_failed_retry_building",
                  queryDurationMs: Date.now() - frRequestStartedAt,
                },
              });
              return NextResponse.json({ ...payload, fr });
            }
          }
        }
      } catch (retryErr) {
        const retryErrAny = retryErr as { message?: string } | undefined;
        const retryErrMessage = retryErrAny?.message ?? "Unknown error";
        console.error("[property-value] France retry without lot failed:", retryErrMessage);
      }

      return cacheAndReturn(frEmptyResponse());
    }
  }

  if (!isIL && !isFR) {
    return NextResponse.json(
      { message: "Country not supported. Only Israel (IL) and France (FR) are supported.", error: "UNSUPPORTED_COUNTRY" },
      { status: 400 }
    );
  }

  const cached = CACHE.get(cacheKey);
  if (cached && Date.now() - cached.ts < CACHE_TTL_MS) {
    let cachedResponse = { ...cached.data, data_source: "cache" as const } as Record<string, unknown>;
    if (isUK) {
      const uk = (cachedResponse.uk_land_registry ?? {}) as { has_exact_flat_match?: boolean; has_building_match?: boolean; latest_building_transaction?: { price: number; date: string } | null; latest_nearby_transaction?: { price: number; date: string } | null; street_average_price?: number | null };
      const hasExactFlatMatch = uk.has_exact_flat_match === true;
      const hasBuildingMatch = uk.has_building_match === true;
      const latestTx = uk.latest_building_transaction ?? uk.latest_nearby_transaction ?? null;
      const streetAvg = uk.street_average_price ?? null;
      const valueLevel = (hasExactFlatMatch && latestTx != null && latestTx.price > 0
        ? "property-level"
        : hasBuildingMatch || (latestTx != null && latestTx.price > 0)
          ? "building-level"
          : streetAvg != null && streetAvg > 0
            ? "street-level"
            : "area-level") as "property-level" | "building-level" | "street-level" | "area-level";
      const pr = (cachedResponse.property_result ?? {}) as Record<string, unknown>;
      cachedResponse = { ...cachedResponse, property_result: { ...pr, value_level: valueLevel } };
    }
    if (isUK && process.env.NODE_ENV === "development") {
      const pr = ((cachedResponse as Record<string, unknown>).property_result ?? {}) as { value_level?: string; last_transaction?: { amount?: number; date?: string | null } };
      const lt = pr.last_transaction;
      const txSource = pr.value_level === "property-level" ? "exact_flat_match" : pr.value_level === "building-level" ? "building_match" : pr.value_level === "street-level" ? "street_match" : "area_fallback";
      console.log("[UK capture CACHE]", JSON.stringify({
        rawInputAddress: rawInputAddress.trim() || "(empty)",
        selectedFormattedAddress: selectedFormattedAddress.trim() || "(empty)",
        parsed_houseNumber: houseNumber.trim() || "(empty)",
        parsed_street: street.trim() || "(empty)",
        parsed_postcode: postcode.trim() || "(empty)",
        latest_transaction: lt && (lt.amount ?? 0) > 0 ? { price: lt.amount, date: lt.date } : null,
        source: txSource,
      }));
    }
    return NextResponse.json(cachedResponse);
  }

  const runHandler = async (): Promise<Response> => {
  try {
    // Matching runs in provider first; valuation (HPI, EPC) runs after in this route
    let result: Awaited<ReturnType<typeof getPropertyValueInsights>>;
    try {
      result = await withTimeout(
        getPropertyValueInsights(
          {
            city: city.trim(),
            street: street.trim(),
            houseNumber: houseNumber.trim(),
            state: state.trim() || undefined,
            zip: zip.trim() || undefined,
            postcode: postcode.trim() || zip.trim() || undefined,
            latitude: Number.isFinite(latitude) ? latitude : undefined,
            longitude: Number.isFinite(longitude) ? longitude : undefined,
            fullAddress: addressParam || undefined,
            ...(isUK && rawInputAddress.trim()
              ? { rawInputAddress: rawInputAddress.trim(), selectedFormattedAddress: selectedFormattedAddress.trim() || undefined }
              : {}),
          },
          countryCode
        ),
        isUK ? LAND_REGISTRY_TIMEOUT_MS : 10000,
        "land_registry"
      );
    } catch (e) {
      if (isUK && e instanceof Error && e.message.startsWith("timeout:")) {
        if (process.env.NODE_ENV === "development") console.debug("[property-value] Land Registry timed out, using HPI fallback");
        result = {
          message: "no transaction found",
          debug: {
            records_fetched: 0,
            records_returned: 0,
            records_after_filter: 0,
            exact_matches_count: 0,
            failure_reason: "Land Registry timeout",
          },
        };
      } else {
        throw e;
      }
    }

    if ("message" in result && "error" in result && result.error) {
      const status =
        result.error === "INVALID_INPUT"
          ? 400
          : result.error === "PROVIDER_NOT_CONFIGURED" || result.error === "DATA_SOURCE_UNAVAILABLE"
            ? 503
            : result.error === "NO_PROVIDER"
              ? 404
              : 502;
      return NextResponse.json(result, { status });
    }

    if ("message" in result && result.message === "no transaction found") {
      if (isUK && result && typeof result === "object" && "uk_land_registry" in result && (result as { uk_land_registry?: unknown }).uk_land_registry) {
        // UK: never return 404 when we have uk_land_registry (postcode data exists)
      } else if (isUK) {
        // UK: return 200 with minimal uk_land_registry; try HPI fallback when Land Registry has no transactions
        const noMatchResult = result as { message: string; debug?: Record<string, unknown> };
        let ukLandRegistry: {
          building_average_price: null;
          transactions_in_building: number;
          latest_building_transaction: null;
          latest_nearby_transaction: null;
          has_building_match: false;
          average_area_price: number | null;
          median_area_price: number | null;
          price_trend: { change_1y_percent: number; ref_month?: string } | null;
          area_transaction_count: number;
          area_fallback_level: "none";
          fallback_level_used: "area";
          match_confidence: "low" | "medium";
          area_data_source: "land_registry" | "HPI";
        } = {
          building_average_price: null,
          transactions_in_building: 0,
          latest_building_transaction: null,
          latest_nearby_transaction: null,
          has_building_match: false,
          average_area_price: null,
          median_area_price: null,
          price_trend: null,
          area_transaction_count: 0,
          area_fallback_level: "none",
          fallback_level_used: "area",
          match_confidence: "low",
          area_data_source: "land_registry",
        };
        try {
          const hpiResult = await withTimeout(
            fetchUKHPIForLocality(city.trim(), postcode.trim() || undefined),
            PROVIDER_TIMEOUT_MS,
            "HPI"
          );
          if (hpiResult) {
            ukLandRegistry = {
              ...ukLandRegistry,
              average_area_price: hpiResult.average_area_price,
              median_area_price: hpiResult.median_area_price,
              price_trend: hpiResult.price_trend,
              area_data_source: "HPI",
              match_confidence: "medium",
            };
          }
        } catch {
          // HPI failure must not break the property card
        }
        let ukLivability: "POOR" | "FAIR" | "GOOD" | "VERY GOOD" | "EXCELLENT" = "POOR";
        try {
          const ukStats = await withTimeout(
            fetchUKNeighborhoodStats(postcode.trim() || "", (ukLandRegistry.average_area_price) ?? undefined),
            PROVIDER_TIMEOUT_MS,
            "UK_neighborhood"
          );
          ukLivability = computeUKLivabilityRating(ukStats);
        } catch {
          if (ukLandRegistry.average_area_price != null && ukLandRegistry.average_area_price > 0) {
            ukLivability = computeUKLivabilityRating({ livability_proxy_from_area_price: ukLandRegistry.average_area_price });
          }
        }
        let noMatchExactValue: number | null = ukLandRegistry.average_area_price;
        if (noMatchExactValue == null && isEPCConfigured() && ukLandRegistry.average_area_price != null && ukLandRegistry.average_area_price > 0) {
          const epcNoMatchTimeout = new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error("EPC timeout")), 5500)
          );
          try {
            const epcNoMatchWork = (async () => {
              try {
                const epcAreas = await fetchEPCFloorAreasForArea(postcode.trim() || "", street.trim() || undefined);
                if (epcAreas.length < 2) return;
                const avgArea = epcAreas.reduce((s, a) => s + a.total_floor_area_m2, 0) / epcAreas.length;
                if (avgArea <= 0) return;
                const pricePerM2 = ukLandRegistry.average_area_price! / avgArea;
                const subjectEPC = await fetchEPCFloorArea(postcode.trim() || "", {
                  houseNumber: houseNumber.trim() || undefined,
                  street: street.trim() || undefined,
                  city: city.trim() || undefined,
                });
                if (subjectEPC && subjectEPC.total_floor_area_m2 > 0) {
                  noMatchExactValue = Math.round(subjectEPC.total_floor_area_m2 * pricePerM2);
                }
              } finally {
                if (process.env.NODE_ENV === "development") console.debug("[property-value] Provider finished: EPC");
              }
            })();
            await Promise.race([epcNoMatchWork, epcNoMatchTimeout]);
          } catch {
            // EPC failure must not block response
          }
        }
        const hasTrustedAreaData = (ukLandRegistry.average_area_price != null && ukLandRegistry.average_area_price > 0) || noMatchExactValue != null;
        const ukNoPropertyRecord = !hasTrustedAreaData;
        // No-data cases: always area-level for consistency (HPI may or may not return data)
        const noMatchLevel = "area-level" as const;
        const augmented = {
          ...noMatchResult,
          address: { city: city.trim() || postcode.trim(), street: street.trim() || postcode.trim(), house_number: houseNumber.trim() },
          uk_land_registry: ukLandRegistry,
          uk_no_property_record: ukNoPropertyRecord,
          data_source: "live" as const,
          property_result: {
            exact_value: noMatchExactValue,
            exact_value_message: ukNoPropertyRecord ? "No exact UK property record found for this address" : (noMatchExactValue == null ? "No HPI or Land Registry data" : null),
            value_level: noMatchLevel as "no_match" | "area-level",
            last_transaction: { amount: 0, date: null, message: "No recorded transaction found" as const },
            street_average: null,
            street_average_message: "No street-level average found" as const,
            livability_rating: ukLivability,
          },
          debug: { ...(noMatchResult.debug ?? {}) },
        };
        const isFromProviderTimeout = noMatchResult.debug?.failure_reason === "Land Registry timeout";
        if (!isFromProviderTimeout) {
          CACHE.set(cacheKey, { data: augmented, ts: Date.now() });
        }
        return NextResponse.json(augmented);
      } else {
        return NextResponse.json(result, { status: 404 });
      }
    }

    if ("message" in result && result.message === "no reliable exact match found") {
      if (isUK && result && typeof result === "object" && "uk_land_registry" in result && (result as { uk_land_registry?: unknown }).uk_land_registry) {
        // UK: never return 404 when we have uk_land_registry
      } else {
        return NextResponse.json(result, { status: 404 });
      }
    }

    if ("error" in result && result.error === "UNIT_REQUIRED") {
      return NextResponse.json(result, { status: 400 });
    }

    let response = result as Record<string, unknown>;
    const dataSource = usMockMode ? ("mock" as const) : ("live" as const);
    response = { ...response, data_source: dataSource };

    if (
      isUS &&
      !usMockMode &&
      Number.isFinite(latitude) &&
      Number.isFinite(longitude) &&
      !(response && typeof response === "object" && "neighborhood_stats" in response && (response as { neighborhood_stats?: unknown }).neighborhood_stats)
    ) {
      try {
        const neighborhoodStats = await fetchNeighborhoodStats(latitude!, longitude!, {
          zip: zip.trim() || undefined,
        });
        if (neighborhoodStats) {
          response = { ...response, neighborhood_stats: neighborhoodStats };
        }
      } catch {
        // Census failure must not break the property card
      }
    }

    if (isUS && (state || usMockMode)) {
      try {
        const fhfaResult = usMockMode
          ? null
          : await fetchMarketTrend({
              state: state.trim() || undefined,
              zip: zip.trim() || undefined,
              latitude,
              longitude,
            });
        if (fhfaResult) {
          response = { ...response, market_trend: fhfaResult.market_trend };
        }
      } catch {
        // FHFA failure must not break the property card
      }
    }

    if (usMockMode && isUS) {
      const mockTrend = (response as { market_trend?: unknown }).market_trend;
      if (!mockTrend) {
        response = { ...response, market_trend: { hpi_index: 412.3, change_1y_percent: 4.2, latest_date: "2024-10" } };
      }
    }

    if (isUS && (usMockMode || (response.avm_value != null && (response.avm_value as number) > 0) || (response.last_sale != null && (response.last_sale as { price?: number }).price != null && ((response.last_sale as { price?: number }).price ?? 0) > 0) || (response.sales_history != null && Array.isArray(response.sales_history) && response.sales_history.length > 0) || (response.estimated_area_price != null && (response.estimated_area_price as number) > 0) || (response.median_sale_price != null && (response.median_sale_price as number) > 0) || (response.nearby_comps != null && typeof response.nearby_comps === "object" && ((response.nearby_comps as { avg_price?: number }).avg_price ?? 0) > 0) || (response.neighborhood_stats != null && typeof response.neighborhood_stats === "object" && (response.neighborhood_stats as { median_home_value?: number }).median_home_value != null && ((response.neighborhood_stats as { median_home_value?: number }).median_home_value ?? 0) > 0))) {
      const r = response as Record<string, unknown>;
      const avm = typeof r.avm_value === "number" && r.avm_value > 0 ? r.avm_value : undefined;
      const lastSale = r.last_sale as { price?: number; date?: string } | undefined;
      const salesHistory = r.sales_history as Array<{ price: number; date: string }> | undefined;
      const latestTx = r.latest_transaction as { transaction_price?: number; transaction_date?: string } | undefined;
      const latestTxPrice = latestTx?.transaction_price != null && latestTx.transaction_price > 0 ? latestTx.transaction_price : undefined;
      const areaPrice = typeof r.estimated_area_price === "number" && r.estimated_area_price > 0 ? r.estimated_area_price : undefined;
      const medianSale = typeof r.median_sale_price === "number" && r.median_sale_price > 0 ? r.median_sale_price : undefined;
      const nearbyComps = r.nearby_comps as { avg_price?: number } | undefined;
      const compsAvg = nearbyComps?.avg_price != null && nearbyComps.avg_price > 0 ? nearbyComps.avg_price : undefined;
      const ns = r.neighborhood_stats as { median_home_value?: number } | undefined;
      const medianHome = ns?.median_home_value != null && ns.median_home_value > 0 ? ns.median_home_value : undefined;
      const lastSalePrice = lastSale?.price != null && lastSale.price > 0 ? lastSale.price : undefined;
      const salesHistoryFirst = Array.isArray(salesHistory) && salesHistory.length > 0 && salesHistory[0]?.price != null ? salesHistory[0].price : undefined;

      const hasPropertyLevelData = (avm != null && avm > 0) || (lastSale?.price != null && lastSale.price > 0) || (Array.isArray(salesHistory) && salesHistory.length > 0) || (latestTxPrice != null) || (compsAvg != null && compsAvg > 0);
      const isAreaLevelOnly = !hasPropertyLevelData && (areaPrice != null || medianSale != null || medianHome != null);

      const primaryValue = avm ?? lastSalePrice ?? salesHistoryFirst ?? latestTxPrice ?? compsAvg ?? (isAreaLevelOnly ? undefined : (areaPrice ?? medianSale ?? medianHome));
      const valueSource =
        avm != null && avm > 0
          ? "rentcast_avm"
          : lastSalePrice != null
            ? "last_sale"
            : salesHistoryFirst != null
              ? "sales_history"
              : latestTxPrice != null
                ? "latest_transaction"
                : compsAvg != null
                  ? "nearby_comps"
                  : areaPrice != null
                    ? "zillow_area"
                    : medianSale != null
                      ? "redfin_area"
                      : medianHome != null
                        ? "census_median"
                        : "none";

      if (typeof primaryValue === "number" && primaryValue > 0) {
        let low: number;
        let high: number;
        if (avm != null && avm > 0) {
          low = Math.round(avm * 0.93);
          high = Math.round(avm * 1.07);
        } else if (lastSalePrice != null && lastSale?.date) {
          const saleAgeYears = (Date.now() - new Date(lastSale.date).getTime()) / (365.25 * 24 * 60 * 60 * 1000);
          const adj = saleAgeYears < 2 ? 0.06 : saleAgeYears < 4 ? 0.1 : 0.15;
          low = Math.round(lastSalePrice * (1 - adj));
          high = Math.round(lastSalePrice * (1 + adj));
        } else if (compsAvg != null && compsAvg > 0) {
          low = Math.round(compsAvg * 0.9);
          high = Math.round(compsAvg * 1.1);
        } else if (salesHistoryFirst != null || latestTxPrice != null) {
          const base = salesHistoryFirst ?? latestTxPrice!;
          low = Math.round(base * 0.92);
          high = Math.round(base * 1.08);
        } else {
          low = Math.round(primaryValue * 0.92);
          high = Math.round(primaryValue * 1.08);
        }
        response = { ...response, value_range: { low_estimate: low, estimated_value: primaryValue, high_estimate: high } };
      } else if (isAreaLevelOnly && (areaPrice != null || medianSale != null || medianHome != null)) {
        const areaVal = areaPrice ?? medianSale ?? medianHome!;
        response = { ...response, value_range: { low_estimate: Math.round(areaVal * 0.9), estimated_value: areaVal, high_estimate: Math.round(areaVal * 1.1) } };
      }
      if (isAreaLevelOnly) {
        response = { ...response, is_area_level_estimate: true, us_match_confidence: "low" as const };
      }
      response = { ...response, value_source: valueSource };

      const valueLevel: "property-level" | "street-level" | "area-level" =
        ((avm != null && avm > 0) || lastSalePrice != null || salesHistoryFirst != null || latestTxPrice != null)
          ? "property-level"
          : compsAvg != null
            ? "street-level"
            : "area-level";

      const exactValue = (avm != null && avm > 0) || lastSalePrice != null || salesHistoryFirst != null || latestTxPrice != null || compsAvg != null
        ? (avm ?? lastSalePrice ?? salesHistoryFirst ?? latestTxPrice ?? compsAvg)!
        : null;

      const lastTransaction =
        lastSalePrice != null && lastSalePrice > 0 && lastSale?.date
          ? { amount: lastSalePrice, date: lastSale.date }
          : Array.isArray(salesHistory) && salesHistory.length > 0 && salesHistory[0]?.price != null
            ? { amount: salesHistory[0].price, date: salesHistory[0].date ?? "" }
            : latestTxPrice != null && latestTx?.transaction_date
              ? { amount: latestTxPrice, date: latestTx.transaction_date }
              : { amount: 0, date: null as string | null, message: "No recorded transaction found" as const };

      const streetAverage = compsAvg != null && compsAvg > 0 ? compsAvg : null;
      const streetAverageMessage = streetAverage == null ? "No street-level average found" as const : null;

      const nsForLivability = r.neighborhood_stats as {
        median_household_income?: number;
        median_home_value?: number;
        population?: number;
        population_growth_percent?: number;
        income_growth_percent?: number;
        pct_bachelors_plus?: number;
      } | undefined;
      const income = nsForLivability?.median_household_income ?? 0;
      const homeVal = nsForLivability?.median_home_value ?? 0;
      const popGrowth = nsForLivability?.population_growth_percent ?? 0;
      const incGrowth = nsForLivability?.income_growth_percent ?? 0;
      const pctBachelors = nsForLivability?.pct_bachelors_plus ?? 0;
      let livabilityRating: "POOR" | "FAIR" | "GOOD" | "VERY GOOD" | "EXCELLENT" = "POOR";
      if (income > 0 || homeVal > 0) {
        const score = (income >= 100000 ? 4 : income >= 75000 ? 3 : income >= 50000 ? 2 : income >= 35000 ? 1 : 0) +
          (incGrowth > 2 ? 0.5 : incGrowth > 0 ? 0.25 : 0) +
          (popGrowth > 0 ? 0.25 : 0) +
          (pctBachelors >= 0.4 ? 0.5 : pctBachelors >= 0.25 ? 0.25 : 0);
        if (score >= 4) livabilityRating = "EXCELLENT";
        else if (score >= 3) livabilityRating = "VERY GOOD";
        else if (score >= 2) livabilityRating = "GOOD";
        else if (score >= 1) livabilityRating = "FAIR";
        else livabilityRating = "POOR";
      }

      response = {
        ...response,
        property_result: {
          exact_value: exactValue,
          exact_value_message: exactValue == null && (areaPrice != null || medianSale != null || medianHome != null)
            ? "No exact property-level value found"
            : null,
          value_level: valueLevel,
          last_transaction: lastTransaction,
          street_average: streetAverage,
          street_average_message: streetAverageMessage,
          livability_rating: livabilityRating,
        },
      };

      const dataSrc = (r.data_sources as string[] | undefined) ?? [];
      const parts: string[] = [];
      if (dataSrc.includes("RentCast")) parts.push("RentCast");
      if (dataSrc.includes("Census")) parts.push("Census");
      if (dataSrc.includes("Zillow")) parts.push("Zillow");
      if (dataSrc.includes("Redfin")) parts.push("Redfin");
      if (!parts.includes("Census") && r.neighborhood_stats != null && typeof r.neighborhood_stats === "object") parts.push("Census");
      if (r.market_trend != null && typeof r.market_trend === "object") parts.push("FHFA");
      if (parts.length > 0) {
        response = { ...response, source_summary: `Based on ${parts.join(" + ")}` };
      }
      const mt = r.market_trend as { latest_date?: string } | undefined;
      if (mt?.latest_date) {
        const [y, m] = mt.latest_date.split("-");
        const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
        const monthIdx = parseInt(m ?? "1", 10) - 1;
        response = { ...response, last_market_update: `${monthNames[monthIdx] ?? m} ${y}` };
      } else if (parts.length > 0) {
        response = { ...response, last_market_update: "Updated monthly" };
      }
    } else if (isUS) {
      response = {
        ...response,
        property_result: {
          exact_value: null,
          exact_value_message: "No exact property-level value found",
          value_level: "area-level" as const,
          last_transaction: { amount: 0, date: null, message: "No recorded transaction found" as const },
          street_average: null,
          street_average_message: "No street-level average found" as const,
          livability_rating: "POOR" as const,
        },
      };
    }

    if (isUS) {
      const r = response as Record<string, unknown>;
      const salesHistory = r.sales_history as Array<{ date: string; price: number }> | undefined;
      const addr = r.address as { city?: string; street?: string; house_number?: string } | undefined;
      const shortAddr = addr ? [addr.house_number, addr.street, addr.city].filter(Boolean).join(" ").trim() || undefined : undefined;
      if (Array.isArray(salesHistory) && salesHistory.length > 0) {
        const pd = r.property_details as { sqft?: number } | undefined;
        const sqft = pd?.sqft ?? 0;
        const nearbySales = salesHistory.slice(0, 5).map((s) => ({
          address: shortAddr ?? "Searched property",
          price: s.price,
          date: s.date,
          price_per_sqft: sqft > 0 ? Math.round((s.price / sqft) * 100) / 100 : undefined,
          is_same_property: true,
        }));
        response = { ...response, nearby_sales: nearbySales };
      }
    }

    if (isUS) {
      const r = response as Record<string, unknown>;
      const salesHistory = r.sales_history as Array<{ date: string; price: number }> | undefined;
      const lastSale = r.last_sale as { price: number; date: string } | undefined;
      const latestTx = r.latest_transaction as { transaction_price?: number; transaction_date?: string } | undefined;
      const mostRecent =
        Array.isArray(salesHistory) && salesHistory.length > 0
          ? { price: salesHistory[0].price, date: salesHistory[0].date, source: "RentCast" as const }
          : lastSale && lastSale.price > 0
            ? { price: lastSale.price, date: lastSale.date, source: "RentCast" as const }
            : latestTx && typeof latestTx.transaction_price === "number" && latestTx.transaction_price > 0
              ? { price: latestTx.transaction_price, date: latestTx.transaction_date ?? "", source: "RentCast" as const }
              : null;
      if (mostRecent) {
        response = { ...response, last_recorded_sale: mostRecent };
      } else if (process.env.NODE_ENV === "development" || searchParams.get("trace") === "1") {
        const trace = {
          reason: "no_sale_data",
          checked: ["sales_history", "last_sale", "latest_transaction"],
          sales_history_count: Array.isArray(salesHistory) ? salesHistory.length : 0,
          last_sale_present: Boolean(lastSale && lastSale.price > 0),
          latest_transaction_price: latestTx?.transaction_price ?? null,
        };
        response = { ...response, _last_recorded_sale_trace: trace };
      }
    }

    if (isUK && response.uk_land_registry && typeof response.uk_land_registry === "object") {
      const uk = response.uk_land_registry as {
        average_area_price?: number | null;
        street_average_price?: number | null;
        area_transaction_count?: number;
        area_data_source?: string;
        latest_building_transaction?: { price: number; date: string } | null;
        latest_nearby_transaction?: { price: number; date: string } | null;
      };
      const latest = uk.latest_building_transaction ?? uk.latest_nearby_transaction;
      if (latest && latest.price > 0) {
        const source = uk.area_data_source === "HPI" ? "HPI" : "Land Registry";
        response = { ...response, last_recorded_sale: { price: latest.price, date: latest.date ?? "", source } };
      }
      const hasNoUsableAreaData =
        (uk.average_area_price == null || uk.average_area_price <= 0) && (uk.area_transaction_count ?? 0) === 0;
      if (hasNoUsableAreaData) {
        try {
          const hpiResult = await withTimeout(
            fetchUKHPIForLocality(city.trim(), postcode.trim() || undefined),
            PROVIDER_TIMEOUT_MS,
            "HPI"
          );
          if (hpiResult) {
            response = {
              ...response,
              uk_land_registry: {
                ...(response.uk_land_registry as Record<string, unknown>),
                average_area_price: hpiResult.average_area_price,
                median_area_price: hpiResult.median_area_price,
                price_trend: hpiResult.price_trend,
                area_data_source: "HPI",
                match_confidence: "medium",
              },
            };
          }
        } catch {
          // HPI failure must not break the property card
        }
      }

      const ukForResult = response.uk_land_registry as {
        has_building_match?: boolean;
        has_exact_flat_match?: boolean;
        average_area_price?: number | null;
        street_average_price?: number | null;
        latest_building_transaction?: { price: number; date: string } | null;
        latest_nearby_transaction?: { price: number; date: string } | null;
      };
      const hasBuildingMatch = ukForResult.has_building_match === true;
      const hasExactFlatMatch = ukForResult.has_exact_flat_match === true;
      const latestBuildingTx = ukForResult.latest_building_transaction ?? null;
      const latestNearbyTx = ukForResult.latest_nearby_transaction ?? null;
      const latestTx = latestBuildingTx ?? latestNearbyTx;
      const areaPrice = ukForResult.average_area_price ?? null;
      const streetAvg = ukForResult.street_average_price ?? null;

      let exactValue: number | null = null;
      let exactValueFromEPC = false;
      if (latestTx && latestTx.price > 0) {
        try {
          const indices = await withTimeout(
            fetchUKHPIIndicesForLocality(city.trim(), postcode.trim() || undefined),
            PROVIDER_TIMEOUT_MS,
            "HPI_indices"
          );
          const hpiAdjusted = estimateValueFromHPI(latestTx.price, latestTx.date ?? "", indices);
          if (hpiAdjusted) exactValue = hpiAdjusted;
        } catch {
          exactValue = latestTx.price;
        }
        if (exactValue == null && latestTx.price > 0) exactValue = latestTx.price;
      }

      if (exactValue == null && isEPCConfigured()) {
        const epcTimeout = new Promise<null>((_, reject) =>
          setTimeout(() => reject(new Error("EPC timeout")), 5500)
        );
        try {
          const epcWork = (async () => {
            try {
              const avgPrice = (streetAvg ?? areaPrice) ?? 0;
              if (avgPrice <= 0) return;
              const epcAreas = await fetchEPCFloorAreasForArea(postcode.trim() || "", street.trim() || undefined);
              if (epcAreas.length < 2) return;
              const avgArea = epcAreas.reduce((s, a) => s + a.total_floor_area_m2, 0) / epcAreas.length;
              if (avgArea <= 0) return;
              const pricePerM2 = avgPrice / avgArea;
              const subjectEPC = await fetchEPCFloorArea(postcode.trim() || "", {
                houseNumber: houseNumber.trim() || undefined,
                street: street.trim() || undefined,
                city: city.trim() || undefined,
              });
              if (subjectEPC && subjectEPC.total_floor_area_m2 > 0) {
                exactValue = Math.round(subjectEPC.total_floor_area_m2 * pricePerM2);
                exactValueFromEPC = true;
              }
            } finally {
              if (process.env.NODE_ENV === "development") console.debug("[property-value] Provider finished: EPC");
            }
          })();
          await Promise.race([epcWork, epcTimeout]);
        } catch (e) {
          if (process.env.NODE_ENV === "development") {
            console.debug("[property-value] EPC skipped:", e instanceof Error ? e.message : String(e));
          }
        }
      }

      if (exactValue == null && streetAvg != null && streetAvg > 0) exactValue = streetAvg;
      if (exactValue == null && areaPrice != null && areaPrice > 0) exactValue = areaPrice;

      const valuationMethod =
        exactValueFromEPC ? "epc" : latestTx && latestTx.price > 0 ? "exact_transaction" : streetAvg != null ? "street" : "area";

      // Property-level requires exact flat (SAON) match; building tx alone must never be labeled property-level.
      // Area-level only when no building match, no street avg, no transactions. If latestTx exists, min level is building-level.
      const valueLevel = (hasExactFlatMatch && latestTx != null && latestTx.price > 0
        ? "property-level"
        : hasBuildingMatch || (latestTx != null && latestTx.price > 0)
          ? "building-level"
          : streetAvg != null && streetAvg > 0
            ? "street-level"
            : "area-level") as "property-level" | "building-level" | "street-level" | "area-level";
      const flatMatch = valueLevel === "property-level";
      const buildingMatch = hasBuildingMatch;
      const streetMatch = streetAvg != null && streetAvg > 0;

      const matchLevelAttempted = flatMatch ? "property" : buildingMatch ? "building" : streetMatch ? "street" : "area";

      const requestId = crypto.randomUUID();
      if (process.env.NODE_ENV === "development") {
        console.debug("[property-value] UK request", {
          request_id: requestId,
          rawInputAddress: rawInputAddress.trim() || "(empty)",
          selectedFormattedAddress: selectedFormattedAddress.trim() || "(empty)",
          valuation_method: valuationMethod,
          value_level: valueLevel,
          match_level_attempted: matchLevelAttempted,
          flat_match: flatMatch,
          building_match: buildingMatch,
          street_match: streetMatch,
          fallback_level: valueLevel,
          has_exact_flat_match: hasExactFlatMatch,
          has_building_match: hasBuildingMatch,
          street_avg: streetAvg ?? null,
          latest_transaction: latestTx ? { price: latestTx.price, date: latestTx.date } : null,
        });
        const txSource = flatMatch ? "exact_flat_match" : buildingMatch ? "building_match" : streetMatch ? "street_match" : "area_fallback";
        console.log("[UK capture]", JSON.stringify({
          rawInputAddress: rawInputAddress.trim() || "(empty)",
          selectedFormattedAddress: selectedFormattedAddress.trim() || "(empty)",
          parsed_houseNumber: houseNumber.trim() || "(empty)",
          parsed_street: street.trim() || "(empty)",
          parsed_postcode: postcode.trim() || "(empty)",
          latest_transaction: latestTx ? { price: latestTx.price, date: latestTx.date } : null,
          source: txSource,
        }));
      }

      const lastTransaction =
        latestTx && latestTx.price > 0
          ? { amount: latestTx.price, date: latestTx.date ?? null, message: undefined as string | undefined }
          : { amount: 0, date: null as string | null, message: "No recorded transaction found" as const };

      const streetAverage = streetAvg != null && streetAvg > 0 ? streetAvg : null;
      const streetAverageMessage = streetAverage == null ? "No street-level average found" as const : null;

      let livabilityRating: "POOR" | "FAIR" | "GOOD" | "VERY GOOD" | "EXCELLENT" = "POOR";
      try {
        const ukStats = await withTimeout(
          fetchUKNeighborhoodStats(postcode.trim() || "", (areaPrice) ?? undefined),
          PROVIDER_TIMEOUT_MS,
          "UK_neighborhood"
        );
        livabilityRating = computeUKLivabilityRating(ukStats);
      } catch {
        if (areaPrice != null && areaPrice > 0) {
          livabilityRating = computeUKLivabilityRating({ livability_proxy_from_area_price: areaPrice });
        }
      }

      const existingDebug = (response.debug ?? {}) as Record<string, unknown>;
      response = {
        ...response,
        property_result: {
          exact_value: exactValue,
          exact_value_message: exactValue == null && areaPrice != null ? "No HPI-adjusted value; area average only" : null,
          value_level: valueLevel,
          last_transaction: lastTransaction,
          street_average: streetAverage,
          street_average_message: streetAverageMessage,
          livability_rating: livabilityRating,
        },
        debug: { ...existingDebug },
      };
    }

    if ("address" in result && result.address) {
      CACHE.set(cacheKey, { data: response, ts: Date.now() });
    }

    return NextResponse.json(response);
  } catch (err) {
    console.error("[property-value] Error:", err);
    return NextResponse.json(
      {
        message: "Failed to fetch property value insights. Please try again later.",
        error: err instanceof Error ? err.message : "Unknown error",
        ...(isUK && { property_result: buildUKMinimalResponse().property_result }),
      },
      { status: 500 }
    );
  }
  };

  const routeTimeoutMs = isUK ? ROUTE_TIMEOUT_MS_UK : isFR ? ROUTE_TIMEOUT_MS_FR : ROUTE_TIMEOUT_MS;
  const timeoutResponse = new Promise<Response>((resolve) => {
    setTimeout(() => {
      if (process.env.NODE_ENV === "development") console.debug("[property-value] Route timeout, returning minimal response");
      resolve(NextResponse.json(isUK ? buildUKMinimalResponse() : { message: "Request timeout", error: "TIMEOUT" }, isUK ? { status: 200 } : { status: 503 }));
    }, routeTimeoutMs);
  });

  return Promise.race([runHandler(), timeoutResponse]);
}
