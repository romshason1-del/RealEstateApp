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
  opts?: { latitude?: number; longitude?: number }
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
    const hasCityOrStreet = !!(city.trim() || street.trim());
    if (!hasCityOrStreet) return { valid: false, error: "city or street required for France addresses" };
    if (city.length > MAX_ADDRESS_LENGTH || street.length > MAX_ADDRESS_LENGTH) return { valid: false, error: "address too long" };
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
  const projectId = (process.env.GOOGLE_CLOUD_PROJECT_ID ?? "").trim();
  const raw = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!projectId) throw new Error("GOOGLE_CLOUD_PROJECT_ID is required");
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT_KEY is required");
  const credentials = JSON.parse(raw) as { client_email?: string; private_key?: string };
  if (credentials.private_key) {
    credentials.private_key = credentials.private_key.replace(/\\n/g, "\n");
  }
  return new BigQuery({ projectId, credentials });
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

  if (addressParam) {
    const codeRaw = (countryCode ?? "").toUpperCase();
    const code = codeRaw === "RE" ? "FR" : codeRaw;
    if (code === "US") {
      const parsed = parseUSAddressFromFullString(addressParam);
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
        const parsed = parseUKAddressFromFullString(addressParam);
        street = parsed.street || street;
        city = parsed.city || city;
        postcode = parsed.postcode || postcode;
        houseNumber = parsed.houseNumber || houseNumber;
      }
    } else if (code === "IT") {
      const parsed = parseAddressFromFullString(addressParam);
      if (parsed.city) city = city || parsed.city;
      if (parsed.street) street = street || parsed.street;
      if (parsed.houseNumber) houseNumber = houseNumber || parsed.houseNumber;
    } else if (code === "FR") {
      const parsed = parseFRAddressFromFullString(addressParam);
      if (parsed.city) city = city || parsed.city;
      if (parsed.street) street = street || parsed.street;
      if (parsed.houseNumber) houseNumber = houseNumber || parsed.houseNumber;
      if (parsed.postcode) postcode = postcode || parsed.postcode;
    } else {
      if (!city || !street) {
        const parsed = parseAddressFromFullString(addressParam);
        if (parsed.city) city = city || parsed.city;
        if (parsed.street) street = street || parsed.street;
        if (parsed.houseNumber) houseNumber = houseNumber || parsed.houseNumber;
      }
    }
  }

  const validation = validateInput(city.trim(), street.trim(), countryCode, ((postcode ?? "").trim() || (zip ?? "").trim()), { latitude, longitude });
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
    try {
      const frStartTs = Date.now();
      const requestedLotNorm = normalizeLot(aptNumber) || null;
      const normalizedRequestedLot = requestedLotNorm ? (requestedLotNorm.replace(/^0+/, "") || requestedLotNorm) : null;
      console.log("[FR_GOLD] request_start", {
        city: city.trim(),
        street: street.trim(),
        houseNumber: houseNumber.trim(),
        postcode: (postcode || zip || "").trim(),
        aptNumber: (aptNumber ?? "").trim(),
      });
      const bq = getGoldBigQueryClient();
      const country = "FR";
      const cityNorm = city.trim();
      const streetNorm = street.trim();
      const postcodeNorm = (postcode || zip || "").trim();
      const houseNumberNorm = houseNumber.trim();
      const unitNumberNorm = (aptNumber ?? "").trim();
      const propertyType = (searchParams.get("property_type") ?? searchParams.get("propertyType") ?? "").trim() || null;
      const inputSurfaceRaw = searchParams.get("surface_m2") ?? searchParams.get("surfaceM2");
      const inputSurfaceM2 = inputSurfaceRaw ? Number(inputSurfaceRaw) : NaN;
      const validInputSurfaceM2 = Number.isFinite(inputSurfaceM2) && inputSurfaceM2 > 0 ? inputSurfaceM2 : null;
      const queryWithTimeout = async <T,>(opts: Parameters<typeof bq.query>[0], label: string): Promise<T> => {
        const timeoutMs = 10000;
        return (await Promise.race([
          bq.query(opts),
          new Promise<never>((_, reject) => setTimeout(() => reject(new Error(`timeout:${label}`)), timeoutMs)),
        ])) as T;
      };
      const frReturn = (payload: Record<string, unknown>, tag: string, status?: number) => {
        console.log("[FR_GOLD] return", { tag, status: status ?? 200, durationMs: Date.now() - frStartTs });
        return NextResponse.json(payload, status ? { status } : undefined);
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

        const prefixes = ["RUE", "AVENUE", "AV", "BD", "BOULEVARD", "CHEMIN", "ROUTE", "IMPASSE"];
        const prefixRegex = new RegExp(`^(?:${prefixes.join("|")})\\.?\\s+`, "i");
        const cleaned = unified.replace(prefixRegex, "").replace(/\s+/g, " ").trim();
        return cleaned;
      };

      const streetNormalizedDet = normalizeStreetForDetection(streetNorm);

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
        WHERE LOWER(TRIM(city)) = LOWER(TRIM(@city))
          AND TRIM(postcode) = TRIM(@postcode)
          AND TRIM(CAST(house_number AS STRING)) = TRIM(CAST(@house_number AS STRING))
          AND (
            street_norm_clean LIKE CONCAT('%', @normalizedStreet, '%')
            OR @normalizedStreet LIKE CONCAT('%', street_norm_clean, '%')
          )
        ORDER BY unit_signal_count DESC, row_count DESC
        LIMIT 1
      `;

      console.log("[FR_GOLD] before_intelligence_detection_query");
      const [detectRows] = await queryWithTimeout<[Array<Record<string, unknown>>]>(
        {
          query: detectionQuery,
          params: {
            city: cityNorm,
            postcode: postcodeNorm,
            normalizedStreet: streetNormalizedDet,
            house_number: houseNumberNorm,
          },
        },
        "intelligence_detection_query"
      );
      console.log("[FR_GOLD] after_intelligence_detection_query", { rows: detectRows?.length ?? 0 });

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

      const detectedApartment = isMultiUnitDetected || apartmentFromType;
      const detectedHouse = isHouseLikeDetected || houseFromType;

      // Final apartment-vs-house decision rule (gold-table driven).
      // - If house-like evidence exists, run house-direct flow.
      // - Else if multi-unit / apartment-like evidence exists, ask for apartment/lot.
      const detectClass: "apartment" | "house" | "unclear" = detectedHouse ? "house" : detectedApartment ? "apartment" : "unclear";

      console.log("[FR_GOLD] intelligence_detection_computed", {
        isMultiUnitDetected,
        isHouseLikeDetected,
        detectedTypeStr,
        detectClass,
      });

      const candidateLots = getStringArray(detectRow, [
        "candidate_lots",
        "candidateLots",
        "available_lots",
        "availableLots",
        "lots",
        "candidate_lot",
      ]);

      console.log("[FR_GOLD] intelligence_apartment_vs_house", {
        detectClass,
        isMultiUnitDetected,
        isHouseLikeDetected,
        detectedTypeStr: detectedTypeStr || null,
        candidateLotsCount: candidateLots.length,
      });

      const exactQuery = `
        SELECT
          surface_m2,
          price_per_m2,
          last_sale_price,
          last_sale_date
        FROM \`streetiq-bigquery.streetiq_gold.property_latest_facts\`
        WHERE LOWER(TRIM(country)) = LOWER(TRIM(@country))
          AND LOWER(TRIM(city)) = LOWER(TRIM(@city))
          AND TRIM(postcode) = @postcode
          AND LOWER(TRIM(street)) = LOWER(TRIM(@street))
          AND TRIM(CAST(house_number AS STRING)) = TRIM(CAST(@house_number AS STRING))
          AND (@unit_number = '' OR TRIM(COALESCE(CAST(unit_number AS STRING), '')) = TRIM(CAST(@unit_number AS STRING)))
        LIMIT 1
      `;
      if (!unitNumberNorm) {
        if (detectClass === "apartment") {
          console.log("[FR_GOLD] apartment_lot_prompt_triggered");
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
      }

      console.log("[FR_GOLD] before_exact_query");
      const [exactRows] = await queryWithTimeout<[Array<{ surface_m2?: number; price_per_m2?: number; last_sale_price?: number; last_sale_date?: string | null }> ]>(
        {
          query: exactQuery,
          params: {
            country,
            city: cityNorm,
            postcode: postcodeNorm,
            street: streetNorm,
            house_number: houseNumberNorm,
            unit_number: unitNumberNorm,
          },
        },
        "exact_query"
      );
      console.log("[FR_GOLD] after_exact_query", { rows: (exactRows as any[])?.length ?? 0 });

      const exact = (exactRows as Array<{ surface_m2?: number; price_per_m2?: number; last_sale_price?: number; last_sale_date?: string | null }>)[0];
      if (exact) {
        const surface = Number(exact.surface_m2 ?? 0);
        const pricePerM2 = Number(exact.price_per_m2 ?? 0);
        const estimated = Number.isFinite(surface) && surface > 0 && Number.isFinite(pricePerM2) && pricePerM2 > 0
          ? Math.round(surface * pricePerM2)
          : null;
        const hasEstimated = estimated != null;
        return frReturn({
          address: { city: cityNorm, street: streetNorm, house_number: houseNumberNorm },
          data_source: "properties_france",
          fr_detect: detectClass,
          property_result: {
            exact_value: estimated,
            exact_value_message: estimated == null ? "No reliable data found" : null,
            value_level: "property-level",
            last_transaction: {
              amount: Number(exact.last_sale_price ?? 0) || 0,
              date: exact.last_sale_date ?? null,
              message: Number(exact.last_sale_price ?? 0) > 0 ? undefined : "No recent transaction available",
            },
            street_average: null,
            street_average_message: hasEstimated ? (detectClass === "apartment" ? "Exact apartment" : "Exact property") : "No reliable data found",
            livability_rating: "FAIR",
          },
          fr: emptyFranceResponse({
            success: hasEstimated,
            resultType: hasEstimated ? "exact_apartment" : "no_reliable_data",
            confidence: hasEstimated ? "high" : "low",
            requestedLot: requestedLotNorm,
            normalizedLot: normalizedRequestedLot,
            property: {
              transactionDate: exact.last_sale_date ?? null,
              transactionValue: Number(exact.last_sale_price ?? 0) || null,
              pricePerSqm: Number.isFinite(pricePerM2) && pricePerM2 > 0 ? pricePerM2 : null,
              surfaceArea: Number.isFinite(surface) && surface > 0 ? surface : null,
              rooms: null,
              propertyType: null,
              building: `${houseNumberNorm} ${streetNorm}`.trim() || null,
              postalCode: postcodeNorm || null,
              commune: cityNorm || null,
            },
            buildingStats: null,
            comparables: [],
            matchExplanation: hasEstimated ? "Exact property" : "No reliable data found",
          }),
        }, "exact_match");
      }

      // Same building fallback (when unit/lot was provided but exact unit match is missing):
      // Uses property_latest_facts with the same address key but without unit_number filtering.
      if (houseNumberNorm) {
        const buildingQuery = `
          SELECT
            surface_m2,
            price_per_m2,
            last_sale_price,
            last_sale_date
          FROM \`streetiq-bigquery.streetiq_gold.property_latest_facts\`
          WHERE LOWER(TRIM(country)) = LOWER(TRIM(@country))
            AND LOWER(TRIM(city)) = LOWER(TRIM(@city))
            AND TRIM(postcode) = @postcode
            AND LOWER(TRIM(street)) = LOWER(TRIM(@street))
            AND TRIM(CAST(house_number AS STRING)) = TRIM(CAST(@house_number AS STRING))
          LIMIT 1
        `;
        console.log("[FR_GOLD] before_building_query");
        const [buildingRows] = await queryWithTimeout<
          Array<{ surface_m2?: number; price_per_m2?: number; last_sale_price?: number; last_sale_date?: string | null }>
        >(
          {
            query: buildingQuery,
            params: {
              country,
              city: cityNorm,
              postcode: postcodeNorm,
              street: streetNorm,
              house_number: houseNumberNorm,
            },
          },
          "building_same_address_query"
        );
        console.log("[FR_GOLD] after_building_query", { rows: (buildingRows as any[])?.length ?? 0 });
        const buildingRow = (buildingRows as Array<{ surface_m2?: number; price_per_m2?: number; last_sale_price?: number; last_sale_date?: string | null }>)[0];
        if (buildingRow) {
          const surface = Number(buildingRow.surface_m2 ?? 0);
          const pricePerM2 = Number(buildingRow.price_per_m2 ?? 0);
          const estimated = Number.isFinite(surface) && surface > 0 && Number.isFinite(pricePerM2) && pricePerM2 > 0
            ? Math.round(surface * pricePerM2)
            : null;
          const hasEstimated = estimated != null;
          return frReturn(
            {
              address: { city: cityNorm, street: streetNorm, house_number: houseNumberNorm },
              data_source: "properties_france",
              fr_detect: detectClass,
              property_result: {
                exact_value: estimated,
                exact_value_message: estimated == null ? "No reliable data found" : null,
                value_level: "building-level",
                last_transaction: {
                  amount: Number(buildingRow.last_sale_price ?? 0) || 0,
                  date: buildingRow.last_sale_date ?? null,
                  message: Number(buildingRow.last_sale_price ?? 0) > 0 ? undefined : "No recent transaction available",
                },
                street_average: null,
                street_average_message: hasEstimated ? "Building-level estimate" : "No reliable data found",
                livability_rating: "FAIR",
              },
              fr: emptyFranceResponse({
                success: hasEstimated,
                resultType: hasEstimated ? "building_level" : "no_reliable_data",
                confidence: hasEstimated ? "medium" : "low",
                requestedLot: requestedLotNorm,
                normalizedLot: normalizedRequestedLot,
                property: null,
                buildingStats: { transactionCount: 1, avgPricePerSqm: null, avgTransactionValue: null },
                comparables: [],
                matchExplanation: hasEstimated ? "Building-level estimate (no exact unit match found)." : "No reliable data found",
              }),
            },
            "building_same_address_match"
          );
        }
      }

      // Use the gold-table driven apartment-vs-house decision for the valuation ladder.
      const frDetectToUse: "apartment" | "house" | "unclear" = detectClass;

      const fallbackStreetQuery = `
        SELECT
          avg_price_per_m2,
          newest_sale_date
        FROM \`streetiq-bigquery.streetiq_gold.property_area_fallback\`
        WHERE LOWER(TRIM(country)) = LOWER(TRIM(@country))
          AND LOWER(TRIM(city)) = LOWER(TRIM(@city))
          AND TRIM(postcode) = @postcode
          AND LOWER(TRIM(street)) = LOWER(TRIM(@street))
          AND (@property_type IS NULL OR LOWER(TRIM(property_type)) = LOWER(TRIM(@property_type)))
        LIMIT 1
      `;

      const fallbackCommuneStatsQuery = `
        SELECT
          avg_price_per_m2,
          newest_sale_date
        FROM \`streetiq-bigquery.streetiq_gold.france_commune_property_stats\`
        WHERE LOWER(TRIM(country)) = LOWER(TRIM(@country))
          AND LOWER(TRIM(city)) = LOWER(TRIM(@city))
          AND TRIM(postcode) = @postcode
        LIMIT 1
      `;

      console.log("[FR_GOLD] before_fallback_query", { level: "same_street" });
      const [fallbackStreetRows] = await queryWithTimeout<[Array<{ avg_price_per_m2?: number; newest_sale_date?: string | null }> ]>(
        {
          query: fallbackStreetQuery,
          params: {
            country,
            city: cityNorm,
            postcode: postcodeNorm,
            street: streetNorm,
            property_type: propertyType,
          },
        },
        "fallback_street_query"
      );
      console.log("[FR_GOLD] after_fallback_query", { level: "same_street", rows: (fallbackStreetRows as any[])?.length ?? 0 });

      let fallback = (fallbackStreetRows as Array<{ avg_price_per_m2?: number; newest_sale_date?: string | null }>)[0] ?? null;
      const fallbackSourceStreet = "Similar properties on same street";
      const fallbackSourceCommune = "Commune fallback";
      let fallbackSource = fallbackSourceStreet;

      if (!fallback) {
        console.log("[FR_GOLD] before_fallback_query", { level: "commune_stats" });
        const [fallbackCommuneRows] = await queryWithTimeout<[Array<{ avg_price_per_m2?: number; newest_sale_date?: string | null }> ]>(
          {
            query: fallbackCommuneStatsQuery,
            params: {
              country,
              city: cityNorm,
              postcode: postcodeNorm,
            },
          },
          "fallback_commune_stats_query"
        );
        console.log("[FR_GOLD] after_fallback_query", { level: "commune_stats", rows: (fallbackCommuneRows as any[])?.length ?? 0 });
        fallback = (fallbackCommuneRows as Array<{ avg_price_per_m2?: number; newest_sale_date?: string | null }>)[0] ?? null;
        fallbackSource = fallback ? fallbackSourceCommune : fallbackSource;
      }

      if (fallback) {
        const avgPrice = Number(fallback.avg_price_per_m2 ?? 0);
        const estimated = validInputSurfaceM2 != null && Number.isFinite(avgPrice) && avgPrice > 0
          ? Math.round(validInputSurfaceM2 * avgPrice)
          : null;
        const hasEstimated = estimated != null;
        const resolvedSourceLabel = hasEstimated ? fallbackSource : "No reliable data found";
        const frResultType = resolvedSourceLabel === fallbackSourceStreet ? "nearby_comparable" : "building_level";
        const frConfidence = resolvedSourceLabel === fallbackSourceStreet ? "low_medium" : "low";
        return frReturn({
          address: { city: cityNorm, street: streetNorm, house_number: houseNumberNorm },
          data_source: "properties_france",
          fr_detect: frDetectToUse,
          property_result: {
            exact_value: estimated,
            exact_value_message: estimated == null ? "No reliable data found" : null,
            value_level: resolvedSourceLabel === "No reliable data found" ? "no_match" : "street-level",
            last_transaction: {
              amount: 0,
              date: fallback.newest_sale_date ?? null,
              message: "No recent transaction available",
            },
            street_average: null,
            street_average_message: resolvedSourceLabel,
            livability_rating: "FAIR",
          },
          fr: emptyFranceResponse({
            success: hasEstimated,
            resultType: hasEstimated ? frResultType : "no_reliable_data",
            confidence: hasEstimated ? frConfidence : "low",
            requestedLot: requestedLotNorm,
            normalizedLot: normalizedRequestedLot,
            property: estimated != null
              ? {
                  transactionDate: fallback.newest_sale_date ?? null,
                  transactionValue: estimated,
                  pricePerSqm: Number.isFinite(avgPrice) && avgPrice > 0 ? avgPrice : null,
                  surfaceArea: validInputSurfaceM2,
                  rooms: null,
                  propertyType: propertyType,
                  building: `${houseNumberNorm} ${streetNorm}`.trim() || null,
                  postalCode: postcodeNorm || null,
                  commune: cityNorm || null,
                }
              : null,
            buildingStats: null,
            comparables: [],
            matchExplanation: hasEstimated ? fallbackSource : "No reliable data found",
          }),
        }, "fallback_match");
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
      }, "no_data");
    } catch (err) {
      console.log("[FR_GOLD] catch_error", { message: err instanceof Error ? err.message : "Unknown error" });
      const payload = {
        message: "Failed to fetch France property value",
        error: err instanceof Error ? err.message : "Unknown error",
        fr_detect: "unclear",
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
        const payload = {
          address: { city: city.trim(), street: street.trim(), house_number: houseNumber.trim() },
          data_source: "properties_france",
          multiple_units: true,
          prompt_for_apartment: true,
          result_level: "building",
          match_stage: result.matchStage,
          rows_at_stage: result.rowsAtStage,
          average_building_value: result.averageBuildingValue ?? 0,
          unit_count: result.unitCount ?? 0,
          building_sales: result.buildingSales,
          available_lots: result.availableLots ?? [],
          property_result: {
            exact_value: null,
            exact_value_message: "Multiple units found. Please enter apartment number to see exact value.",
            value_level: "building-level",
            last_transaction: { amount: 0, date: null, message: "No DVF data" },
            street_average: result.averageBuildingValue ?? null,
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
        const exactValue = result.currentValue ?? (lastTx?.value ?? null);
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
              amount: lastTx?.value ?? 0,
              date: lastTx?.date ?? null,
              message: lastTx?.value ? undefined : "No recorded transaction",
            },
            street_average: result.areaAverageValue,
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
        const buildingVal = result.averageBuildingValue ?? result.currentValue ?? 0;
        const payload = {
          address: { city: city.trim(), street: street.trim(), house_number: houseNumber.trim() },
          data_source: "properties_france",
          multiple_units: false,
          prompt_for_apartment: true,
          apartment_not_matched: true,
          average_building_value: buildingVal,
          building_sales: result.buildingSales,
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
            street_average: result.areaAverageValue,
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
                transactionValue: sim?.value ?? null,
                pricePerSqm: sim?.surface && sim?.value ? Math.round(((sim?.value ?? 0) / (sim?.surface ?? 1)) * 100) / 100 : null,
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
                  transactionValue: nearby?.value ?? null,
                  pricePerSqm: nearby?.surface && nearby?.value ? Math.round(((nearby?.value ?? 0) / (nearby?.surface ?? 1)) * 100) / 100 : null,
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
      const exactValue = result.currentValue ?? (lastTx?.value ?? null);
      const streetAvgDisplay = result.areaAverageValue;
      const streetAvgDisplayValue = streetAvgDisplay ?? 0;
      const isBuildingLevel = result.resultLevel === "building";
      const isAreaFallback = result.resultLevel === "commune_fallback" && streetAvgDisplayValue > 0;
      const hasRows = (result.rowsAtStage ?? 0) > 0;
      const matchStageHighEnough = (result.matchStage ?? 0) >= 3;
      const isExactProperty = result.resultLevel === "exact_property";
      const coerceToBuilding = hasRows && matchStageHighEnough && !isExactProperty;
      const hasExactValue = typeof exactValue === "number" && (exactValue ?? 0) > 0;
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

      const payload = {
        address: { city: city.trim(), street: street.trim(), house_number: houseNumber.trim() },
        data_source: "properties_france",
        multiple_units: coerceToBuilding,
        result_level: coerceToBuilding ? "building" : result.resultLevel,
        ...(coerceToBuilding ? { average_building_value: result.averageBuildingValue ?? result.currentValue ?? streetAvgDisplay ?? 0 } : {}),
        lot_number: result.lotNumber,
        surface_reelle_bati: result.surfaceReelleBati,
        date_mutation: lastTx?.date ?? null,
        building_sales: result.buildingSales ?? [],
        match_stage: result.matchStage,
        rows_at_stage: result.rowsAtStage,
        property_result: {
          exact_value: exactValue,
          exact_value_message: exactValueMessage,
          value_level: valueLevel,
          last_transaction: {
            amount: lastTx?.value ?? 0,
            date: lastTx?.date ?? null,
            message: lastTx?.value ? undefined : "No recorded transaction",
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
              const payload = {
                address: { city: city.trim(), street: street.trim(), house_number: houseNumber.trim() },
                data_source: "properties_france",
                multiple_units: true,
                prompt_for_apartment: true,
                apartment_not_matched: true,
                result_level: "building",
                average_building_value: buildingResult.averageBuildingValue ?? buildingResult.currentValue ?? 0,
                unit_count: buildingResult.unitCount ?? 0,
                building_sales: buildingResult.buildingSales ?? [],
                available_lots: buildingResult.availableLots ?? [],
                match_stage: buildingResult.matchStage,
                rows_at_stage: buildingResult.rowsAtStage,
                property_result: {
                  exact_value: buildingResult.averageBuildingValue ?? buildingResult.currentValue ?? null,
                  exact_value_message: `Apartment/lot ${aptTrimmed} was not found in DVF data. Showing building-level estimate.`,
                  value_level: "building-level" as const,
                  last_transaction: { amount: 0, date: null, message: "No recorded transaction for this apartment" },
                  street_average: buildingResult.areaAverageValue ?? null,
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
