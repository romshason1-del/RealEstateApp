/**
 * StreetIQ Property Value API
 * GET /api/property-value?city=...&street=...&houseNumber=...
 * GET /api/property-value?address=... (parsed to city, street, houseNumber)
 * Uses only official Israeli government real estate data (data.gov.il).
 * Returns results ONLY for exact building matches.
 */

import { NextRequest, NextResponse } from "next/server";
import getPropertyValueInsights from "@/lib/property-value-insights";
import { parseAddressFromFullString, parseUSAddressFromFullString, parseUKAddressFromFullString } from "@/lib/address-parse";
import { fetchNeighborhoodStats } from "@/lib/property-value-providers/us-census-provider";
import { fetchMarketTrend } from "@/lib/property-value-providers/us-fhfa-provider";
import { fetchUKHPIForLocality } from "@/lib/property-value-providers/uk-house-price-index-provider";
import { isUSMockEnabled } from "@/lib/property-value-providers/config";

const CACHE = new Map<string, { data: Record<string, unknown>; ts: number }>();
const CACHE_TTL_MS = 5 * 60 * 1000;
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
  postcode?: string
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

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  let city = searchParams.get("city") ?? "";
  let street = searchParams.get("street") ?? "";
  let houseNumber = searchParams.get("houseNumber") ?? searchParams.get("house_number") ?? "";
  let state = searchParams.get("state") ?? "";
  let zip = searchParams.get("zip") ?? searchParams.get("zipCode") ?? "";
  let postcode = searchParams.get("postcode") ?? searchParams.get("postCode") ?? "";
  const addressParam = searchParams.get("address") ?? "";
  const countryCode = searchParams.get("countryCode") ?? searchParams.get("country") ?? "IL";
  const latParam = searchParams.get("latitude");
  const lngParam = searchParams.get("longitude");
  const latitude = latParam ? parseFloat(latParam) : undefined;
  const longitude = lngParam ? parseFloat(lngParam) : undefined;

  if (addressParam) {
    const code = (countryCode ?? "").toUpperCase();
    if (code === "US") {
      const parsed = parseUSAddressFromFullString(addressParam);
      city = parsed.city || city;
      street = parsed.street || street;
      houseNumber = parsed.houseNumber || houseNumber;
      state = parsed.state || state;
      zip = parsed.zip || zip;
    } else if (code === "UK" || code === "GB") {
      const parsed = parseUKAddressFromFullString(addressParam);
      street = parsed.street || street;
      city = parsed.city || city;
      postcode = parsed.postcode || postcode;
      houseNumber = parsed.houseNumber || houseNumber;
    } else {
      if (!city || !street) {
        const parsed = parseAddressFromFullString(addressParam);
        if (parsed.city) city = city || parsed.city;
        if (parsed.street) street = street || parsed.street;
        if (parsed.houseNumber) houseNumber = houseNumber || parsed.houseNumber;
      }
    }
  }

  const validation = validateInput(city.trim(), street.trim(), countryCode, postcode.trim() || zip.trim());
  if (!validation.valid) {
    return NextResponse.json(
      { message: validation.error, error: "INVALID_INPUT" },
      { status: 400 }
    );
  }

  const ukPostcode = (countryCode ?? "").toUpperCase() === "UK" || (countryCode ?? "").toUpperCase() === "GB"
    ? (postcode.trim() || zip.trim())
    : undefined;
  const cacheKey = buildCacheKey(city, street, houseNumber, latitude, longitude, state, zip, ukPostcode);
  const isUS = (countryCode ?? "").toUpperCase() === "US";
  const usMockMode = isUS && isUSMockEnabled();

  const cached = CACHE.get(cacheKey);
  if (cached && Date.now() - cached.ts < CACHE_TTL_MS) {
    const cachedResponse = { ...cached.data, data_source: "cache" as const };
    return NextResponse.json(cachedResponse);
  }

  try {
    const result = await getPropertyValueInsights(
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
      },
      countryCode
    );

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

    const isUK = (countryCode ?? "").toUpperCase() === "UK" || (countryCode ?? "").toUpperCase() === "GB";

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
          const hpiResult = await fetchUKHPIForLocality(city.trim(), postcode.trim() || undefined);
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
        const augmented = {
          ...noMatchResult,
          address: { city: city.trim() || postcode.trim(), street: street.trim() || postcode.trim(), house_number: houseNumber.trim() },
          uk_land_registry: ukLandRegistry,
          data_source: "live" as const,
        };
        CACHE.set(cacheKey, { data: augmented, ts: Date.now() });
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
      Number.isFinite(longitude)
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
        response = { ...response, market_trend: { hpi_index: 412.3, change_1y_percent: 4.2 } };
      }
    }

    if (isUK && response.uk_land_registry && typeof response.uk_land_registry === "object") {
      const uk = response.uk_land_registry as {
        average_area_price?: number | null;
        area_transaction_count?: number;
        area_data_source?: string;
      };
      const hasNoUsableAreaData =
        (uk.average_area_price == null || uk.average_area_price <= 0) && (uk.area_transaction_count ?? 0) === 0;
      if (hasNoUsableAreaData) {
        try {
          const hpiResult = await fetchUKHPIForLocality(city.trim(), postcode.trim() || undefined);
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
      },
      { status: 500 }
    );
  }
}
