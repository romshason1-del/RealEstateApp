/**
 * StreetIQ Property Value API
 * GET /api/property-value?city=...&street=...&houseNumber=...
 * GET /api/property-value?address=... (parsed to city, street, houseNumber)
 * Uses only official Israeli government real estate data (data.gov.il).
 * Returns results ONLY for exact building matches.
 */

import { NextRequest, NextResponse } from "next/server";
import getPropertyValueInsights from "@/lib/property-value-insights";
import { parseAddressFromFullString, parseUSAddressFromFullString, parseUKAddressFromFullString, extractFlatPrefix } from "@/lib/address-parse";
import { fetchNeighborhoodStats } from "@/lib/property-value-providers/us-census-provider";
import { fetchMarketTrend } from "@/lib/property-value-providers/us-fhfa-provider";
import { fetchUKHPIForLocality, fetchUKHPIIndicesForLocality, estimateValueFromHPI } from "@/lib/property-value-providers/uk-house-price-index-provider";
import { fetchUKNeighborhoodStats, computeUKLivabilityRating } from "@/lib/property-value-providers/uk-ons-census-provider";
import { fetchEPCFloorArea, fetchEPCFloorAreasForArea, isEPCConfigured } from "@/lib/property-value-providers/uk-epc-provider";
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

const ROUTE_TIMEOUT_MS = 6000;
const ROUTE_TIMEOUT_MS_UK = 20000;
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
      livability_rating: "BAD" as const,
    },
  };
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
  const isUK = (countryCode ?? "").toUpperCase() === "UK" || (countryCode ?? "").toUpperCase() === "GB";
  const raw = (isUK && rawInputAddress.trim()) ? `|raw:${rawInputAddress.trim()}` : "";
  const sel = (isUK && selectedFormattedAddress.trim()) ? `|sel:${selectedFormattedAddress.trim()}` : "";
  // UK: cache key includes raw+selected so same address always yields same cached response (including level)
  const cacheKey = buildCacheKey(city, street, houseNumber, latitude, longitude, state, zip, ukPostcode) + raw + sel;
  const isUS = (countryCode ?? "").toUpperCase() === "US";
  const usMockMode = isUS && isUSMockEnabled();

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
        : hasBuildingMatch
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
        let ukLivability: "BAD" | "ALMOST GOOD" | "GOOD" | "VERY GOOD" | "EXCELLENT" = "BAD";
        try {
          const ukStats = await withTimeout(
            fetchUKNeighborhoodStats(postcode.trim() || "", ukLandRegistry.average_area_price ?? undefined),
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
        const noMatchLevel = ukNoPropertyRecord ? "no_match" : "area-level";
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
      let livabilityRating: "BAD" | "ALMOST GOOD" | "GOOD" | "VERY GOOD" | "EXCELLENT" = "BAD";
      if (income > 0 || homeVal > 0) {
        const score = (income >= 100000 ? 4 : income >= 75000 ? 3 : income >= 50000 ? 2 : income >= 35000 ? 1 : 0) +
          (incGrowth > 2 ? 0.5 : incGrowth > 0 ? 0.25 : 0) +
          (popGrowth > 0 ? 0.25 : 0) +
          (pctBachelors >= 0.4 ? 0.5 : pctBachelors >= 0.25 ? 0.25 : 0);
        if (score >= 4) livabilityRating = "EXCELLENT";
        else if (score >= 3) livabilityRating = "VERY GOOD";
        else if (score >= 2) livabilityRating = "GOOD";
        else if (score >= 1) livabilityRating = "ALMOST GOOD";
        else livabilityRating = "BAD";
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
          livability_rating: "BAD" as const,
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
      const valueLevel = (hasExactFlatMatch && latestTx != null && latestTx.price > 0
        ? "property-level"
        : hasBuildingMatch
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

      let livabilityRating: "BAD" | "ALMOST GOOD" | "GOOD" | "VERY GOOD" | "EXCELLENT" = "BAD";
      try {
        const ukStats = await withTimeout(
          fetchUKNeighborhoodStats(postcode.trim() || "", areaPrice ?? undefined),
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

  const routeTimeoutMs = isUK ? ROUTE_TIMEOUT_MS_UK : ROUTE_TIMEOUT_MS;
  const timeoutResponse = new Promise<Response>((resolve) => {
    setTimeout(() => {
      if (process.env.NODE_ENV === "development") console.debug("[property-value] Route timeout, returning minimal response");
      resolve(NextResponse.json(isUK ? buildUKMinimalResponse() : { message: "Request timeout", error: "TIMEOUT" }, isUK ? { status: 200 } : { status: 503 }));
    }, routeTimeoutMs);
  });

  return Promise.race([runHandler(), timeoutResponse]);
}
