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
      } | undefined;
      const income = nsForLivability?.median_household_income ?? 0;
      const homeVal = nsForLivability?.median_home_value ?? 0;
      const popGrowth = nsForLivability?.population_growth_percent ?? 0;
      const incGrowth = nsForLivability?.income_growth_percent ?? 0;
      let livabilityRating: "BAD" | "ALMOST GOOD" | "GOOD" | "VERY GOOD" | "EXCELLENT" = "BAD";
      if (income > 0 || homeVal > 0) {
        const score = (income >= 100000 ? 4 : income >= 75000 ? 3 : income >= 50000 ? 2 : income >= 35000 ? 1 : 0) +
          (incGrowth > 2 ? 0.5 : incGrowth > 0 ? 0.25 : 0) +
          (popGrowth > 0 ? 0.25 : 0);
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
      if (dataSrc.includes("Zillow")) parts.push("Zillow");
      if (dataSrc.includes("Redfin")) parts.push("Redfin");
      if (r.neighborhood_stats != null && typeof r.neighborhood_stats === "object") parts.push("Census");
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
