"use client";

/**
 * Fetches property value insights from the exact-building-only API.
 * Uses ONLY official Israeli government data. Never returns street-level or nearby data.
 */

import { parseAddressFromFullString, parseUSAddressFromFullString, parseUKAddressFromFullString, parseFRAddressFromFullString } from "./address-parse";
import { NYC_CANDIDATE_GENERATOR_VERSION } from "./us/us-nyc-address-normalize";
import { toCanonicalAddress } from "./address-canonical";

export type PropertyValueInsightsResponse = {
  address?: { city: string; street: string; house_number: string };
  match_quality?: "exact_building" | "exact_property" | "nearby_building" | "no_reliable_match";
  latest_transaction?: {
    transaction_date: string;
    transaction_price: number;
    property_size: number;
    price_per_m2: number;
  };
  current_estimated_value?: {
    estimated_value: number;
    estimated_price_per_m2: number;
    estimation_method: string;
  } | null;
  building_summary_last_3_years?: {
    transactions_count_last_3_years: number;
    transactions_count_last_5_years?: number;
    latest_building_transaction_price: number;
    average_apartment_value_today: number;
  } | null;
  explanation?: string;
  avm_value?: number;
  avm_rent?: number;
  /** US: Value range for display. estimated_value is primary; low/high show spread. */
  value_range?: { low_estimate: number; estimated_value: number; high_estimate: number };
  /** US: Human-readable source summary, e.g. "Based on Zillow + Redfin + Census + FHFA" */
  source_summary?: string;
  /** US: When source data was last updated, e.g. "Jan 2025" or "Updated monthly" */
  last_market_update?: string;
  /** US: True when value is from area-level data only (Census/median_sale_price), not property-specific */
  is_area_level_estimate?: boolean;
  /** US: Simplified 4-line property result */
  property_result?: {
    exact_value: number | null;
    exact_value_message: string | null;
    value_level: "property-level" | "building-level" | "street-level" | "area-level" | "no_match";
    last_transaction: { amount: number; date: string | null; message?: string };
    street_average: number | null;
    street_average_message: string | null;
    livability_rating: "POOR" | "FAIR" | "GOOD" | "VERY GOOD" | "EXCELLENT";
  };
  last_sale?: { price: number; date: string };
  sales_history?: Array<{ date: string; price: number }>;
  /** Global: Most recent recorded sale for the searched property when available */
  last_recorded_sale?: { price: number; date: string; source?: string };
  nearby_comps?: { avg_price: number; avg_price_per_sqft: number; count: number };
  /** Individual nearby or area sales. When is_same_property=true, these are sales for the searched property. */
  nearby_sales?: Array<{
    address: string;
    price: number;
    date: string;
    distance_m?: number;
    price_per_sqft?: number;
    is_same_property?: boolean;
  }>;
  property_details?: { beds?: number; baths?: number; sqft?: number; year_built?: number; property_type?: string };
  unit_required?: boolean;
  neighborhood_stats?: {
    median_home_value: number;
    median_household_income: number;
    population: number;
    median_rent?: number;
    population_growth_percent?: number;
    income_growth_percent?: number;
  };
  investment_metrics?: {
    median_rent: number;
    gross_rent_yield_percent: number;
    median_price_per_sqft?: number;
    /** @deprecated Use gross_rent_yield_percent. Kept for backward compatibility. */
    estimated_roi_percent?: number;
  };
  data_source?:
    | "live"
    | "cache"
    | "mock"
    | "us_nyc_truth"
    | "us_nyc_app_output_v4"
    | "us_nyc_app_output_v5"
    | "us_nyc_property_ui_production_v10";
  /** NYC production UI table: server-normalized; UI must not recompute. */
  nyc_display_hierarchy?: "EXACT" | "BUILDING" | "STREET" | "NONE";
  nyc_match_confidence?: "HIGH" | "LOW" | "NONE" | "MEDIUM";
  nyc_has_exact_transaction?: boolean;
  nyc_show_street_reference?: boolean;
  nyc_street_reference?: { price: number | null; date: string | null; source_address: string | null } | null;
  nyc_show_search_another_cta?: boolean;
  nyc_neighborhood_score?: string | null;
  nyc_building_type_display?: string | null;
  /** Raw BigQuery row when returned by `/api/us/nyc-app-output` (omitted from production debug stripping only for nested debug keys). */
  row?: Record<string, unknown> | null;
  /** NYC v4: true when BigQuery returned a row for this lookup (set by adapter). */
  nyc_bq_row_matched?: boolean;
  market_trend?: { hpi_index: number; change_1y_percent: number; latest_date?: string };
  fr_dvf?: { transaction_count: number; radius_used_m: number; price_per_sqm: number | null };
  /** France: true when multiple units found at address; user must enter apt number */
  multiple_units?: boolean;
  /** France: average value across units when multiple_units */
  average_building_value?: number;
  /** France: built surface in m² for price/sqm calculation */
  surface_reelle_bati?: number | null;
  /** France: sale date (YYYY-MM-DD) for "Sold in: Oct 2023" display */
  date_mutation?: string | null;
  /** France: last 5 sales in this building (Date, Type, Price, Surface) */
  building_sales?: Array<{ date: string | null; type: string; price: number; surface: number | null; lot_number?: string | null }>;
  /** France: ETL helper `france_multi_unit_transactions` — combined/aggregated sale disclosure */
  multi_unit_transaction?: boolean;
  /** France: distinct units from helper row (debug / transparency) */
  multi_unit_distinct_unit_count?: number | null;
  /** France: product display tier from winning_step (exact_unit, exact_address, building_level, street_level, area_level). */
  fr_display_context?:
    | "exact_unit"
    | "exact_address"
    | "building_level"
    | "street_level"
    | "area_level"
    | "unknown";
  unit_count?: number;
  uk_land_registry?: {
    building_average_price: number | null;
    transactions_in_building: number;
    latest_building_transaction: { price: number; date: string; property_type?: string } | null;
    latest_nearby_transaction?: { price: number; date: string; property_type?: string } | null;
    has_building_match: boolean;
    average_area_price: number | null;
    area_transaction_count: number;
    area_fallback_level: "postcode" | "outward_postcode" | "postcode_area" | "street" | "locality" | "none";
    fallback_level_used?: "building" | "postcode" | "locality" | "area";
  };
  debug?: {
    raw_input_address: { city: string; street: string; house_number: string };
    canonical_address?: { city_key: string; street_key: string; house_key: string };
    records_fetched?: number;
    records_after_filter?: number;
    exact_matches_count?: number;
    nearby_matches_count?: number;
    street_matches_found?: string[];
    building_numbers_found?: string[];
    distance_from_requested_m?: number;
    dataset_sample?: Array<{ city: string; street: string; house_number: string; canonical: { city_key: string; street_key: string; house_key: string } }>;
    rejection_reason?: string;
    api_status?: number;
    api_error?: string;
    records_returned?: number;
    raw_dataset_sample?: Record<string, unknown>[];
    dataset_id?: string;
    resource_id_selected?: string;
    datastore_active?: boolean;
    property_found?: boolean;
    avm_value_found?: boolean;
    avm_rent_found?: boolean;
    sales_history_found?: boolean;
    comps_found?: boolean;
    market_value_source?: string;
    price_per_sqft_used?: number;
    unit_required?: boolean;
    fallback_level_used?: string;
  };
  message?: string;
  error?: string;
};

const CACHE = new Map<string, { data: PropertyValueInsightsResponse; ts: number }>();
const CACHE_TTL_MS = 5 * 60 * 1000;

export type FetchPropertyValueOptions = {
  latitude?: number;
  longitude?: number;
  countryCode?: string;
  /** Optional abort signal (used by hooks to prevent stale updates). */
  signal?: AbortSignal;
  /** UK only: raw typed input (preserves Flat/Unit) */
  rawInputAddress?: string;
  /** UK only: Google formatted_address from selected suggestion */
  selectedFormattedAddress?: string;
  /** France: apartment/lot number for multi-unit buildings */
  aptNumber?: string;
  /** France: postcode from Google address_components (avoids "Postcode required") */
  postcode?: string;
  /** US NYC: optional apartment/lot for precomputed unit-level lookup (cache key + query param). */
  unitOrLot?: string;
};

export async function fetchPropertyValueInsights(
  address: string,
  options?: FetchPropertyValueOptions
): Promise<PropertyValueInsightsResponse> {
  const lat = options?.latitude;
  const lng = options?.longitude;
  const code = (options?.countryCode ?? "").toUpperCase();
  const isUK = code === "UK" || code === "GB";
  const isFR = code === "FR";
  const raw = (isUK && options?.rawInputAddress) ? `|raw:${options.rawInputAddress.trim()}` : "";
  const sel = (isUK && options?.selectedFormattedAddress) ? `|sel:${options.selectedFormattedAddress.trim()}` : "";
  const frRaw = (isFR && options?.rawInputAddress) ? `|raw:${options.rawInputAddress.trim()}` : "";
  const apt = (options?.aptNumber ?? "").trim();
  const frPostcode = (isFR && options?.postcode) ? `|pc:${options.postcode.trim()}` : "";
  const usUnitKey =
    code === "US" && (options?.unitOrLot ?? "").trim()
      ? `|uol:${(options?.unitOrLot ?? "").trim()}`
      : "";
  const usNycNormKey = code === "US" ? `|nycv:${NYC_CANDIDATE_GENERATOR_VERSION}` : "";
  const normalizeFranceAddress = (a: string) =>
    a
      .trim()
      .toLowerCase()
      .replace(/\s+/g, " ")
      .replace(/\bprom\.?\b/gi, "promenade")
      .replace(/\bav\.\b/gi, "avenue")
      .replace(/\bbd\.\b/gi, "boulevard")
      .replace(/,/g, " ");
  const addrForKey = isFR ? normalizeFranceAddress(address) : address.trim().toLowerCase();
  const key =
    Number.isFinite(lat) && Number.isFinite(lng)
      ? `${addrForKey}${raw}${sel}${frRaw}${apt ? `|apt:${apt}` : ""}${frPostcode}${usUnitKey}${usNycNormKey}|${lat}|${lng}${isFR ? "|final_v1" : ""}`
      : `${addrForKey}${raw}${sel}${frRaw}${apt ? `|apt:${apt}` : ""}${frPostcode}${usUnitKey}${usNycNormKey}${isFR ? "|final_v1" : ""}`;
  const cached = CACHE.get(key);
  const frRawPresent = isFR && !!(options?.rawInputAddress ?? "").trim();
  const frCachedNoData =
    isFR &&
    cached &&
    (() => {
      const d = cached.data as Record<string, unknown>;
      const rd = d?.fr_runtime_debug as Record<string, unknown> | undefined;
      return String(rd?.winning_step ?? "") === "no_data" || (d?.fr as Record<string, unknown> | undefined)?.resultType === "no_result";
    })();
  const bypassCache = frRawPresent && frCachedNoData;
  let didBypassCache = false;
  if (cached && Date.now() - cached.ts < CACHE_TTL_MS) {
    if (!bypassCache) {
      const cachedData = cached.data as Record<string, unknown>;
      if (isFR && cachedData && typeof cachedData === "object") {
        const rd = (cachedData.fr_runtime_debug ?? {}) as Record<string, unknown>;
        cachedData.fr_runtime_debug = { ...rd, fr_cache_hit: true, fr_cache_bypass_reason: null };
      }
      return cached.data;
    }
    didBypassCache = true;
  }

  const parsed =
    code === "US"
      ? (() => {
          const us = parseUSAddressFromFullString(address);
          return { city: us.city, street: us.street, houseNumber: us.houseNumber, postcode: "" };
        })()
      : isUK
        ? (() => {
            const uk = parseUKAddressFromFullString(address);
            return { city: uk.city, street: uk.street, houseNumber: uk.houseNumber, postcode: uk.postcode };
          })()
        : code === "IT"
          ? (() => {
              const g = parseAddressFromFullString(address);
              const city = g.city || g.street;
              return { city, street: g.city ? g.street : "", houseNumber: g.houseNumber, postcode: "" };
            })()
          : code === "FR"
            ? (() => {
                const g = parseFRAddressFromFullString(address);
                return { city: g.city, street: g.street, houseNumber: g.houseNumber, postcode: g.postcode };
              })()
            : (() => {
              const g = parseAddressFromFullString(address);
              return { city: g.city, street: g.street, houseNumber: g.houseNumber, postcode: "" };
            })();
  const fullAddress = address.trim() || undefined;
  if (isUK) {
    const hasStreetAndCity = !!(parsed.street.trim() && parsed.city.trim());
    if (!parsed.postcode && !hasStreetAndCity) {
      return {
        message: "UK Land Registry requires a postcode or street and town. Could not parse from address.",
        debug: { raw_input_address: { city: parsed.city, street: parsed.street, house_number: parsed.houseNumber } },
      };
    }
  }
  const isIT = code === "IT";
  if (!isIT && !isFR && code !== "US" && (!parsed.city || !parsed.street)) {
    return {
      message: "no reliable exact match found",
      debug: {
        raw_input_address: { city: parsed.city, street: parsed.street, house_number: parsed.houseNumber },
        canonical_address: (() => {
          const c = toCanonicalAddress(parsed.city, parsed.street, parsed.houseNumber);
          return { city_key: c.cityKey, street_key: c.streetKey, house_key: c.houseKey };
        })(),
        records_fetched: 0,
        records_after_filter: 0,
        exact_matches_count: 0,
        rejection_reason: "Could not parse city and street from address",
      },
    };
  }
  if (isIT && !parsed.city) {
    return {
      message: "City required for Italy. Could not parse from address.",
      debug: { raw_input_address: { city: parsed.city, street: parsed.street, house_number: parsed.houseNumber } },
    };
  }
  if (isFR) {
    const hasPostcodeOrStreet = !!(parsed.postcode?.trim() || parsed.street?.trim());
    const hasRawInput = !!(options?.rawInputAddress ?? "").trim();
    if (!hasPostcodeOrStreet && !hasRawInput) {
      return {
        message: "Postcode or street name required for France. Could not parse from address.",
        debug: { raw_input_address: { city: parsed.city, street: parsed.street, house_number: parsed.houseNumber } },
      };
    }
    // When rawInputAddress is provided, always call API - server can parse and attempt raw BAN lookup.
  }

  try {
    let addressParam = address.trim();
    if (code === "US" && addressParam.includes("Long Island City")) {
      addressParam = addressParam.replace(/Long Island City/g, "Queens");
    }
    const params = new URLSearchParams({ address: addressParam });
    if (options?.countryCode) params.set("countryCode", options.countryCode);
    if (options?.latitude != null && Number.isFinite(options.latitude)) {
      params.set("latitude", String(options.latitude));
    }
    if (options?.longitude != null && Number.isFinite(options.longitude)) {
      params.set("longitude", String(options.longitude));
    }
    if (isUK && options?.rawInputAddress) params.set("rawInputAddress", options.rawInputAddress);
    if (isUK && options?.selectedFormattedAddress) params.set("selectedFormattedAddress", options.selectedFormattedAddress);
    if (isFR && options?.rawInputAddress) params.set("rawInputAddress", options.rawInputAddress);
    if (apt) params.set("apt_number", apt);
    if (isFR && options?.postcode?.trim()) params.set("postcode", options.postcode.trim());
    if (code === "US" && options?.unitOrLot?.trim()) params.set("unit_or_lot", options.unitOrLot.trim());
    // US / NYC: `real_estate_us.us_nyc_app_output_final_v5` via dedicated route (not `/api/property-value` truth pipeline).
    const apiPath = code === "US" ? `/api/us/nyc-app-output?${params.toString()}` : `/api/property-value?${params.toString()}`;
    if (code === "US" && process.env.NODE_ENV === "development") {
      console.log("[NYC_SEARCH_DEBUG]", {
        phase: "fetchPropertyValueInsights",
        addressSentToNycApi: addressParam,
      });
    }
    const res = await fetch(apiPath, {
      signal: options?.signal ?? AbortSignal.timeout(code === "FR" ? 60000 : 20000),
    });
    const data: PropertyValueInsightsResponse = await res.json().catch(() => ({
      message: "Invalid response",
      error: "PARSE_ERROR",
    }));

    const isUKTimeoutFallback = isUK && (data as { debug?: { failure_reason?: string } }).debug?.failure_reason === "Land Registry timeout";
    const frHasRealData =
      isFR &&
      (data.multiple_units === true ||
        (Array.isArray(data.building_sales) && data.building_sales.length > 0) ||
        ((data.average_building_value ?? 0) > 0) ||
        (data.property_result &&
          data.property_result.value_level !== "no_match" &&
          ((data.property_result.street_average ?? 0) > 0 || (data.property_result.exact_value ?? 0) > 0)));
    const usNycTruthOk =
      code === "US" &&
      data &&
      typeof data === "object" &&
      ((data as { data_source?: string }).data_source === "us_nyc_truth" ||
        (data as { data_source?: string }).data_source === "us_nyc_app_output_v4" ||
        (data as { data_source?: string }).data_source === "us_nyc_app_output_v5" ||
        (data as { data_source?: string }).data_source === "us_nyc_property_ui_production_v10") &&
      (data as { success?: boolean }).success === true;
    const hasValidData =
      frHasRealData ||
      usNycTruthOk ||
      (!isFR &&
        (data.address ||
          data.avm_value ||
          data.avm_rent ||
          data.last_sale ||
          data.property_result ||
          data.neighborhood_stats ||
          data.uk_land_registry));
    if (res.ok && !isUKTimeoutFallback && hasValidData) {
      CACHE.set(key, { data, ts: Date.now() });
    }
    // France no_data with rawInputAddress is never cached (avoids stale no_data for typed addresses)
    if (isFR && didBypassCache && data && typeof data === "object") {
      const d = data as Record<string, unknown>;
      const rd = (d.fr_runtime_debug ?? {}) as Record<string, unknown>;
      d.fr_runtime_debug = { ...rd, fr_cache_bypass_reason: "typed_address_no_data_bypass" };
    }

    return data;
  } catch (err) {
    console.error("[fetchPropertyValueInsights]", err);
    return {
      message: "Failed to fetch property value insights.",
      error: err instanceof Error ? err.message : "Connection failed",
    };
  }
}
