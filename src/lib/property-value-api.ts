"use client";

/**
 * Fetches property value insights from the exact-building-only API.
 * Uses ONLY official Israeli government data. Never returns street-level or nearby data.
 */

import { parseAddressFromFullString, parseUSAddressFromFullString, parseUKAddressFromFullString } from "./address-parse";
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
  data_source?: "live" | "cache" | "mock";
  market_trend?: { hpi_index: number; change_1y_percent: number; latest_date?: string };
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
  /** UK only: raw typed input (preserves Flat/Unit) */
  rawInputAddress?: string;
  /** UK only: Google formatted_address from selected suggestion */
  selectedFormattedAddress?: string;
};

export async function fetchPropertyValueInsights(
  address: string,
  options?: FetchPropertyValueOptions
): Promise<PropertyValueInsightsResponse> {
  const lat = options?.latitude;
  const lng = options?.longitude;
  const code = (options?.countryCode ?? "").toUpperCase();
  const isUK = code === "UK" || code === "GB";
  const raw = (isUK && options?.rawInputAddress) ? `|raw:${options.rawInputAddress.trim()}` : "";
  const sel = (isUK && options?.selectedFormattedAddress) ? `|sel:${options.selectedFormattedAddress.trim()}` : "";
  const key =
    Number.isFinite(lat) && Number.isFinite(lng)
      ? `${address.trim().toLowerCase()}${raw}${sel}|${lat}|${lng}`
      : `${address.trim().toLowerCase()}${raw}${sel}`;
  const cached = CACHE.get(key);
  if (cached && Date.now() - cached.ts < CACHE_TTL_MS) {
    return cached.data;
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
  } else if (!parsed.city || !parsed.street) {
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

  try {
    const params = new URLSearchParams({ address });
    if (options?.countryCode) params.set("countryCode", options.countryCode);
    if (options?.latitude != null && Number.isFinite(options.latitude)) {
      params.set("latitude", String(options.latitude));
    }
    if (options?.longitude != null && Number.isFinite(options.longitude)) {
      params.set("longitude", String(options.longitude));
    }
    if (isUK && options?.rawInputAddress) params.set("rawInputAddress", options.rawInputAddress);
    if (isUK && options?.selectedFormattedAddress) params.set("selectedFormattedAddress", options.selectedFormattedAddress);
    const res = await fetch(`/api/property-value?${params.toString()}`, {
      signal: AbortSignal.timeout(20000),
    });
    const data: PropertyValueInsightsResponse = await res.json().catch(() => ({
      message: "Invalid response",
      error: "PARSE_ERROR",
    }));

    const isUKTimeoutFallback = isUK && (data as { debug?: { failure_reason?: string } }).debug?.failure_reason === "Land Registry timeout";
    if (res.ok && !isUKTimeoutFallback && (data.address || data.avm_value || data.avm_rent || data.last_sale || data.property_result || data.neighborhood_stats || data.uk_land_registry)) {
      CACHE.set(key, { data, ts: Date.now() });
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
