"use client";

/**
 * Fetches property value insights from the exact-building-only API.
 * Uses ONLY official Israeli government data. Never returns street-level or nearby data.
 */

import { parseAddressFromFullString, parseUSAddressFromFullString } from "./address-parse";
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
  last_sale?: { price: number; date: string };
  sales_history?: Array<{ date: string; price: number }>;
  nearby_comps?: { avg_price: number; avg_price_per_sqft: number; count: number };
  property_details?: { beds?: number; baths?: number; sqft?: number; year_built?: number; property_type?: string };
  unit_required?: boolean;
  neighborhood_stats?: {
    median_home_value: number;
    median_household_income: number;
    population: number;
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
};

export async function fetchPropertyValueInsights(
  address: string,
  options?: FetchPropertyValueOptions
): Promise<PropertyValueInsightsResponse> {
  const lat = options?.latitude;
  const lng = options?.longitude;
  const key =
    Number.isFinite(lat) && Number.isFinite(lng)
      ? `${address.trim().toLowerCase()}|${lat}|${lng}`
      : address.trim().toLowerCase();
  const cached = CACHE.get(key);
  if (cached && Date.now() - cached.ts < CACHE_TTL_MS) {
    return cached.data;
  }

  const code = (options?.countryCode ?? "").toUpperCase();
  const parsed =
    code === "US"
      ? (() => {
          const us = parseUSAddressFromFullString(address);
          return { city: us.city, street: us.street, houseNumber: us.houseNumber };
        })()
      : parseAddressFromFullString(address);
  const fullAddress = address.trim() || undefined;
  if (!parsed.city || !parsed.street) {
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
    const res = await fetch(`/api/property-value?${params.toString()}`, {
      signal: AbortSignal.timeout(20000),
    });
    const data: PropertyValueInsightsResponse = await res.json().catch(() => ({
      message: "Invalid response",
      error: "PARSE_ERROR",
    }));

    if (res.ok && (data.address || data.avm_value || data.avm_rent || data.last_sale || data.neighborhood_stats)) {
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
