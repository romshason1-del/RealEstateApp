"use client";

/**
 * Fetches property value insights from the exact-building-only API.
 * Uses ONLY official Israeli government data. Never returns street-level or nearby data.
 */

import { parseAddressFromFullString } from "./address-parse";
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
    latest_building_transaction_price: number;
    average_apartment_value_today: number;
  } | null;
  explanation?: string;
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
  };
  message?: string;
  error?: string;
};

const CACHE = new Map<string, { data: PropertyValueInsightsResponse; ts: number }>();
const CACHE_TTL_MS = 5 * 60 * 1000;

export type FetchPropertyValueOptions = {
  latitude?: number;
  longitude?: number;
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

  const parsed = parseAddressFromFullString(address);
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

    if (res.ok && data.address) {
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
