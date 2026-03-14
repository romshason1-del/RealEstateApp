/**
 * Geocoding API with Supabase cache.
 * 1. Check cached_locations in Supabase first.
 * 2. If not found, fetch from Google Maps Geocoding API.
 * 3. Save result to cached_locations on miss.
 */

import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

const GEOCODE_BASE = "https://maps.googleapis.com/maps/api/geocode/json";

function getSupabaseClient() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY ?? "";
  if (!url || !key) return null;
  return createClient(url, key, {
    auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false },
  });
}

function buildLookupKey(
  type: "address" | "place_id" | "reverse",
  address?: string,
  placeId?: string,
  lat?: number,
  lng?: number
): string {
  if (type === "address" && address) {
    return address.toLowerCase().trim();
  }
  if (type === "place_id" && placeId) {
    return placeId;
  }
  if (type === "reverse" && Number.isFinite(lat) && Number.isFinite(lng)) {
    return `${Number(lat).toFixed(5)},${Number(lng).toFixed(5)}`;
  }
  return "";
}

function cachedToGeocodeResult(cached: {
  formatted_address: string | null;
  lat: number | null;
  lng: number | null;
  address_components: unknown;
}) {
  const lat = Number(cached.lat) || 0;
  const lng = Number(cached.lng) || 0;
  return {
    results: [
      {
        formatted_address: cached.formatted_address ?? undefined,
        address_components: cached.address_components as unknown[] | undefined,
        geometry: {
          location: { lat, lng },
        },
      },
    ],
    status: "OK",
  };
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const raw = body as {
      address?: string;
      placeId?: string;
      lat?: number;
      lng?: number;
      location?: { lat: number; lng: number };
    };
    const address = raw.address;
    const placeId = raw.placeId;
    const lat = raw.lat ?? raw.location?.lat;
    const lng = raw.lng ?? raw.location?.lng;

    const apiKey = (process.env.NEXT_PUBLIC_GOOGLE_MAPS_API_KEY ?? "").trim();
    if (!apiKey) {
      return NextResponse.json({ error: "Google API key not configured" }, { status: 503 });
    }

    let lookupType: "address" | "place_id" | "reverse";
    let lookupKey: string;

    if (address && typeof address === "string") {
      lookupType = "address";
      lookupKey = buildLookupKey("address", address);
    } else if (placeId && typeof placeId === "string") {
      lookupType = "place_id";
      lookupKey = buildLookupKey("place_id", undefined, placeId);
    } else if (Number.isFinite(lat) && Number.isFinite(lng)) {
      lookupType = "reverse";
      lookupKey = buildLookupKey("reverse", undefined, undefined, lat, lng);
    } else {
      return NextResponse.json({ error: "Provide address, placeId, or lat/lng" }, { status: 400 });
    }

    if (!lookupKey) {
      return NextResponse.json({ error: "Invalid lookup parameters" }, { status: 400 });
    }

    let cached: { formatted_address: string | null; lat: number | null; lng: number | null; address_components: unknown } | null = null;
    const supabase = getSupabaseClient();
    if (supabase) {
      try {
        const { data } = await supabase
        .from("cached_locations")
        .select("formatted_address, lat, lng, address_components")
        .eq("lookup_key", lookupKey)
        .maybeSingle();
        cached = data;
      } catch {
        // cached_locations missing or error - skip cache, go to Google
      }
    }

    if (cached) {
      return NextResponse.json(cachedToGeocodeResult(cached));
    }

    let url: string;
    if (lookupType === "address" && typeof address === "string") {
      url = `${GEOCODE_BASE}?address=${encodeURIComponent(address)}&key=${apiKey}`;
    } else if (lookupType === "place_id" && typeof placeId === "string") {
      url = `${GEOCODE_BASE}?place_id=${encodeURIComponent(placeId)}&key=${apiKey}`;
    } else if (lookupType === "reverse" && Number.isFinite(lat) && Number.isFinite(lng)) {
      url = `${GEOCODE_BASE}?latlng=${lat},${lng}&key=${apiKey}`;
    } else {
      return NextResponse.json({ error: "Invalid geocode parameters" }, { status: 400 });
    }

    const res = await fetch(url, {
      method: "GET",
      headers: { "Accept": "application/json", "User-Agent": "StreetIQ-Geocode/1.0" },
      signal: AbortSignal.timeout(8000),
      cache: "no-store",
    });
    const data = (await res.json()) as {
      status: string;
      error_message?: string;
      results?: Array<{
        formatted_address?: string;
        address_components?: Array<{ long_name: string; short_name: string; types: string[] }>;
        geometry?: { location?: { lat?: number; lng?: number } };
        place_id?: string;
      }>;
    };

    if (data.status !== "OK" || !data.results?.[0]) {
      return NextResponse.json(
        { results: null, status: data.status ?? "UNKNOWN_ERROR" },
        { status: 200 }
      );
    }

    const first = data.results[0];
    const loc = first.geometry?.location;
    const geoLat = loc?.lat ?? 0;
    const geoLng = loc?.lng ?? 0;

    if (supabase) {
      try {
        await supabase.from("cached_locations").upsert(
        {
          lookup_key: lookupKey,
          lookup_type: lookupType,
          formatted_address: first.formatted_address ?? null,
          lat: geoLat,
          lng: geoLng,
          place_id: first.place_id ?? null,
          address_components: first.address_components ?? null,
        },
        { onConflict: "lookup_key" }
        );
      } catch {
        // Cache save failed - still return results
      }
    }

    return NextResponse.json(cachedToGeocodeResult({
      formatted_address: first.formatted_address ?? null,
      lat: geoLat,
      lng: geoLng,
      address_components: first.address_components ?? null,
    }));
  } catch {
    return NextResponse.json({ error: "Geocoding failed" }, { status: 500 });
  }
}
