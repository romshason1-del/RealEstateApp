/**
 * Geocoding API with Supabase cache.
 * Check cached_locations before calling Google; save on miss.
 * Reduces Google Geocoding API costs.
 */

import { NextRequest, NextResponse } from "next/server";
import { createAdminClient } from "@/lib/supabase/admin";

const GEOCODE_BASE = "https://maps.googleapis.com/maps/api/geocode/json";

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
}): { results: Array<{ formatted_address?: string; address_components?: unknown; geometry: { location: { lat: () => number; lng: () => number } } }>; status: string } {
  const lat = cached.lat ?? 0;
  const lng = cached.lng ?? 0;
  return {
    results: [
      {
        formatted_address: cached.formatted_address ?? undefined,
        address_components: cached.address_components as unknown[] | undefined,
        geometry: {
          location: {
            lat: () => lat,
            lng: () => lng,
          },
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
    const apiKeyPresent = apiKey.length > 0;
    const apiKeyPreview = apiKeyPresent
      ? `${apiKey.slice(0, 4)}...${apiKey.slice(-4)}`
      : "MISSING";
    console.log("[geocode] API key:", apiKeyPreview, "| present:", apiKeyPresent);
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

    console.log("[geocode] Request:", { lookupType, lookupKey, address: address ?? "(none)", placeId: placeId ?? "(none)", lat, lng });

    let cached: { formatted_address: string | null; lat: number | null; lng: number | null; address_components: unknown } | null = null;
    try {
      const supabase = createAdminClient();
      const { data } = await supabase
        .from("cached_locations")
        .select("formatted_address, lat, lng, address_components")
        .eq("lookup_key", lookupKey)
        .maybeSingle();
      cached = data;
    } catch {
      // Supabase not configured or cached_locations missing - skip cache, go to Google
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
      console.error("[geocode] Invalid URL params:", { lookupType, address, placeId, lat, lng });
      return NextResponse.json({ error: "Invalid geocode parameters" }, { status: 400 });
    }

    const urlSafe = url.replace(/key=[^&]+/, "key=***");
    console.log("[geocode] Google URL:", urlSafe);

    const res = await fetch(url, { signal: AbortSignal.timeout(8000) });
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

    console.log("[geocode] Google API response:", {
      status: data.status,
      error_message: data.error_message ?? "(none)",
      results_count: data.results?.length ?? 0,
      full: JSON.stringify(data),
    });

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

    try {
      const supabase = createAdminClient();
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

    return NextResponse.json(cachedToGeocodeResult({
      formatted_address: first.formatted_address ?? null,
      lat: geoLat,
      lng: geoLng,
      address_components: first.address_components ?? null,
    }));
  } catch (err) {
    console.error("[geocode] Error:", err);
    return NextResponse.json(
      { error: err instanceof Error ? err.message : "Geocoding failed" },
      { status: 500 }
    );
  }
}
