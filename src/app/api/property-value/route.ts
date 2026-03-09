/**
 * StreetIQ Property Value API
 * GET /api/property-value?city=...&street=...&houseNumber=...
 * Uses only official Israeli government real estate data (data.gov.il).
 */

import { NextRequest, NextResponse } from "next/server";
import getPropertyValueInsights from "@/lib/property-value-insights";

const CACHE = new Map<string, { data: unknown; ts: number }>();
const CACHE_TTL_MS = 5 * 60 * 1000;
const MAX_ADDRESS_LENGTH = 200;

function buildCacheKey(city: string, street: string, houseNumber: string): string {
  return [city.trim().toLowerCase(), street.trim().toLowerCase(), houseNumber.trim()].join("|");
}

function validateInput(city: string, street: string): { valid: boolean; error?: string } {
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
  const city = searchParams.get("city") ?? "";
  const street = searchParams.get("street") ?? "";
  const houseNumber = searchParams.get("houseNumber") ?? searchParams.get("house_number") ?? "";

  const validation = validateInput(city, street);
  if (!validation.valid) {
    return NextResponse.json(
      { message: validation.error, error: "INVALID_INPUT" },
      { status: 400 }
    );
  }

  const cacheKey = buildCacheKey(city, street, houseNumber);
  const cached = CACHE.get(cacheKey);
  if (cached && Date.now() - cached.ts < CACHE_TTL_MS) {
    return NextResponse.json(cached.data);
  }

  try {
    const result = await getPropertyValueInsights({
      city: city.trim(),
      street: street.trim(),
      houseNumber: houseNumber.trim(),
    });

    if ("message" in result && "error" in result && result.error) {
      const status =
        result.error === "INVALID_INPUT"
          ? 400
          : result.error === "DATA_SOURCE_UNAVAILABLE"
            ? 503
            : 502;
      return NextResponse.json(result, { status });
    }

    if ("message" in result && result.message === "no transaction found") {
      return NextResponse.json(result, { status: 404 });
    }

    if ("message" in result && result.message === "no reliable exact match found") {
      return NextResponse.json(result, { status: 404 });
    }

    if ("address" in result && result.address) {
      CACHE.set(cacheKey, { data: result, ts: Date.now() });
    }

    return NextResponse.json(result);
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
