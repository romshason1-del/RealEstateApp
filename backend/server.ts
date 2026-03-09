/**
 * StreetIQ Property Value Express API Server
 * Run from project root: npm run backend
 *
 * GET /api/property-value?city=...&street=...&houseNumber=...
 *
 * Uses only official Israeli government real estate data (data.gov.il).
 * No scraping. No commercial sources. No guessing.
 */

import express from "express";
const { default: getPropertyValueInsights } = await import("../src/lib/property-value-insights");

const app = express();
const PORT = process.env.PORT || 3001;

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

app.get("/api/property-value", async (req, res) => {
  const city = String(req.query.city ?? "");
  const street = String(req.query.street ?? "");
  const houseNumber = String(req.query.houseNumber ?? req.query.house_number ?? "");

  const validation = validateInput(city, street);
  if (!validation.valid) {
    return res.status(400).json({ message: validation.error, error: "INVALID_INPUT" });
  }

  const cacheKey = buildCacheKey(city, street, houseNumber);
  const cached = CACHE.get(cacheKey);
  if (cached && Date.now() - cached.ts < CACHE_TTL_MS) {
    return res.json(cached.data);
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
      return res.status(status).json(result);
    }

    if ("message" in result && result.message === "no transaction found") {
      return res.status(404).json(result);
    }

    if ("message" in result && result.message === "no reliable exact match found") {
      return res.status(404).json(result);
    }

    if ("address" in result && result.address) {
      CACHE.set(cacheKey, { data: result, ts: Date.now() });
    }

    return res.json(result);
  } catch (err) {
    console.error("[property-value] Error:", err);
    return res.status(500).json({
      message: "Failed to fetch property value insights. Please try again later.",
      error: err instanceof Error ? err.message : "Unknown error",
    });
  }
});

app.get("/health", (_req, res) => {
  res.json({ status: "ok", service: "StreetIQ Property Value API" });
});

app.listen(PORT, () => {
  console.log(`StreetIQ Property Value API listening on http://localhost:${PORT}`);
});
