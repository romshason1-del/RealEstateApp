/**
 * United States Provider (RentCast API)
 * Fetches property data by address or coordinates.
 * Maps to Property Value structure for UI.
 */

import type { PropertyDataProvider } from "./provider-interface";
import type {
  PropertyValueInput,
  PropertyValueInsightsResult,
  PropertyValueInsightsSuccess,
  PropertyValueInsightsNoMatch,
  PropertyValueInsightsError,
} from "./types";

import { propertyProviderConfig } from "./config";

const RENTCAST_BASE_URL = propertyProviderConfig.rentcast.baseUrl;
const RENTCAST_API_KEY = propertyProviderConfig.rentcast.apiKey;
const RENTCAST_TIMEOUT_MS = parseInt(process.env.RENTCAST_API_TIMEOUT_MS ?? "15000", 10) || 15000;

/** RentCast property record response */
type RentCastProperty = {
  lastSaleDate?: string;
  lastSalePrice?: number;
  squareFootage?: number;
  formattedAddress?: string;
  addressLine1?: string;
  city?: string;
  state?: string;
  zipCode?: string;
  history?: Record<string, { price?: number; date?: string }>;
  [key: string]: unknown;
};

type RentCastResponse = RentCastProperty[] | { data?: RentCastProperty[] };

function isConfigured(): boolean {
  return Boolean(RENTCAST_API_KEY);
}

function buildAddressUrl(input: PropertyValueInput): string | null {
  const street = (input.street ?? "").trim();
  const city = (input.city ?? "").trim();
  const houseNumber = (input.houseNumber ?? "").trim();
  const state = (input.state ?? "").trim();
  const zip = (input.zip ?? "").trim();

  if (!street || !city) return null;

  const streetPart = [houseNumber, street].filter(Boolean).join(" ");
  const addr =
    state && zip ? `${streetPart}, ${city}, ${state} ${zip}` : streetPart ? `${streetPart}, ${city}` : null;
  if (addr) {
    return `${RENTCAST_BASE_URL}/properties?address=${encodeURIComponent(addr)}`;
  }

  const fullAddr = (input.fullAddress ?? "").trim();
  if (fullAddr) {
    return `${RENTCAST_BASE_URL}/properties?address=${encodeURIComponent(fullAddr)}`;
  }

  return null;
}

function buildCoordinatesUrl(input: PropertyValueInput): string | null {
  const lat = input.latitude;
  const lng = input.longitude;
  if (lat == null || lng == null || !Number.isFinite(lat) || !Number.isFinite(lng)) return null;
  const radius = 0.1; // miles
  return `${RENTCAST_BASE_URL}/properties?latitude=${lat}&longitude=${lng}&radius=${radius}`;
}

function parseDate(val: unknown): string {
  if (val == null || val === "") return "";
  const d = new Date(String(val));
  return Number.isFinite(d.getTime()) ? d.toISOString().slice(0, 10) : "";
}

function parseNumeric(val: unknown): number {
  if (typeof val === "number" && Number.isFinite(val)) return val;
  const s = String(val ?? "").replace(/[^\d.]/g, "");
  const n = parseFloat(s);
  return Number.isFinite(n) ? n : 0;
}

/** Convert sq ft to m² */
function sqftToSqm(sqft: number): number {
  return Math.round(sqft * 0.0929 * 10) / 10;
}

/** Price per sq ft to price per m² */
function pricePerSqftToSqm(pricePerSqft: number): number {
  return Math.round((pricePerSqft / 0.0929) * 100) / 100;
}

function mapToInsights(prop: RentCastProperty, input: PropertyValueInput): PropertyValueInsightsSuccess | null {
  const price = parseNumeric(prop.lastSalePrice);
  const sqft = parseNumeric(prop.squareFootage);
  if (price <= 0) return null;

  const city = (prop.city ?? input.city ?? "").trim() || "Unknown";
  const street = (prop.addressLine1 ?? input.street ?? "").trim() || "Unknown";
  const houseNumber = (input.houseNumber ?? "").trim() || "";

  const propertySizeM2 = sqft > 0 ? sqftToSqm(sqft) : 0;
  const pricePerSqft = sqft > 0 ? price / sqft : 0;
  const pricePerM2 = pricePerSqft > 0 ? pricePerSqftToSqm(pricePerSqft) : 0;

  const lastSaleDate = parseDate(prop.lastSaleDate);

  const threeYearsAgo = Date.now() - 3 * 365.25 * 24 * 60 * 60 * 1000;
  const history = prop.history ?? {};
  const historyEntries = Object.entries(history)
    .filter(([, v]) => v && typeof v === "object" && parseNumeric((v as { price?: number }).price) > 0)
    .map(([date, v]) => ({
      date,
      price: parseNumeric((v as { price?: number }).price),
    }))
    .filter((e) => new Date(e.date).getTime() >= threeYearsAgo)
    .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());

  const transactionsLast3Years = historyEntries.length;
  const latestBuildingPrice = historyEntries[0]?.price ?? price;
  const avgValue =
    historyEntries.length > 0
      ? Math.round(
          historyEntries.reduce((sum, e) => sum + e.price, 0) / historyEntries.length
        )
      : price;

  return {
    address: { city, street, house_number: houseNumber },
    match_quality: "exact_building",
    latest_transaction: {
      transaction_date: lastSaleDate,
      transaction_price: price,
      property_size: propertySizeM2,
      price_per_m2: pricePerM2,
    },
    current_estimated_value:
      price > 0
        ? {
            estimated_value: price,
            estimated_price_per_m2: pricePerM2,
            estimation_method:
              "Based on the latest official sale record. This is NOT an official appraisal.",
          }
        : null,
    building_summary_last_3_years:
      transactionsLast3Years > 0
        ? {
            transactions_count_last_3_years: transactionsLast3Years,
            latest_building_transaction_price: latestBuildingPrice,
            average_apartment_value_today: avgValue,
          }
        : price > 0
          ? {
              transactions_count_last_3_years: 1,
              latest_building_transaction_price: price,
              average_apartment_value_today: price,
            }
          : null,
    explanation: `Property record for ${city}, ${street} ${houseNumber}. Last sale: ${lastSaleDate || "Unknown"}.`,
    source: "rentcast",
  };
}

function mapError(err: unknown): PropertyValueInsightsError {
  if (err instanceof Error) {
    if (err.name === "AbortError" || err.message.includes("timeout")) {
      return {
        message: "Property data request timed out. Please try again later.",
        error: "TIMEOUT",
      };
    }
    return {
      message: "Could not fetch property data. Please try again later.",
      error: err.message,
    };
  }
  return {
    message: "Could not fetch property data. Please try again later.",
    error: "UNKNOWN_ERROR",
  };
}

async function fetchWithTimeout(url: string): Promise<Response> {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), RENTCAST_TIMEOUT_MS);
  try {
    return await fetch(url, {
      method: "GET",
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${RENTCAST_API_KEY}`,
      },
      signal: controller.signal,
    });
  } finally {
    clearTimeout(id);
  }
}

export class UnitedStatesRentcastProvider implements PropertyDataProvider {
  readonly id = "us-rentcast";
  readonly name = "United States (RentCast)";

  async getInsights(input: PropertyValueInput): Promise<PropertyValueInsightsResult> {
    if (!isConfigured()) {
      return {
        message: "Property data source is not configured for the United States.",
        error: "PROVIDER_NOT_CONFIGURED",
      };
    }

    const url = buildAddressUrl(input) ?? buildCoordinatesUrl(input);
    if (!url) {
      return {
        message: "Address must include city and street, or coordinates.",
        error: "INVALID_INPUT",
      };
    }

    try {
      const res = await fetchWithTimeout(url);

      if (!res.ok) {
        if (res.status === 401 || res.status === 403) {
          return {
            message: "Property data API authentication failed. Please contact support.",
            error: "AUTH_ERROR",
          };
        }
        if (res.status === 404) {
          return { message: "no transaction found" };
        }
        const body = await res.json().catch(() => ({}));
        return {
          message: "Could not fetch property data. Please try again later.",
          error: (body as { message?: string })?.message ?? `HTTP ${res.status}`,
        };
      }

      const body = (await res.json().catch(() => null)) as RentCastResponse | null;
      if (!body) {
        return { message: "no transaction found" };
      }

      const records: RentCastProperty[] = Array.isArray(body)
        ? body
        : (body as { data?: RentCastProperty[] }).data ?? [];
      const prop = records[0];

      if (!prop) {
        return { message: "no transaction found" };
      }

      const mapped = mapToInsights(prop, input);
      if (!mapped) {
        return { message: "no transaction found" };
      }

      return mapped;
    } catch (err) {
      return mapError(err);
    }
  }
}
