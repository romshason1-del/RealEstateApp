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
  PropertyValueInsightsDebug,
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
  latitude?: number;
  longitude?: number;
  formattedAddress?: string;
  addressLine1?: string;
  city?: string;
  state?: string;
  zipCode?: string;
  history?: Record<string, { price?: number; date?: string }>;
  [key: string]: unknown;
};

/** 200m ≈ 0.124 miles */
const NEARBY_RADIUS_MILES = 0.125;

type RentCastResponse = RentCastProperty[] | { data?: RentCastProperty[] };

function isConfigured(): boolean {
  return Boolean(RENTCAST_API_KEY);
}

function buildUSProviderDebug(overrides: Partial<{
  request_attempted: boolean;
  http_status?: number;
  api_error?: string;
  reason?: string;
  records_fetched?: number;
  records_returned?: number;
  exact_matches_count?: number;
  response_summary?: string;
}>): PropertyValueInsightsDebug {
  return {
    active_provider_id: "us-rentcast",
    provider_configured: isConfigured(),
    PROPERTY_PROVIDER_US: propertyProviderConfig.us || "(not set)",
    RENTCAST_API_KEY_present: Boolean(RENTCAST_API_KEY),
    request_attempted: false,
    ...overrides,
  };
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

function buildCoordinatesUrl(input: PropertyValueInput, radiusMiles = 0.1): string | null {
  const lat = input.latitude;
  const lng = input.longitude;
  if (lat == null || lng == null || !Number.isFinite(lat) || !Number.isFinite(lng)) return null;
  return `${RENTCAST_BASE_URL}/properties?latitude=${lat}&longitude=${lng}&radius=${radiusMiles}&limit=50`;
}

function getCoordinates(prop: RentCastProperty, input: PropertyValueInput): { lat: number; lng: number } | null {
  const lat = prop?.latitude ?? input.latitude;
  const lng = prop?.longitude ?? input.longitude;
  if (lat != null && lng != null && Number.isFinite(lat) && Number.isFinite(lng)) {
    return { lat, lng };
  }
  return null;
}

function median(values: number[]): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
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
            value_type: "sale" as const,
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

/** Build success result when property has no sale price but we have street median from nearby properties */
function mapToInsightsWithStreetMedian(
  prop: RentCastProperty,
  input: PropertyValueInput,
  medianPrice: number
): PropertyValueInsightsSuccess {
  const sqft = parseNumeric(prop.squareFootage);
  const city = (prop.city ?? input.city ?? "").trim() || "Unknown";
  const street = (prop.addressLine1 ?? input.street ?? "").trim() || "Unknown";
  const houseNumber = (input.houseNumber ?? "").trim() || "";
  const propertySizeM2 = sqft > 0 ? sqftToSqm(sqft) : 0;
  const pricePerM2 = propertySizeM2 > 0 && medianPrice > 0 ? pricePerSqftToSqm(medianPrice / sqft) : 0;

  return {
    address: { city, street, house_number: houseNumber },
    match_quality: "nearby_building",
    latest_transaction: {
      transaction_date: "",
      transaction_price: 0,
      property_size: propertySizeM2,
      price_per_m2: pricePerM2,
    },
    current_estimated_value: {
      estimated_value: Math.round(medianPrice),
      estimated_price_per_m2: pricePerM2,
      estimation_method: "Median sale price of nearby properties within 200m. No sale record for this exact property.",
      value_type: "street_median",
    },
    building_summary_last_3_years: null,
    explanation: `No sale record for this property. Estimated from median of ${Math.round(medianPrice).toLocaleString()} based on nearby sales within 200m.`,
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
        "X-Api-Key": RENTCAST_API_KEY,
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
        debug: buildUSProviderDebug({
          request_attempted: false,
          reason: "US provider not configured in production environment",
        }),
      };
    }

    const url = buildAddressUrl(input) ?? buildCoordinatesUrl(input);
    if (!url) {
      return {
        message: "Address must include city and street, or coordinates.",
        error: "INVALID_INPUT",
        debug: buildUSProviderDebug({
          request_attempted: false,
          reason: "Could not build request URL: address must include city and street, or latitude/longitude",
        }),
      };
    }

    try {
      const res = await fetchWithTimeout(url);
      const httpStatus = res.status;

      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        const apiError = (body as { message?: string })?.message ?? `HTTP ${httpStatus}`;
        if (res.status === 401 || res.status === 403) {
          return {
            message: "Property data API authentication failed. Please contact support.",
            error: "AUTH_ERROR",
            debug: buildUSProviderDebug({
              request_attempted: true,
              http_status: httpStatus,
              api_error: apiError,
              reason: "API authentication failed (401/403)",
            }),
          };
        }
        if (res.status === 404) {
          return {
            message: "no transaction found",
            debug: buildUSProviderDebug({
              request_attempted: true,
              http_status: httpStatus,
              api_error: apiError,
              records_fetched: 0,
              records_returned: 0,
              exact_matches_count: 0,
              response_summary: "No property found at this address",
            }),
          };
        }
        return {
          message: "Could not fetch property data. Please try again later.",
          error: apiError,
          debug: buildUSProviderDebug({
            request_attempted: true,
            http_status: httpStatus,
            api_error: apiError,
            reason: `API returned ${httpStatus}`,
          }),
        };
      }

      const body = (await res.json().catch(() => null)) as RentCastResponse | null;
      if (!body) {
        return {
          message: "no transaction found",
          debug: buildUSProviderDebug({
            request_attempted: true,
            http_status: httpStatus,
            api_error: "Failed to parse response body",
            records_fetched: 0,
            records_returned: 0,
            exact_matches_count: 0,
            response_summary: "Empty or invalid JSON response",
          }),
        };
      }

      const records: RentCastProperty[] = Array.isArray(body)
        ? body
        : (body as { data?: RentCastProperty[] }).data ?? [];
      const prop = records[0];

      if (!prop) {
        return {
          message: "no transaction found",
          debug: buildUSProviderDebug({
            request_attempted: true,
            http_status: httpStatus,
            records_fetched: records.length,
            records_returned: records.length,
            exact_matches_count: 0,
            response_summary: "API returned empty property list",
          }),
        };
      }

      let mapped = mapToInsights(prop, input);
      if (!mapped) {
        const coords = getCoordinates(prop, input);
        if (coords) {
          const nearbyUrl = buildCoordinatesUrl(
            { ...input, latitude: coords.lat, longitude: coords.lng },
            NEARBY_RADIUS_MILES
          );
          if (nearbyUrl) {
            try {
              const nearbyRes = await fetchWithTimeout(nearbyUrl);
              if (nearbyRes.ok) {
                const nearbyBody = (await nearbyRes.json().catch(() => null)) as RentCastResponse | null;
                const nearbyRecords: RentCastProperty[] = nearbyBody
                  ? Array.isArray(nearbyBody)
                    ? nearbyBody
                    : (nearbyBody as { data?: RentCastProperty[] }).data ?? []
                  : [];
                const salePrices = nearbyRecords
                  .map((r) => parseNumeric(r.lastSalePrice))
                  .filter((p) => p > 0);
                const medianPrice = median(salePrices);
                if (medianPrice > 0) {
                  mapped = mapToInsightsWithStreetMedian(prop, input, medianPrice);
                }
              }
            } catch {
              /* fall through to no transaction found */
            }
          }
        }
        if (!mapped) {
          return {
            message: "no transaction found",
            debug: buildUSProviderDebug({
              request_attempted: true,
              http_status: httpStatus,
              records_fetched: records.length,
              records_returned: records.length,
              exact_matches_count: 0,
              response_summary: "Property record had no valid sale price",
            }),
          };
        }
      }

      const isStreetMedian = mapped.current_estimated_value?.value_type === "street_median";
      const debug: PropertyValueInsightsDebug = buildUSProviderDebug({
        request_attempted: true,
        http_status: httpStatus,
        records_fetched: records.length,
        records_returned: 1,
        exact_matches_count: isStreetMedian ? 0 : 1,
        response_summary: isStreetMedian
          ? `Estimated street value from nearby sales: ${mapped.address.city}, ${mapped.address.street}`
          : `Mapped 1 property: ${mapped.address.city}, ${mapped.address.street}`,
      });
      return { ...mapped, debug };
    } catch (err) {
      const mappedErr = mapError(err);
      return {
        ...mappedErr,
        debug: buildUSProviderDebug({
          request_attempted: true,
          api_error: mappedErr.error,
          reason: err instanceof Error ? err.message : "Request failed before HTTP response",
        }),
      };
    }
  }
}
