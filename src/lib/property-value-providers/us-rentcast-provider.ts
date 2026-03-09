/**
 * United States Provider (RentCast API)
 * Fetches property data by address or coordinates.
 * Maps to Property Value structure for UI.
 * Never fails the card when property record exists; shows best available data.
 */

import type { PropertyDataProvider } from "./provider-interface";
import type {
  PropertyValueInput,
  PropertyValueInsightsResult,
  PropertyValueInsightsSuccess,
  PropertyValueInsightsNoMatch,
  PropertyValueInsightsError,
  PropertyValueInsightsDebug,
  MarketValueSource,
  FallbackLevel,
} from "./types";

import { propertyProviderConfig } from "./config";

const RENTCAST_BASE_URL = propertyProviderConfig.rentcast.baseUrl;
const RENTCAST_API_KEY = propertyProviderConfig.rentcast.apiKey;
const RENTCAST_TIMEOUT_MS = parseInt(process.env.RENTCAST_API_TIMEOUT_MS ?? "15000", 10) || 15000;

const FIVE_YEARS_MS = 5 * 365.25 * 24 * 60 * 60 * 1000;
const NEARBY_RADIUS_MILES = 0.125; // ~200m

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

type RentCastResponse = RentCastProperty[] | { data?: RentCastProperty[] };

function isConfigured(): boolean {
  return Boolean(RENTCAST_API_KEY);
}

function buildUSProviderDebug(overrides: Partial<Record<string, unknown>>): PropertyValueInsightsDebug {
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
  const addr = state && zip ? `${streetPart}, ${city}, ${state} ${zip}` : `${streetPart}, ${city}`;
  if (addr) return `${RENTCAST_BASE_URL}/properties?address=${encodeURIComponent(addr)}`;
  const fullAddr = (input.fullAddress ?? "").trim();
  if (fullAddr) return `${RENTCAST_BASE_URL}/properties?address=${encodeURIComponent(fullAddr)}`;
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
  if (lat != null && lng != null && Number.isFinite(lat) && Number.isFinite(lng)) return { lat, lng };
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

function sqftToSqm(sqft: number): number {
  return Math.round(sqft * 0.0929 * 10) / 10;
}

function pricePerSqftToSqm(pricePerSqft: number): number {
  return Math.round((pricePerSqft / 0.0929) * 100) / 100;
}

/** Extract valid history entries for last 5 years */
function getHistory5y(prop: RentCastProperty): { date: string; price: number }[] {
  const history = prop.history ?? {};
  const fiveYearsAgo = Date.now() - FIVE_YEARS_MS;
  return Object.entries(history)
    .filter(([, v]) => v && typeof v === "object" && parseNumeric((v as { price?: number }).price) > 0)
    .map(([date, v]) => ({ date, price: parseNumeric((v as { price?: number }).price) }))
    .filter((e) => new Date(e.date).getTime() >= fiveYearsAgo)
    .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
}

/**
 * Build PropertyValueInsightsSuccess from property data.
 * Uses best available source for market value; never fails when property exists.
 */
function buildInsightsFromProperty(
  prop: RentCastProperty,
  input: PropertyValueInput,
  streetMedianPrice?: number,
  streetMedianPricePerSqft?: number
): PropertyValueInsightsSuccess {
  const lastSalePrice = parseNumeric(prop.lastSalePrice);
  const sqft = parseNumeric(prop.squareFootage);
  const propertySizeM2 = sqft > 0 ? sqftToSqm(sqft) : 0;
  const lastSaleDate = parseDate(prop.lastSaleDate);
  const history5y = getHistory5y(prop);
  const transactionsCount5y = history5y.length;
  const latestPrice = history5y[0]?.price ?? lastSalePrice;

  const city = (prop.city ?? input.city ?? "").trim() || "Unknown";
  const street = (prop.addressLine1 ?? input.street ?? "").trim() || "Unknown";
  const houseNumber = (input.houseNumber ?? "").trim() || "";

  let marketValue = 0;
  let marketValueSource: MarketValueSource = "none";
  let pricePerM2Used = 0;
  let fallbackLevel: FallbackLevel = "none";

  if (lastSalePrice > 0 && propertySizeM2 > 0) {
    const pricePerSqft = lastSalePrice / sqft;
    pricePerM2Used = pricePerSqftToSqm(pricePerSqft);
    marketValue = Math.round(pricePerM2Used * propertySizeM2);
    marketValueSource = "exact_transaction";
    fallbackLevel = "exact_building";
  } else if (propertySizeM2 > 0 && streetMedianPricePerSqft && streetMedianPricePerSqft > 0) {
    pricePerM2Used = pricePerSqftToSqm(streetMedianPricePerSqft);
    marketValue = Math.round(pricePerM2Used * propertySizeM2);
    marketValueSource = "price_per_m2_x_size";
    fallbackLevel = "street_fallback";
  } else if (streetMedianPrice && streetMedianPrice > 0) {
    marketValue = Math.round(streetMedianPrice);
    marketValueSource = "street_median";
    fallbackLevel = "street_fallback";
  }

  const pricePerM2 = lastSalePrice > 0 && propertySizeM2 > 0
    ? pricePerSqftToSqm(lastSalePrice / sqft)
    : pricePerM2Used;

  const latestTransactionPrice = latestPrice > 0 ? latestPrice : 0;
  const hasExactSale = lastSalePrice > 0;

  const buildingSummary =
    transactionsCount5y > 0 || hasExactSale
      ? {
          transactions_count_last_3_years: history5y.filter((e) => new Date(e.date).getTime() >= Date.now() - 3 * 365.25 * 24 * 60 * 60 * 1000).length,
          transactions_count_last_5_years: transactionsCount5y,
          latest_building_transaction_price: latestPrice > 0 ? latestPrice : lastSalePrice,
          average_apartment_value_today:
            history5y.length > 0
              ? Math.round(history5y.reduce((s, e) => s + e.price, 0) / history5y.length)
              : lastSalePrice,
        }
      : null;

  const currentEstimatedValue =
    marketValue > 0
      ? {
          estimated_value: marketValue,
          estimated_price_per_m2: pricePerM2Used > 0 ? pricePerM2Used : pricePerM2,
          estimation_method:
            marketValueSource === "exact_transaction"
              ? "Based on the latest official sale record. This is NOT an official appraisal."
              : marketValueSource === "street_median"
                ? "Estimated from provider data. Median of nearby sales within 200m."
                : "Estimated from provider data. Price per m² × property size.",
          value_type: (marketValueSource === "street_median" ? "street_median" : "sale") as "sale" | "street_median",
        }
      : null;

  const debug: PropertyValueInsightsDebug = {
    ...buildUSProviderDebug({ request_attempted: true }),
    property_found: true,
    market_value_source: marketValueSource,
    price_per_m2_used: pricePerM2Used || undefined,
    property_size_used: propertySizeM2 || undefined,
    transactions_count_5y: transactionsCount5y,
    latest_transaction_amount: latestTransactionPrice || undefined,
    fallback_level_used: fallbackLevel,
  };

  return {
    address: { city, street, house_number: houseNumber },
    match_quality: hasExactSale ? "exact_building" : "nearby_building",
    latest_transaction: {
      transaction_date: lastSaleDate,
      transaction_price: latestTransactionPrice,
      property_size: propertySizeM2,
      price_per_m2: pricePerM2,
    },
    current_estimated_value: currentEstimatedValue,
    building_summary_last_3_years: buildingSummary,
    market_value_source: marketValueSource,
    fallback_level: fallbackLevel,
    explanation: hasExactSale
      ? `Property record for ${city}, ${street} ${houseNumber}. Last sale: ${lastSaleDate || "Unknown"}.`
      : marketValue > 0
        ? `No sale record for this property. Estimated from nearby sales within 200m.`
        : `Property record for ${city}, ${street} ${houseNumber}. No sale data available.`,
    source: "rentcast",
    debug,
  };
}

function mapError(err: unknown): PropertyValueInsightsError {
  if (err instanceof Error) {
    if (err.name === "AbortError" || err.message.includes("timeout")) {
      return { message: "Property data request timed out. Please try again later.", error: "TIMEOUT" };
    }
    return { message: "Could not fetch property data. Please try again later.", error: err.message };
  }
  return { message: "Could not fetch property data. Please try again later.", error: "UNKNOWN_ERROR" };
}

async function fetchWithTimeout(url: string): Promise<Response> {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), RENTCAST_TIMEOUT_MS);
  try {
    return await fetch(url, {
      method: "GET",
      headers: { Accept: "application/json", "X-Api-Key": RENTCAST_API_KEY },
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
          reason: "Could not build request URL",
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
            }),
          };
        }
        if (res.status === 404) {
          return {
            message: "no transaction found",
            debug: buildUSProviderDebug({
              request_attempted: true,
              http_status: httpStatus,
              property_found: false,
              market_value_source: "none",
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
            property_found: false,
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
            property_found: false,
          }),
        };
      }

      let streetMedianPrice: number | undefined;
      let streetMedianPricePerSqft: number | undefined;
      const lastSalePrice = parseNumeric(prop.lastSalePrice);
      if (lastSalePrice <= 0) {
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
                streetMedianPrice = median(salePrices);
                const pricesPerSqft = nearbyRecords
                  .map((r) => {
                    const p = parseNumeric(r.lastSalePrice);
                    const s = parseNumeric(r.squareFootage);
                    return s > 0 && p > 0 ? p / s : 0;
                  })
                  .filter((x) => x > 0);
                if (pricesPerSqft.length > 0) {
                  streetMedianPricePerSqft = median(pricesPerSqft);
                }
              }
            } catch {
              /* continue without street median */
            }
          }
        }
      }

      const result = buildInsightsFromProperty(prop, input, streetMedianPrice, streetMedianPricePerSqft);
      return {
        ...result,
        debug: {
          ...result.debug,
          http_status: httpStatus,
          records_fetched: records.length,
          records_returned: 1,
        },
      };
    } catch (err) {
      const mappedErr = mapError(err);
      return {
        ...mappedErr,
        debug: buildUSProviderDebug({
          request_attempted: true,
          api_error: mappedErr.error,
          property_found: false,
        }),
      };
    }
  }
}
