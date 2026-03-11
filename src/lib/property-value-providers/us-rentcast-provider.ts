/**
 * United States Provider (RentCast API)
 * Uses /avm/value for market value, /avm/rent for rent, /properties for details.
 * NEVER uses last sale price as market value.
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
import { buildUSAddressVariants } from "../address-parse";

const RENTCAST_BASE_URL = propertyProviderConfig.rentcast.baseUrl;
const RENTCAST_API_KEY = propertyProviderConfig.rentcast.apiKey;
const RENTCAST_TIMEOUT_MS = propertyProviderConfig.rentcast.timeoutMs;
const RENTCAST_RETRIES = propertyProviderConfig.rentcast.retries;

const FIVE_YEARS_MS = 5 * 365.25 * 24 * 60 * 60 * 1000;

type RentCastProperty = {
  lastSaleDate?: string;
  lastSalePrice?: number;
  squareFootage?: number;
  bedrooms?: number;
  bathrooms?: number;
  yearBuilt?: number;
  propertyType?: string;
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

type AVMValueResponse = {
  price?: number;
  subjectProperty?: RentCastProperty;
  comparables?: Array<{ price?: number; squareFootage?: number }>;
};

type AVMRentResponse = {
  rent?: number;
  subjectProperty?: RentCastProperty;
};

type RentCastPropertiesResponse = RentCastProperty[] | { data?: RentCastProperty[] };

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

function buildAddressVariants(input: PropertyValueInput): string[] {
  const street = (input.street ?? "").trim();
  const city = (input.city ?? "").trim();
  if (!street || !city) return [];
  return buildUSAddressVariants({
    houseNumber: (input.houseNumber ?? "").trim(),
    street,
    city,
    state: (input.state ?? "").trim(),
    zip: (input.zip ?? "").trim(),
    fullAddress: (input.fullAddress ?? "").trim(),
  });
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

function mapError(err: unknown): PropertyValueInsightsError {
  if (err instanceof Error) {
    if (err.name === "AbortError" || err.message.includes("timeout")) {
      return { message: "Property data request timed out. Please try again later.", error: "TIMEOUT" };
    }
    return { message: "Could not fetch property data. Please try again later.", error: err.message };
  }
  return { message: "Could not fetch property data. Please try again later.", error: "UNKNOWN_ERROR" };
}

async function fetchRentCastWithRetry(
  url: string,
  attempt = 0
): Promise<Response> {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), RENTCAST_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      method: "GET",
      headers: { Accept: "application/json", "X-Api-Key": RENTCAST_API_KEY },
      signal: controller.signal,
    });
    clearTimeout(id);
    const retryable = res.status === 429 || res.status >= 500;
    if (retryable && attempt < RENTCAST_RETRIES) {
      const delay = Math.min(1000 * Math.pow(2, attempt), 5000);
      await new Promise((r) => setTimeout(r, delay));
      return fetchRentCastWithRetry(url, attempt + 1);
    }
    return res;
  } catch (err) {
    clearTimeout(id);
    const isRetryable = err instanceof Error && (err.name === "AbortError" || err.message.includes("timeout"));
    if (isRetryable && attempt < RENTCAST_RETRIES) {
      const delay = Math.min(1000 * Math.pow(2, attempt), 5000);
      await new Promise((r) => setTimeout(r, delay));
      return fetchRentCastWithRetry(url, attempt + 1);
    }
    throw err;
  }
}

/** Detect if address likely needs unit (condo/multi-unit) - no # or unit in address */
function mightNeedUnit(address: string, input: PropertyValueInput): boolean {
  const hasUnit = /\b#\d+|\bunit\s+\d+|\bapt\.?\s*\d+|\bste\.?\s*\d+/i.test(address) ||
    (input.apartmentNumber ?? "").trim() !== "";
  return !hasUnit;
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

    const addressVariants = buildAddressVariants(input);
    const lat = input.latitude != null && Number.isFinite(input.latitude) ? input.latitude : undefined;
    const lng = input.longitude != null && Number.isFinite(input.longitude) ? input.longitude : undefined;
    if (addressVariants.length === 0 && !(lat != null && lng != null)) {
      return {
        message: "Address must include city and street.",
        error: "INVALID_INPUT",
        debug: buildUSProviderDebug({
          request_attempted: false,
          reason: "Could not build address",
        }),
      };
    }

    const queryVariants: { type: string; param: string; label: string }[] = [];
    for (const addr of addressVariants) {
      queryVariants.push({ type: "address", param: `address=${encodeURIComponent(addr)}`, label: addr });
    }
    if (lat != null && lng != null) {
      queryVariants.push({ type: "coordinates", param: `latitude=${lat}&longitude=${lng}`, label: `${lat},${lng}` });
    }

    const debug: PropertyValueInsightsDebug = buildUSProviderDebug({
      request_attempted: true,
      property_found: false,
      avm_value_found: false,
      avm_rent_found: false,
      sales_history_found: false,
      comps_found: false,
      market_value_source: "none",
      unit_required: false,
      fallback_level_used: "none",
    });

    let avmValue: number | undefined;
    let avmRent: number | undefined;
    let prop: RentCastProperty | undefined;
    let subjectFromAvm: RentCastProperty | undefined;
    let comps: Array<{ price?: number; squareFootage?: number }> = [];
    let matchedQuery: string | undefined;

    let lastError: unknown;
    try {
      for (const q of queryVariants) {
        try {
          const [avmValueRes, avmRentRes, propsRes] = await Promise.all([
            fetchRentCastWithRetry(`${RENTCAST_BASE_URL}/avm/value?${q.param}`),
            fetchRentCastWithRetry(`${RENTCAST_BASE_URL}/avm/rent/long-term?${q.param}`),
            fetchRentCastWithRetry(`${RENTCAST_BASE_URL}/properties?${q.param}`),
          ]);

        if (avmValueRes.ok) {
          const body = (await avmValueRes.json().catch(() => null)) as AVMValueResponse | null;
          const price = body?.price != null ? parseNumeric(body.price) : 0;
          if (price > 0) {
            avmValue = price;
            debug.avm_value_found = true;
            debug.market_value_source = "avm";
            subjectFromAvm = body?.subjectProperty as RentCastProperty | undefined;
            if (body?.comparables && body.comparables.length > 0) {
              comps = body.comparables;
              debug.comps_found = true;
            }
          }
        }

        if (avmRentRes.ok) {
          const body = (await avmRentRes.json().catch(() => null)) as AVMRentResponse | null;
          const rent = body?.rent != null ? parseNumeric(body.rent) : 0;
          if (rent > 0) {
            avmRent = rent;
            debug.avm_rent_found = true;
            if (!subjectFromAvm) subjectFromAvm = body?.subjectProperty as RentCastProperty | undefined;
          }
        }

        if (propsRes.ok) {
          const body = (await propsRes.json().catch(() => null)) as RentCastPropertiesResponse | null;
          const records = body
            ? Array.isArray(body)
              ? body
              : (body as { data?: RentCastProperty[] }).data ?? []
            : [];
          prop = records[0];
          if (prop) {
            debug.property_found = true;
            const history = prop.history ?? {};
            const entries = Object.entries(history).filter(
              ([, v]) => v && typeof v === "object" && parseNumeric((v as { price?: number }).price) > 0
            );
            if (entries.length > 0) debug.sales_history_found = true;
          }
        }

        const hasData = !!(prop || subjectFromAvm || (avmValue != null && avmValue > 0) || (avmRent != null && avmRent > 0));
        if (hasData) {
          matchedQuery = `${q.type}:${q.label}`;
          break;
        }
        } catch (variantErr) {
          lastError = variantErr;
          continue;
        }
      }

      const subject = prop ?? subjectFromAvm;
      const city = (subject?.city ?? input.city ?? "").trim() || "Unknown";
      const street = (subject?.addressLine1 ?? input.street ?? "").trim() || "Unknown";
      const houseNumber = (input.houseNumber ?? "").trim() || "";

      const hasAnyData = subject || (avmValue != null && avmValue > 0) || (avmRent != null && avmRent > 0);
      if (!hasAnyData) {
        const unitRequired = mightNeedUnit(addressVariants[0] ?? "", input);
        if (unitRequired) {
          return {
            message: "This building requires a unit number to retrieve property data.",
            error: "UNIT_REQUIRED",
            debug: {
              ...debug,
              unit_required: true,
              property_found: false,
            },
          };
        }
        return {
          message: "No Data Available",
          debug: { ...debug, property_found: false },
        };
      }

      const sqft = parseNumeric(subject?.squareFootage ?? 0);
      const propertySizeM2 = sqft > 0 ? Math.round(sqft * 0.0929 * 10) / 10 : 0;
      const lastSalePrice = parseNumeric(subject?.lastSalePrice ?? 0);
      const lastSaleDate = parseDate(subject?.lastSaleDate ?? "");

      const history = subject?.history ?? {};
      const history5y = Object.entries(history)
        .filter(([, v]) => v && typeof v === "object" && parseNumeric((v as { price?: number }).price) > 0)
        .map(([date, v]) => ({ date, price: parseNumeric((v as { price?: number }).price) }))
        .filter((e) => new Date(e.date).getTime() >= Date.now() - FIVE_YEARS_MS)
        .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());

      const lastSale: { price: number; date: string } | undefined =
        history5y.length > 0
          ? { price: history5y[0].price, date: history5y[0].date }
          : lastSalePrice > 0
            ? { price: lastSalePrice, date: lastSaleDate }
            : undefined;

      const pricePerSqft = avmValue && sqft > 0 ? avmValue / sqft : 0;
      if (pricePerSqft > 0) debug.price_per_sqft_used = Math.round(pricePerSqft * 100) / 100;

      const displayValue = avmValue ?? ((lastSale?.price ?? 0) || (history5y.length > 0 ? history5y[0]!.price : 0));
      const currentEstimatedValue =
        displayValue > 0
          ? {
              estimated_value: displayValue,
              estimated_price_per_m2: sqft > 0 ? Math.round((displayValue / sqft / 0.0929) * 100) / 100 : 0,
              estimation_method: avmValue != null && avmValue > 0
                ? "Estimated Market Value from RentCast AVM. This is NOT an official appraisal."
                : lastSale
                  ? "Last recorded sale price for this property. Not a current market valuation."
                  : "Historical sale data for this property.",
              value_type: "sale" as const,
            }
          : null;

      const buildingSummary =
        history5y.length > 0 || (lastSale && lastSale.price > 0)
          ? {
              transactions_count_last_3_years: history5y.filter(
                (e) => new Date(e.date).getTime() >= Date.now() - 3 * 365.25 * 24 * 60 * 60 * 1000
              ).length,
              transactions_count_last_5_years: history5y.length,
              latest_building_transaction_price: lastSale?.price ?? 0,
              average_apartment_value_today:
                history5y.length > 0
                  ? Math.round(history5y.reduce((s, e) => s + e.price, 0) / history5y.length)
                  : lastSale?.price ?? 0,
            }
          : null;

      const result: PropertyValueInsightsSuccess = {
        address: { city, street, house_number: houseNumber },
        match_quality: lastSale && lastSale.price > 0 ? "exact_building" : "nearby_building",
        latest_transaction: {
          transaction_date: lastSale?.date ?? "",
          transaction_price: lastSale?.price ?? 0,
          property_size: propertySizeM2,
          price_per_m2: sqft > 0 && lastSale && lastSale.price > 0
            ? Math.round((lastSale.price / sqft / 0.0929) * 100) / 100
            : 0,
        },
        current_estimated_value: currentEstimatedValue,
        building_summary_last_3_years: buildingSummary,
        market_value_source: avmValue != null && avmValue > 0 ? "avm" : "none",
        fallback_level: avmValue != null && avmValue > 0 ? "exact_property" : "none",
        avm_value: avmValue,
        avm_rent: avmRent,
        last_sale: lastSale,
        sales_history: history5y.length > 0 ? history5y : undefined,
        nearby_comps:
          comps.length > 0
            ? (() => {
                const prices = comps.map((c) => parseNumeric(c.price)).filter((p) => p > 0);
                const withSqft = comps.filter((c) => parseNumeric(c.squareFootage) > 0 && parseNumeric(c.price) > 0);
                const pricesPerSqft = withSqft.map((c) => parseNumeric(c.price) / parseNumeric(c.squareFootage));
                return {
                  avg_price: prices.length > 0 ? Math.round(prices.reduce((a, b) => a + b, 0) / prices.length) : 0,
                  avg_price_per_sqft:
                    pricesPerSqft.length > 0
                      ? Math.round((pricesPerSqft.reduce((a, b) => a + b, 0) / pricesPerSqft.length) * 100) / 100
                      : 0,
                  count: comps.length,
                };
              })()
            : undefined,
        property_details:
          subject && (subject.bedrooms != null || subject.bathrooms != null || sqft > 0 || subject.yearBuilt != null || subject.propertyType)
            ? {
                beds: subject.bedrooms != null ? parseNumeric(subject.bedrooms) : undefined,
                baths: subject.bathrooms != null ? parseNumeric(subject.bathrooms) : undefined,
                sqft: sqft > 0 ? sqft : undefined,
                year_built: subject.yearBuilt != null ? parseNumeric(subject.yearBuilt) : undefined,
                property_type: typeof subject.propertyType === "string" ? subject.propertyType : undefined,
              }
            : undefined,
        unit_required: false,
        explanation: avmValue != null && avmValue > 0
          ? "Estimated Market Value from RentCast AVM."
          : lastSale
            ? "Last sale data available. No AVM estimate for this property."
            : "No Data Available",
        source: "rentcast",
        debug: {
          ...debug,
          match_query: matchedQuery,
          property_found: Boolean(subject),
          avm_value_found: Boolean(avmValue && avmValue > 0),
          avm_rent_found: Boolean(avmRent && avmRent > 0),
          sales_history_found: history5y.length > 0,
          comps_found: comps.length > 0,
          market_value_source: avmValue != null && avmValue > 0 ? "avm" : "none",
          price_per_sqft_used: pricePerSqft > 0 ? Math.round(pricePerSqft * 100) / 100 : undefined,
          unit_required: false,
          fallback_level_used: avmValue != null && avmValue > 0 ? "exact_property" : "none",
          property_size_used: propertySizeM2 || undefined,
        },
      };

      return result;
    } catch (err) {
      const mappedErr = mapError(lastError ?? err);
      return {
        ...mappedErr,
        debug: {
          ...debug,
          property_found: false,
          api_error: mappedErr.error,
          rentcast_timeout_ms: RENTCAST_TIMEOUT_MS,
          rentcast_retries: RENTCAST_RETRIES,
        },
      };
    }
  }
}
