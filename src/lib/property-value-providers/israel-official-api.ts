/**
 * Israel Official API Integration Skeleton
 * Request builder, auth, timeout, retry, response/error mapping.
 * All config from env. No hardcoded endpoints.
 */

import { propertyProviderConfig } from "./config";
import type {
  PropertyValueInput,
  PropertyValueInsightsResult,
  PropertyValueInsightsSuccess,
  PropertyValueInsightsNoMatch,
  PropertyValueInsightsError,
} from "./types";

// ---------------------------------------------------------------------------
// Auth config layer
// ---------------------------------------------------------------------------

function getAuthHeaders(): Record<string, string> {
  const { apiKey, clientId, clientSecret } = propertyProviderConfig.israelTaxApi;

  if (apiKey) {
    return {
      Authorization: `Bearer ${apiKey}`,
      "X-API-Key": apiKey,
    };
  }

  if (clientId && clientSecret) {
    // OAuth2 client credentials - token must be fetched separately
    // Placeholder: when integrating, call token endpoint and add Bearer token
    return {
      "X-Client-Id": clientId,
      "X-Client-Secret": clientSecret,
    };
  }

  return {};
}

// ---------------------------------------------------------------------------
// Request builder
// ---------------------------------------------------------------------------

function buildRequestUrl(): string | null {
  const { baseUrl, endpointTransactions } = propertyProviderConfig.israelTaxApi;
  if (!baseUrl) return null;
  const path = endpointTransactions || "/transactions";
  const base = baseUrl.replace(/\/$/, "");
  const endpoint = path.startsWith("/") ? path : `/${path}`;
  return `${base}${endpoint}`;
}

function buildRequestInit(input: PropertyValueInput): RequestInit {
  const headers: Record<string, string> = {
    Accept: "application/json",
    "Content-Type": "application/json",
    "User-Agent": "StreetIQ/1.0 (Official Government Data)",
    ...getAuthHeaders(),
  };

  const body = JSON.stringify({
    city: input.city,
    street: input.street,
    houseNumber: input.houseNumber,
    latitude: input.latitude,
    longitude: input.longitude,
  });

  return {
    method: "POST",
    headers,
    body,
  };
}

// ---------------------------------------------------------------------------
// Response mapper
// ---------------------------------------------------------------------------

/** Raw API response shape - adapt when real API is known */
type RawApiTransaction = {
  date?: string;
  transactionDate?: string;
  price?: number;
  salePrice?: number;
  amount?: number;
  area?: number;
  propertySize?: number;
  sqm?: number;
  [key: string]: unknown;
};

type RawApiResponse = {
  transactions?: RawApiTransaction[];
  data?: RawApiTransaction[];
  results?: RawApiTransaction[];
  [key: string]: unknown;
};

function parseNumeric(val: unknown): number {
  if (typeof val === "number" && Number.isFinite(val)) return val;
  const s = String(val ?? "").replace(/[^\d.]/g, "");
  const n = parseFloat(s);
  return Number.isFinite(n) ? n : 0;
}

function parseDate(val: unknown): string {
  if (val == null || val === "") return "";
  const d = new Date(String(val));
  return Number.isFinite(d.getTime()) ? d.toISOString().slice(0, 10) : "";
}

function mapResponseToInsights(
  raw: RawApiResponse,
  input: PropertyValueInput
): PropertyValueInsightsSuccess | null {
  const list =
    raw.transactions ?? raw.data ?? raw.results ?? (Array.isArray(raw) ? raw : []);
  const transactions = list as RawApiTransaction[];

  if (!transactions.length) return null;

  const latest = transactions[0];
  const date =
    latest.transactionDate ?? latest.date ?? "";
  const price =
    parseNumeric(latest.salePrice ?? latest.price ?? latest.amount) ||
    parseNumeric(latest.price);
  const size =
    parseNumeric(latest.propertySize ?? latest.area ?? latest.sqm) || 100;

  const pricePerM2 = size > 0 ? Math.round((price / size) * 100) / 100 : 0;
  const threeYearsAgo = Date.now() - 3 * 365.25 * 24 * 60 * 60 * 1000;
  const last3 = transactions.filter((t) => {
    const d = t.transactionDate ?? t.date;
    return d && new Date(String(d)).getTime() >= threeYearsAgo;
  });

  const city = (input.city ?? "").trim() || "Unknown";
  const street = (input.street ?? "").trim() || "Unknown";
  const houseNumber = (input.houseNumber ?? "").trim() || "";

  return {
    address: { city, street, house_number: houseNumber },
    match_quality: "exact_building",
    latest_transaction: {
      transaction_date: parseDate(date),
      transaction_price: price,
      property_size: size,
      price_per_m2: pricePerM2,
    },
    current_estimated_value:
      price > 0 && size > 0
        ? {
            estimated_value: price,
            estimated_price_per_m2: pricePerM2,
            estimation_method:
              "Based only on the latest exact official transaction. This is NOT an official appraisal.",
          }
        : null,
    building_summary_last_3_years:
      last3.length > 0
        ? {
            transactions_count_last_3_years: last3.length,
            latest_building_transaction_price: price,
            average_apartment_value_today: price,
          }
        : null,
    explanation: `Exact match for ${city}, ${street} ${houseNumber}. ${transactions.length} official transaction(s) for this building.`,
    source: "israel-official",
  };
}

// ---------------------------------------------------------------------------
// Error mapper
// ---------------------------------------------------------------------------

function mapErrorToInsights(
  err: unknown,
  input: PropertyValueInput
): PropertyValueInsightsError {
  if (err instanceof Error) {
    if (err.name === "AbortError" || err.message.includes("timeout")) {
      return {
        message: "Official government API request timed out. Please try again later.",
        error: "TIMEOUT",
      };
    }
    if (err.message.includes("fetch") || err.message.includes("network")) {
      return {
        message: "Could not reach official government API. Please check your connection.",
        error: "NETWORK_ERROR",
      };
    }
    return {
      message: "Official government API returned an error. Please try again later.",
      error: err.message,
    };
  }
  return {
    message: "Official government API returned an error. Please try again later.",
    error: "UNKNOWN_ERROR",
  };
}

function mapHttpErrorToInsights(
  status: number,
  body: unknown,
  input: PropertyValueInput
): PropertyValueInsightsError | PropertyValueInsightsNoMatch {
  if (status === 404) {
    return {
      message: "no transaction found",
    };
  }
  if (status === 401 || status === 403) {
    return {
      message: "Official government API authentication failed. Please contact support.",
      error: "AUTH_ERROR",
    };
  }
  if (status >= 500) {
    return {
      message: "Official government API is temporarily unavailable. Please try again later.",
      error: `HTTP_${status}`,
    };
  }
  const msg =
    typeof body === "object" && body !== null && "message" in body
      ? String((body as { message: unknown }).message)
      : `HTTP ${status}`;
  return {
    message: "Official government API returned an error. Please try again later.",
    error: msg,
  };
}

// ---------------------------------------------------------------------------
// Timeout + retry
// ---------------------------------------------------------------------------

async function fetchWithTimeout(
  url: string,
  init: RequestInit,
  timeoutMs: number
): Promise<Response> {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      ...init,
      signal: controller.signal,
    });
    return res;
  } finally {
    clearTimeout(id);
  }
}

async function fetchWithRetry(
  url: string,
  init: RequestInit,
  timeoutMs: number,
  retries: number
): Promise<Response> {
  let lastError: unknown;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await fetchWithTimeout(url, init, timeoutMs);
      if (res.ok || (res.status >= 400 && res.status < 500)) return res;
      lastError = new Error(`HTTP ${res.status}`);
    } catch (e) {
      lastError = e;
      if (attempt < retries) {
        const delay = Math.min(1000 * 2 ** attempt, 5000);
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  }
  throw lastError;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export async function fetchIsraelOfficialTransactions(
  input: PropertyValueInput
): Promise<PropertyValueInsightsResult> {
  const url = buildRequestUrl();
  if (!url) {
    return {
      message: "Official government transaction source is not configured yet.",
      error: "PROVIDER_NOT_CONFIGURED",
    };
  }

  const { timeoutMs, retries } = propertyProviderConfig.israelTaxApi;
  const init = buildRequestInit(input);

  try {
    const res = await fetchWithRetry(url, init, timeoutMs, retries);
    const body = (await res.json().catch(() => ({}))) as RawApiResponse;

    if (!res.ok) {
      return mapHttpErrorToInsights(res.status, body, input);
    }

    const mapped = mapResponseToInsights(body, input);
    if (mapped) return mapped;

    return {
      message: "no transaction found",
    };
  } catch (err) {
    return mapErrorToInsights(err, input);
  }
}
