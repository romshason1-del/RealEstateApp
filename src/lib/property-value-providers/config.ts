/**
 * Property Value Provider Configuration
 * All config from env variables. No hardcoded values.
 */

function env(key: string, defaultValue = ""): string {
  return (process.env[key] ?? defaultValue).trim();
}

function envBool(key: string): boolean {
  const v = (process.env[key] ?? "").trim().toLowerCase();
  return v === "true" || v === "1" || v === "yes";
}

function envNumber(key: string, defaultValue: number): number {
  const v = parseInt(process.env[key] ?? "", 10);
  return Number.isFinite(v) ? v : defaultValue;
}

export const propertyProviderConfig = {
  /** Israel: "official" | "mock" | unset = not configured */
  israel: env("PROPERTY_PROVIDER_ISRAEL"),

  /** United States: "rentcast" | "mock" | unset = not configured */
  us: env("PROPERTY_PROVIDER_US"),

  /** RentCast API (United States) */
  rentcast: {
    apiKey: env("RENTCAST_API_KEY"),
    baseUrl: env("RENTCAST_API_BASE_URL") || "https://api.rentcast.io/v1",
  },

  /** US Census Bureau (neighborhood stats) */
  census: {
    apiKey: env("CENSUS_API_KEY"),
  },

  /** Israel Tax Authority API */
  israelTaxApi: {
    baseUrl: env("ISRAEL_TAX_API_BASE_URL"),
    apiKey: env("ISRAEL_TAX_API_KEY"),
    clientId: env("ISRAEL_TAX_API_CLIENT_ID"),
    clientSecret: env("ISRAEL_TAX_API_CLIENT_SECRET"),
    /** Endpoint path for transactions (e.g. /v1/transactions) - no hardcoding */
    endpointTransactions: env("ISRAEL_TAX_API_ENDPOINT_TRANSACTIONS"),
    timeoutMs: envNumber("ISRAEL_TAX_API_TIMEOUT_MS", 15000),
    retries: envNumber("ISRAEL_TAX_API_RETRIES", 2),
  },
} as const;

export function isIsraelOfficialConfigured(): boolean {
  const { israel, israelTaxApi } = propertyProviderConfig;
  if (israel !== "official") return false;
  if (!israelTaxApi.baseUrl) return false;
  const hasAuth = israelTaxApi.apiKey || (israelTaxApi.clientId && israelTaxApi.clientSecret);
  return Boolean(hasAuth);
}

export function isIsraelMockEnabled(): boolean {
  return propertyProviderConfig.israel === "mock";
}

export function isUSRentcastConfigured(): boolean {
  return propertyProviderConfig.us === "rentcast" && Boolean(propertyProviderConfig.rentcast.apiKey);
}

/** US mock mode: PROPERTY_PROVIDER_US=mock or US_PROPERTY_DEBUG_MODE=true */
export function isUSMockEnabled(): boolean {
  return propertyProviderConfig.us === "mock" || envBool("US_PROPERTY_DEBUG_MODE");
}
