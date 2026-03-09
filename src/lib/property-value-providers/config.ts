/**
 * Property Value Provider Configuration
 * All config from env variables. No hardcoded values.
 */

function env(key: string, defaultValue = ""): string {
  return (process.env[key] ?? defaultValue).trim();
}

function envNumber(key: string, defaultValue: number): number {
  const v = parseInt(process.env[key] ?? "", 10);
  return Number.isFinite(v) ? v : defaultValue;
}

export const propertyProviderConfig = {
  /** Israel: "official" | "mock" | unset = not configured */
  israel: env("PROPERTY_PROVIDER_ISRAEL"),

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
