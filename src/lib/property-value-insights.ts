/**
 * StreetIQ Property Value Insights
 * Provider-based architecture for property transaction data.
 * All user-facing output is English-only.
 */

import {
  getPropertyDataProvider,
  isUSRentcastConfigured,
  propertyProviderConfig,
  type PropertyValueInput as ProviderInput,
  type PropertyValueInsightsResult as ProviderResult,
} from "./property-value-providers";

// Re-export types for backward compatibility
export type PropertyValueInput = ProviderInput;
export type PropertyValueInsightsResult = ProviderResult;

export type {
  LatestTransaction,
  CurrentEstimatedValue,
  BuildingSummary,
  MatchQuality,
  PropertyValueInsightsSuccess,
  PropertyValueInsightsNoMatch,
  PropertyValueInsightsError,
  PropertyValueInsightsDebug,
} from "./property-value-providers";

/**
 * Get property value insights for the given address.
 * Uses the provider for the country (default IL).
 * @param input Address and optional coordinates
 * @param countryCode ISO 3166-1 alpha-2 (default "IL")
 */
export async function getPropertyValueInsights(
  input: PropertyValueInput,
  countryCode = "IL"
): Promise<PropertyValueInsightsResult> {
  let city = (input.city ?? "").trim();
  let street = (input.street ?? "").trim();
  let houseNumber = (input.houseNumber ?? "").trim();

  if (input.resolvedAddress) {
    city = input.resolvedAddress.city?.trim() ?? city;
    street = input.resolvedAddress.street?.trim() ?? street;
    houseNumber = input.resolvedAddress.houseNumber?.trim() ?? houseNumber;
  }

  if (!city || !street) {
    return {
      message: "Address must include city and street. Coordinates should be resolved to a structured address first.",
      error: "INVALID_INPUT",
    };
  }

  const provider = getPropertyDataProvider(countryCode);
  if (!provider) {
    const code = (countryCode ?? "").toUpperCase();
    const debug =
      code === "US"
        ? {
            active_provider_id: "none",
            provider_configured: isUSRentcastConfigured(),
            PROPERTY_PROVIDER_US: propertyProviderConfig.us || "(not set)",
            RENTCAST_API_KEY_present: Boolean(propertyProviderConfig.rentcast.apiKey),
            request_attempted: false,
            reason: "US provider not configured in production environment",
          }
        : undefined;
    return {
      message: "Property value data is not available for this country.",
      error: "NO_PROVIDER",
      debug,
    };
  }

  return provider.getInsights({
    ...input,
    city,
    street,
    houseNumber,
  });
}

export default getPropertyValueInsights;
