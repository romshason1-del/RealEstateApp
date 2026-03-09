/**
 * Property Value Provider Router
 * Returns the appropriate provider for the given country code.
 */

import type { PropertyDataProvider } from "./provider-interface";
import { IsraelOfficialProvider } from "./israel-official-provider";
import { MockProvider } from "./mock-provider";
import { UnitedStatesRentcastProvider } from "./us-rentcast-provider";
import { isIsraelMockEnabled, isUSRentcastConfigured, propertyProviderConfig } from "./config";

const israelOfficial = new IsraelOfficialProvider();
const mockProvider = new MockProvider();
const usRentcast = new UnitedStatesRentcastProvider();

/**
 * Get the property data provider for the given country code.
 * @param countryCode ISO 3166-1 alpha-2 (e.g. "IL", "US")
 * @returns The provider instance, or null if no provider for this country
 */
export function getPropertyDataProvider(countryCode: string): PropertyDataProvider | null {
  const code = (countryCode ?? "").toUpperCase();

  if (code === "IL") {
    if (isIsraelMockEnabled()) {
      return mockProvider;
    }
    return israelOfficial;
  }

  if (code === "US" && isUSRentcastConfigured()) {
    return usRentcast;
  }

  return null;
}

export { IsraelOfficialProvider, MockProvider, UnitedStatesRentcastProvider, isUSRentcastConfigured, propertyProviderConfig };
export type { PropertyDataProvider } from "./provider-interface";
export type {
  PropertyValueInput,
  PropertyValueInsightsResult,
  PropertyValueInsightsSuccess,
  PropertyValueInsightsNoMatch,
  PropertyValueInsightsError,
  PropertyValueInsightsDebug,
  LatestTransaction,
  CurrentEstimatedValue,
  BuildingSummary,
  MatchQuality,
} from "./types";
