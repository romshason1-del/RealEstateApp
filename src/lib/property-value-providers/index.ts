/**
 * Property Value Provider Router
 * Returns the appropriate provider for the given country code.
 */

import type { PropertyDataProvider } from "./provider-interface";
import { IsraelOfficialProvider } from "./israel-official-provider";
import { MockProvider } from "./mock-provider";
import { UnitedStatesRentcastProvider } from "./us-rentcast-provider";
import { UnitedStatesMockProvider } from "./us-mock-provider";
import { USOrchestratorProvider } from "./us-orchestrator";
import { UKLandRegistryProvider } from "./uk-land-registry-provider";
import { isIsraelMockEnabled, isUSMockEnabled, isUSRentcastConfigured, propertyProviderConfig } from "./config";

const israelOfficial = new IsraelOfficialProvider();
const ukLandRegistry = new UKLandRegistryProvider();
const mockProvider = new MockProvider();
const usRentcast = new UnitedStatesRentcastProvider();
const usMock = new UnitedStatesMockProvider();
const usOrchestrator = new USOrchestratorProvider();

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

  if (code === "US") {
    return usOrchestrator;
  }

  if (code === "UK" || code === "GB") {
    return ukLandRegistry;
  }

  return null;
}

export { IsraelOfficialProvider, MockProvider, UnitedStatesRentcastProvider, UnitedStatesMockProvider, UKLandRegistryProvider, isUSMockEnabled, isUSRentcastConfigured, propertyProviderConfig };
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
