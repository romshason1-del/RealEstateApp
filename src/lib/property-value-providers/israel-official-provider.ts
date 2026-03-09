/**
 * Israel Official Provider
 * Integration skeleton for Tax Authority / government API.
 * Returns "not configured yet" until valid API config is provided.
 */

import type { PropertyDataProvider } from "./provider-interface";
import type { PropertyValueInput, PropertyValueInsightsResult } from "./types";
import { isIsraelOfficialConfigured } from "./config";
import { fetchIsraelOfficialTransactions } from "./israel-official-api";

export class IsraelOfficialProvider implements PropertyDataProvider {
  readonly id = "israel-official";
  readonly name = "Israel Official (Tax Authority)";

  async getInsights(input: PropertyValueInput): Promise<PropertyValueInsightsResult> {
    if (!isIsraelOfficialConfigured()) {
      return {
        message: "Official government transaction source is not configured yet.",
        error: "PROVIDER_NOT_CONFIGURED",
      };
    }

    return fetchIsraelOfficialTransactions(input);
  }
}
