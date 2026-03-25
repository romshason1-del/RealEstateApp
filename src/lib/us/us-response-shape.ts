/**
 * Build API-ready US response objects — scaffold / empty shells only (no fabricated values).
 */

import type { USPropertyValueResponse } from "./us-property-response-contract";

export function createEmptyUSPropertyValueResponse(partial?: {
  success?: boolean;
  message?: string | null;
}): USPropertyValueResponse {
  return {
    country_code: "US",
    estimated_value: null,
    last_transaction: null,
    street_average: null,
    area_demand: null,
    display_context: "unknown",
    confidence: null,
    source_label: null,
    success: partial?.success ?? true,
    message: partial?.message ?? null,
  };
}
