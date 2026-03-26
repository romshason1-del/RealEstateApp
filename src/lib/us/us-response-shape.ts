/**
 * Build API-ready US response objects — scaffold / empty shells only (no fabricated values).
 */

import type { USNYCApiTruthResponse, USPropertyValueResponse } from "./us-property-response-contract";

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

export function createEmptyUSNYCApiTruthResponse(partial?: Partial<USNYCApiTruthResponse>): USNYCApiTruthResponse {
  const base: USNYCApiTruthResponse = {
    success: true,
    message: null,
    has_truth_property_row: false,
    estimated_value: null,
    latest_sale_price: null,
    latest_sale_date: null,
    latest_sale_total_units: null,
    avg_street_price: null,
    avg_street_price_per_sqft: null,
    transaction_count: null,
    price_per_sqft: null,
    sales_address: null,
    pluto_address: null,
    street_name: null,
  };
  return { ...base, ...partial };
}
