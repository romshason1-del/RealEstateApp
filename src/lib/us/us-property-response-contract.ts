/**
 * US property value API response contract (StreetIQ-aligned).
 * Isolated from France types — do not import france-response-contract here.
 */

export type USDisplayContext =
  | "exact_property"
  | "building_level"
  | "street_level"
  | "area_level"
  | "unknown";

/** Last official / comparable sale disclosure block (shape only; values null in scaffold). */
export interface USLastTransaction {
  amount: number | null;
  date: string | null;
  match_type: string | null;
  disclosure: string | null;
  source_address: string | null;
}

/**
 * Core US response surface for UI + API.
 * Populated later from official/public datasets via BigQuery (US pipeline only).
 */
export interface USPropertyValueResponse {
  country_code: "US";
  estimated_value: number | null;
  last_transaction: USLastTransaction | null;
  street_average: number | null;
  area_demand: string | null;
  display_context: USDisplayContext;
  confidence: string | null;
  source_label: string | null;
  success: boolean;
  message: string | null;
}
