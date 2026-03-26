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

/**
 * NYC: response built only from BigQuery `streetiq_gold.us_nyc_api_truth` (exact address match).
 */
export interface USNYCApiTruthResponse {
  success: boolean;
  message: string | null;
  /** True iff BigQuery returned a matching row from `us_nyc_api_truth` (deterministic; not debug-only). */
  has_truth_property_row: boolean;
  estimated_value: number | null;
  latest_sale_price: number | null;
  latest_sale_date: string | null;
  /** Unit count tied to the matched latest sale row in truth data (not street-level). */
  latest_sale_total_units: number | null;
  avg_street_price: number | null;
  avg_street_price_per_sqft: number | null;
  transaction_count: number | null;
  price_per_sqft: number | null;
  sales_address: string | null;
  pluto_address: string | null;
  street_name: string | null;
  /** Optional apartment/lot refinement: exact BigQuery address match on `pluto_address`/`sales_address` with `, UNIT`. */
  unit_lookup_status: "not_requested" | "matched" | "not_found";
  unit_or_lot_submitted: string | null;
}

/** NYC main `/api/property-value` payload extensions (after {@link adaptUsNycTruthJsonForMainPropertyValueRoute}). */
export type USNycUnitClassification = "single_property" | "multi_unit_building" | "unknown";

export interface USNycMainPropertyValueUnitFields {
  unit_classification: USNycUnitClassification;
  should_prompt_for_unit: boolean;
  unit_prompt_reason: string;
}
