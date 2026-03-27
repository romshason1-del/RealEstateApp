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
 * NYC: response from BigQuery (precomputed card v5 + last-transaction engine v3, exact `full_address` match).
 */
export interface USNYCApiTruthResponse {
  success: boolean;
  message: string | null;
  /** True iff BigQuery returned a matching card row (deterministic; not debug-only). */
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
  /** Precomputed pipeline: skip runtime ACRIS/DOB in main NYC adapter. */
  nyc_precomputed_card?: boolean;
  nyc_card_full_address?: string | null;
  nyc_card_badge_1?: string | null;
  nyc_card_badge_2?: string | null;
  nyc_card_badge_3?: string | null;
  nyc_card_badge_4?: string | null;
  nyc_estimated_value_subtext?: string | null;
  nyc_price_per_sqft_text?: string | null;
  nyc_final_match_level?: string | null;
  nyc_final_last_transaction_text?: string | null;
  nyc_final_transaction_match_level?: string | null;
  /** True when multi-unit / condo rules require apartment or lot before treating last sale as unit-specific. */
  nyc_pending_unit_prompt?: boolean;
  /** When last sale is withheld from top-level fields (e.g. similar-only while unit pending). */
  nyc_last_transaction_unavailable_reason?: string | null;
}

/** NYC main `/api/property-value` payload extensions (after {@link adaptUsNycTruthJsonForMainPropertyValueRoute}). */
export type USNycUnitClassification = "single_property" | "multi_unit_building" | "unknown";

export interface USNycMainPropertyValueUnitFields {
  unit_classification: USNycUnitClassification;
  should_prompt_for_unit: boolean;
  unit_prompt_reason: string;
}
