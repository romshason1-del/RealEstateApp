/**
 * Canonical BigQuery column names for `real_estate_us.us_nyc_app_output_final_v5` (production: `streetiq-bigquery`, location **US**).
 * Verified against INFORMATION_SCHEMA — update only this map when the table changes.
 */

import { normalizeNycBuildingTypeKey } from "@/lib/us/us-nyc-precomputed-card";

/**
 * One semantic → one column name (no alternate fallbacks elsewhere).
 * v5 adds `match_scope` (EXACT_UNIT | BUILDING), `unit`, `normalized_unit_number` for address+unit matching.
 *
 * {@link NYC_APP_OUTPUT_V4_COL} is a legacy alias for scripts that still import the old name.
 */
export const NYC_APP_OUTPUT_V5_COL = {
  lookup_address: "lookup_address",
  property_address: "property_address",
  match_scope: "match_scope",
  unit: "unit",
  normalized_unit_number: "normalized_unit_number",
  final_display_mode: "final_display_mode",
  final_confidence: "final_confidence",
  has_exact_transaction: "has_exact_transaction",
  final_value_amount: "final_value_amount",
  last_transaction_amount: "last_transaction_amount",
  last_transaction_date: "last_transaction_date",
  total_units: "total_units",
  street_transaction_price: "street_transaction_price",
  street_transaction_date: "street_transaction_date",
  street_transaction_source_address: "street_transaction_source_address",
  final_specific_price_per_sqft: "final_specific_price_per_sqft",
  neighborhood_score: "neighborhood_score",
  neighborhood_score_label: "neighborhood_score_label",
  building_type: "building_type",
  requires_apartment_number: "requires_apartment_number",
} as const;

/** @deprecated Use {@link NYC_APP_OUTPUT_V5_COL}. */
export const NYC_APP_OUTPUT_V4_COL = NYC_APP_OUTPUT_V5_COL;

/** Human-readable building category for NYC card (matches gold-layer display conventions). */
export function formatNycAppOutputBuildingTypeLabel(raw: string | null | undefined): string {
  const k = normalizeNycBuildingTypeKey(String(raw ?? ""));
  switch (k) {
    case "single_family":
      return "Single Family";
    case "two_family":
      return "Two Family";
    case "small_multifamily":
      return "Walk-Up Apartments";
    case "large_multifamily":
      return "Elevator Apartments";
    case "co_op":
    case "coop":
      return "Co-op Building";
    case "condo":
      return "Condo Building";
    case "mixed_use":
      return "Mixed Use";
    case "vacant":
      return "Vacant Land";
    default:
      const t = String(raw ?? "").trim();
      return t ? t.replace(/_/g, " ") : "—";
  }
}
