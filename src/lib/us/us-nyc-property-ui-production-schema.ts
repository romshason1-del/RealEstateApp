/**
 * BigQuery column names for `real_estate_us.us_nyc_property_ui_production_v10` (streetiq-bigquery, US).
 * Pass-through only — do not rename in the API layer.
 *
 * `match_scope` is used for address+unit SQL matching (same semantics as prior NYC gold tables).
 */

export const NYC_PROPERTY_UI_PRODUCTION_V10_COL = {
  lookup_address: "lookup_address",
  /** Row grain for SQL matching (EXACT_UNIT | BUILDING); required for lookup. */
  match_scope: "match_scope",
  /** Unit identifiers for SQL + verified-source display. */
  unit: "unit",
  normalized_unit_number: "normalized_unit_number",
  property_type_display: "property_type_display",
  size_sqft_final: "size_sqft_final",
  display_estimated_value: "display_estimated_value",
  display_estimated_value_low: "display_estimated_value_low",
  display_estimated_value_high: "display_estimated_value_high",
  value_display_type: "value_display_type",
  value_explanation: "value_explanation",
  display_value_is_estimate: "display_value_is_estimate",
  last_transaction_amount: "last_transaction_amount",
  last_transaction_date: "last_transaction_date",
  since_last_sale_pct_display: "since_last_sale_pct_display",
  deal_score_numeric: "deal_score_numeric",
  below_or_above_market_pct: "below_or_above_market_pct",
  below_or_above_market_label: "below_or_above_market_label",
  nearby_sales_count: "nearby_sales_count",
  confidence_label: "confidence_label",
  explanation_display: "explanation_display",
  potential_deal_display: "potential_deal_display",
  ui_card_type: "ui_card_type",
  fallback_message: "fallback_message",
  primary_cta_label: "primary_cta_label",
  requires_apartment_number: "requires_apartment_number",
} as const;
