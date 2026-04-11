/**
 * Maps a BigQuery row from `us_nyc_property_ui_production_v10` into the property-value API payload.
 * Pass-through of DB fields — no valuation math, no alternate sources. US only.
 */

import { coerceBigQueryDateToYyyyMmDd } from "@/lib/us/us-bq-date";
import { NYC_PROPERTY_UI_PRODUCTION_V10_COL as P } from "@/lib/us/us-nyc-property-ui-production-schema";

function num(row: Record<string, unknown>, key: string): number | null {
  const v = row[key];
  if (v == null || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string" && v.trim() !== "") {
    const x = Number(v);
    if (Number.isFinite(x)) return x;
  }
  if (typeof v === "object" && v !== null && "value" in (v as object)) {
    const x = Number((v as { value: string }).value);
    if (Number.isFinite(x)) return x;
  }
  return null;
}

function str(row: Record<string, unknown>, key: string): string | null {
  const v = row[key];
  if (v == null || v === "") return null;
  const s = String(v).trim();
  return s ? s : null;
}

function bool(row: Record<string, unknown>, key: string): boolean {
  const v = row[key];
  if (v === true) return true;
  if (v === false) return false;
  if (typeof v === "string") {
    const u = v.toUpperCase();
    if (u === "TRUE" || u === "1" || u === "Y" || u === "YES") return true;
    if (u === "FALSE" || u === "0" || u === "N" || u === "NO") return false;
  }
  return false;
}

/** BigQuery value as JSON-safe (no fabrication when missing). */
function passthrough(row: Record<string, unknown> | null, key: string): unknown {
  if (!row || !(key in row)) return null;
  const v = row[key];
  if (v === undefined) return null;
  return v;
}

function mapConfidenceLabel(raw: string | null): "HIGH" | "LOW" | "NONE" | "MEDIUM" {
  const s = String(raw ?? "").toUpperCase().trim();
  if (!s) return "NONE";
  if (s.includes("VERY_HIGH") || s.includes("HIGH")) return "HIGH";
  if (s.includes("MEDIUM")) return "MEDIUM";
  if (s.includes("LOW")) return "LOW";
  if (s.includes("NONE") || s.includes("RESTRICT")) return "NONE";
  return "MEDIUM";
}

function normalizeUiCardType(raw: string | null): string {
  return String(raw ?? "")
    .toUpperCase()
    .trim()
    .replace(/\s+/g, "_");
}

/**
 * @param row — BigQuery row or null when no match
 */
export function adaptNycPropertyUiProductionRowToPropertyPayload(
  row: Record<string, unknown> | null,
  parsed: { city: string; street: string; houseNumber: string },
  opts?: { unitOrLotSubmitted?: string | null }
): Record<string, unknown> {
  const city = parsed.city.trim() || "—";
  const street = parsed.street.trim() || "—";
  const houseNumber = parsed.houseNumber.trim() || "—";
  const address = { city, street, house_number: houseNumber };
  const unitSub = opts?.unitOrLotSubmitted?.trim() ?? null;

  const emptyCore = {
    success: true as const,
    data_source: "us_nyc_property_ui_production_v10",
    nyc_bq_row_matched: false,
    message: "No Data Available",
    status: null as string | null,
    address,
    should_prompt_for_unit: false,
    unit_or_lot_submitted: unitSub,
    unit_lookup_status: "not_requested" as const,
    unit_prompt_reason: "insufficient_evidence",
    unit_classification: "unknown",
    nyc_display_hierarchy: "NONE" as const,
    nyc_match_confidence: "NONE" as const,
    nyc_has_exact_transaction: false,
    nyc_show_street_reference: false,
    nyc_street_reference: null,
    nyc_show_search_another_cta: true,
    nyc_neighborhood_score: null,
    nyc_building_type_display: null,
    nyc_final_display_mode: null,
    nyc_match_scope: null,
    nyc_last_transaction_scope: null,
    nyc_verified_source_unit_for_data: null,
    nyc_ui_non_residential_blocked: false,
    estimated_value: null,
    latest_sale_price: null,
    latest_sale_date: null,
    latest_sale_total_units: null,
    price_per_sqft: null,
    property_result: {
      exact_value: null,
      exact_value_message: null,
      value_level: "no_match" as const,
      last_transaction: { amount: 0, date: null, message: null },
      street_average: null,
      street_average_message: null,
      livability_rating: "FAIR" as const,
    },
 };

  const nullProduction: Record<string, unknown> = {
    [P.lookup_address]: null,
    [P.property_type_display]: null,
    [P.size_sqft_final]: null,
    [P.display_estimated_value]: null,
    [P.display_estimated_value_low]: null,
    [P.display_estimated_value_high]: null,
    [P.value_display_type]: null,
    [P.value_explanation]: null,
    [P.display_value_is_estimate]: null,
    [P.last_transaction_amount]: null,
    [P.last_transaction_date]: null,
    [P.since_last_sale_pct_display]: null,
    [P.deal_score_numeric]: null,
    [P.below_or_above_market_pct]: null,
    [P.below_or_above_market_label]: null,
    [P.nearby_sales_count]: null,
    [P.confidence_label]: null,
    [P.explanation_display]: null,
    [P.potential_deal_display]: null,
    [P.ui_card_type]: null,
    [P.fallback_message]: null,
    [P.primary_cta_label]: null,
  };

  if (!row) {
    return { ...emptyCore, ...nullProduction };
  }

  const matchScopeRaw = str(row, P.match_scope)?.toUpperCase().trim() ?? "";
  const isExactUnitScope = matchScopeRaw === "EXACT_UNIT";
  const isBuildingScope = matchScopeRaw === "BUILDING";
  const unitSubmitted = !!unitSub?.trim();
  const shouldPrompt =
    !unitSubmitted && bool(row, P.requires_apartment_number);

  const valueType = str(row, P.value_display_type)?.toUpperCase().trim() ?? "";
  const pointVal = num(row, P.display_estimated_value);
  const lastAmt = num(row, P.last_transaction_amount);
  const lastDateRaw = row[P.last_transaction_date];
  const lastDate = coerceBigQueryDateToYyyyMmDd(lastDateRaw);
  const hasLastSale = lastAmt != null && lastAmt > 0;

  const nyc_last_transaction_scope: "exact_unit" | "building" | null = !hasLastSale
    ? null
    : isExactUnitScope
      ? "exact_unit"
      : isBuildingScope
        ? "building"
        : "exact_unit";

  const estimatedValueBridge = valueType === "RANGE" ? null : pointVal ?? null;

  const confidenceLabel = str(row, P.confidence_label);
  const uiCardNorm = normalizeUiCardType(str(row, P.ui_card_type));
  const isNonResidential = uiCardNorm === "NON_RESIDENTIAL_BLOCKED";

  const productionFlat: Record<string, unknown> = {
    [P.lookup_address]: passthrough(row, P.lookup_address),
    [P.property_type_display]: passthrough(row, P.property_type_display),
    [P.size_sqft_final]: passthrough(row, P.size_sqft_final),
    [P.display_estimated_value]: passthrough(row, P.display_estimated_value),
    [P.display_estimated_value_low]: passthrough(row, P.display_estimated_value_low),
    [P.display_estimated_value_high]: passthrough(row, P.display_estimated_value_high),
    [P.value_display_type]: passthrough(row, P.value_display_type),
    [P.value_explanation]: passthrough(row, P.value_explanation),
    [P.display_value_is_estimate]: passthrough(row, P.display_value_is_estimate),
    [P.last_transaction_amount]: passthrough(row, P.last_transaction_amount),
    [P.last_transaction_date]: passthrough(row, P.last_transaction_date),
    [P.since_last_sale_pct_display]: passthrough(row, P.since_last_sale_pct_display),
    [P.deal_score_numeric]: passthrough(row, P.deal_score_numeric),
    [P.below_or_above_market_pct]: passthrough(row, P.below_or_above_market_pct),
    [P.below_or_above_market_label]: passthrough(row, P.below_or_above_market_label),
    [P.nearby_sales_count]: passthrough(row, P.nearby_sales_count),
    [P.confidence_label]: passthrough(row, P.confidence_label),
    [P.explanation_display]: passthrough(row, P.explanation_display),
    [P.potential_deal_display]: passthrough(row, P.potential_deal_display),
    [P.ui_card_type]: passthrough(row, P.ui_card_type),
    [P.fallback_message]: passthrough(row, P.fallback_message),
    [P.primary_cta_label]: passthrough(row, P.primary_cta_label),
  };

  const out: Record<string, unknown> = {
    ...productionFlat,
    success: true,
    data_source: "us_nyc_property_ui_production_v10",
    nyc_bq_row_matched: true,
    /** Client: skip generic “no NYC record” empty-state when only `fallback_message` should show. */
    nyc_ui_non_residential_blocked: isNonResidential,
    message: null,
    status: isNonResidential ? "non_residential_blocked" : shouldPrompt ? "requires_unit" : null,
    address,
    nyc_final_display_mode: shouldPrompt ? "ASK_APARTMENT" : null,
    nyc_match_scope: matchScopeRaw || null,
    nyc_last_transaction_scope,
    nyc_verified_source_unit_for_data:
      isNonResidential || isBuildingScope
        ? null
        : str(row, P.normalized_unit_number) ?? str(row, P.unit),
    nyc_display_hierarchy: isNonResidential ? "NONE" : isBuildingScope ? "BUILDING" : "EXACT",
    nyc_match_confidence: isNonResidential ? "NONE" : mapConfidenceLabel(confidenceLabel),
    nyc_has_exact_transaction: !isNonResidential && hasLastSale,
    nyc_show_street_reference: false,
    nyc_street_reference: null,
    nyc_show_search_another_cta: false,
    nyc_neighborhood_score: isNonResidential ? null : confidenceLabel,
    nyc_building_type_display: isNonResidential ? null : str(row, P.property_type_display),
    estimated_value: isNonResidential ? null : estimatedValueBridge,
    latest_sale_price: isNonResidential || !hasLastSale ? null : lastAmt,
    latest_sale_date: isNonResidential || !hasLastSale ? null : lastDate,
    latest_sale_total_units: null,
    price_per_sqft: null,
    property_result: {
      exact_value: isNonResidential ? null : estimatedValueBridge,
      exact_value_message: isNonResidential ? null : estimatedValueBridge != null ? null : "Unavailable",
      value_level: isNonResidential ? "no_match" : isBuildingScope ? "building-level" : "property-level",
      last_transaction: {
        amount: isNonResidential || !hasLastSale ? 0 : (lastAmt ?? 0),
        date: isNonResidential || !hasLastSale ? null : lastDate,
        message:
          isNonResidential || !hasLastSale
            ? null
            : lastAmt != null && lastAmt > 0
              ? undefined
              : "No official sale recorded",
      },
      street_average: null,
      street_average_message: null,
      livability_rating: "FAIR",
    },
    should_prompt_for_unit: !isNonResidential && shouldPrompt,
    unit_prompt_reason: shouldPrompt ? "apartment_or_lot_required" : "not_multi_unit_or_unit_provided",
    unit_classification: shouldPrompt ? "multi_unit_building" : "single_property",
    unit_lookup_status: "not_requested",
    unit_or_lot_submitted: unitSub,
  };

  if (!isNonResidential && hasLastSale && lastDate) {
    out.last_sale = { price: lastAmt, date: lastDate };
  }

  return out;
}
