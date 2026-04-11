/**
 * Maps a BigQuery row from `us_nyc_property_ui_production_v10` into the property-value API payload.
 * Pass-through of DB fields — no valuation math, no alternate sources. US only.
 */

import { coerceBigQueryDateToYyyyMmDd } from "@/lib/us/us-bq-date";
import {
  NYC_PROPERTY_UI_PRODUCTION_V10_BQ_COLUMNS,
  NYC_PROPERTY_UI_PRODUCTION_V10_COL as P,
} from "@/lib/us/us-nyc-property-ui-production-schema";

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

/** BQ `value_display_type` may be `RANGE`, `Estimated Market Range`, etc. */
function isRangeValueDisplayType(raw: string | null): boolean {
  const s = String(raw ?? "").toUpperCase().trim();
  return s === "RANGE" || s.includes("RANGE");
}

function finalMatchScopeUpper(row: Record<string, unknown>): string {
  return str(row, P.final_match_scope)?.toUpperCase().trim() ?? "";
}

/** Unit-level row (any EXACT_UNIT* grain). */
function isExactUnitFinalScope(scopeUpper: string): boolean {
  return scopeUpper.startsWith("EXACT_UNIT");
}

function isBuildingGrainFinalScope(scopeUpper: string): boolean {
  return (
    scopeUpper === "BUILDING" ||
    scopeUpper === "BUILDING_RECENT_SALES" ||
    scopeUpper === "EXACT_HOUSE"
  );
}

/**
 * Legacy `nyc_match_scope` bucket for existing clients — derived only from `final_match_scope`.
 */
function legacyNycMatchScopeFromFinal(scopeUpper: string): string | null {
  if (!scopeUpper) return null;
  if (isExactUnitFinalScope(scopeUpper)) return "EXACT_UNIT";
  if (isBuildingGrainFinalScope(scopeUpper)) return "BUILDING";
  return scopeUpper;
}

function productionPassthroughAll(row: Record<string, unknown>): Record<string, unknown> {
  const o: Record<string, unknown> = {};
  for (const col of NYC_PROPERTY_UI_PRODUCTION_V10_BQ_COLUMNS) {
    o[col] = passthrough(row, col);
  }
  return o;
}

function nullProductionAll(): Record<string, unknown> {
  const o: Record<string, unknown> = {};
  for (const col of NYC_PROPERTY_UI_PRODUCTION_V10_BQ_COLUMNS) {
    o[col] = null;
  }
  return o;
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

  const nullProduction = nullProductionAll();

  if (!row) {
    return { ...emptyCore, ...nullProduction };
  }

  const finalScopeUpper = finalMatchScopeUpper(row);
  const isExactUnitScope = isExactUnitFinalScope(finalScopeUpper);
  const isBuildingScope = isBuildingGrainFinalScope(finalScopeUpper);
  const legacyMatchScope = legacyNycMatchScopeFromFinal(finalScopeUpper);
  const unitSubmitted = !!unitSub?.trim();
  const shouldPrompt =
    !unitSubmitted &&
    "requires_apartment_number" in row &&
    bool(row, "requires_apartment_number");

  const valueTypeRaw = str(row, P.value_display_type);
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

  const estimatedValueBridge = isRangeValueDisplayType(valueTypeRaw) ? null : pointVal ?? null;

  const confidenceLabel = str(row, P.confidence_label);
  const uiCardNorm = normalizeUiCardType(str(row, P.ui_card_type));
  const isNonResidential = uiCardNorm === "NON_RESIDENTIAL_BLOCKED";

  const productionFlat = productionPassthroughAll(row);

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
    nyc_match_scope: legacyMatchScope,
    nyc_last_transaction_scope,
    nyc_verified_source_unit_for_data:
      isNonResidential || isBuildingScope ? null : str(row, P.normalized_unit_number),
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
