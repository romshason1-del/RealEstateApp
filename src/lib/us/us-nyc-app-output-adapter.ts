/**
 * Maps a BigQuery row from `us_nyc_app_output_final_v4` into the main property-value payload shape.
 * Uses only {@link NYC_APP_OUTPUT_V4_COL}. Presentation rules live here — not on the client.
 * US only.
 */

import { coerceBigQueryDateToYyyyMmDd } from "@/lib/us/us-bq-date";
import {
  formatNycAppOutputBuildingTypeLabel,
  NYC_APP_OUTPUT_V4_COL as C,
} from "@/lib/us/us-nyc-app-output-schema";

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

export type NycDisplayHierarchy = "EXACT" | "BUILDING" | "STREET" | "NONE";

/** Maps production `final_display_mode` to UI hierarchy (EXACT → BUILDING → STREET → NONE). */
function mapFinalDisplayModeToHierarchy(mode: string): NycDisplayHierarchy {
  const m = String(mode ?? "").toUpperCase().trim();
  switch (m) {
    case "FULL_DATA":
    case "LAST_SALE_ONLY":
      return "EXACT";
    case "VALUATION_ONLY":
      return "STREET";
    case "ASK_APARTMENT":
      return "BUILDING";
    case "NO_DATA":
    case "BLOCKED_NON_RESIDENTIAL":
      return "NONE";
    default:
      return "EXACT";
  }
}

/** Maps production `final_confidence` to HIGH | MEDIUM | LOW | NONE. */
function mapFinalConfidence(raw: string): "HIGH" | "LOW" | "NONE" | "MEDIUM" {
  const s = String(raw ?? "").toUpperCase().trim();
  if (s === "VERY_HIGH" || s === "HIGH") return "HIGH";
  if (s === "MEDIUM") return "MEDIUM";
  if (s === "LOW") return "LOW";
  if (s === "RESTRICTED" || s === "NONE") return "NONE";
  return "HIGH";
}

function hierarchyToValueLevel(
  h: NycDisplayHierarchy
): "property-level" | "building-level" | "street-level" | "area-level" | "no_match" {
  switch (h) {
    case "EXACT":
      return "property-level";
    case "BUILDING":
      return "building-level";
    case "STREET":
      return "street-level";
    case "NONE":
      return "no_match";
    default:
      return "property-level";
  }
}

function neighborhoodScoreDisplay(row: Record<string, unknown>): string | null {
  const label = str(row, C.neighborhood_score_label);
  if (label) return label;
  const n = num(row, C.neighborhood_score);
  if (n != null) return String(n);
  return null;
}

/**
 * @param row — BigQuery row or null when no match
 */
export function adaptNycAppOutputRowToPropertyPayload(
  row: Record<string, unknown> | null,
  parsed: { city: string; street: string; houseNumber: string },
  opts?: { unitOrLotSubmitted?: string | null }
): Record<string, unknown> {
  const city = parsed.city.trim() || "—";
  const street = parsed.street.trim() || "—";
  const houseNumber = parsed.houseNumber.trim() || "—";
  const address = { city, street, house_number: houseNumber };
  const unitSub = opts?.unitOrLotSubmitted?.trim() ?? null;

  if (!row) {
    return {
      success: true,
      data_source: "us_nyc_app_output_v4",
      nyc_bq_row_matched: false,
      message: "No Data Available",
      status: null,
      address,
      nyc_display_hierarchy: "NONE" as const,
      nyc_match_confidence: "NONE" as const,
      nyc_has_exact_transaction: false,
      nyc_show_street_reference: false,
      nyc_street_reference: null,
      nyc_show_search_another_cta: true,
      nyc_neighborhood_score: null,
      nyc_building_type_display: null,
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
      should_prompt_for_unit: false,
      unit_prompt_reason: "insufficient_evidence",
      unit_classification: "unknown",
      unit_lookup_status: "not_requested" as const,
      unit_or_lot_submitted: unitSub,
    };
  }

  const displayModeRaw = str(row, C.final_display_mode) ?? "";
  const hierarchyRaw = mapFinalDisplayModeToHierarchy(displayModeRaw);
  const confidenceRaw = mapFinalConfidence(str(row, C.final_confidence) ?? "");

  const isBlockedCommercial = displayModeRaw.toUpperCase().trim() === "BLOCKED_NON_RESIDENTIAL";
  const isNoDataMode = displayModeRaw.toUpperCase().trim() === "NO_DATA";
  /**
   * Only true no-data rows in BigQuery: explicit NO_DATA or blocked commercial.
   * Do NOT treat LOW confidence, ASK_APARTMENT, or confidence NONE/RESTRICTED alone as missing data —
   * those still have a real row and must return a full card payload.
   */
  const isNone = isNoDataMode || isBlockedCommercial;

  const hasExactRaw = bool(row, C.has_exact_transaction);
  const nyc_has_exact_transaction = !isNone && hasExactRaw;

  const ev = num(row, C.final_value_amount);
  const lastPrice = num(row, C.last_transaction_amount);
  const lastDate = coerceBigQueryDateToYyyyMmDd(row[C.last_transaction_date]);

  const streetRefPrice = num(row, C.street_transaction_price);
  const streetRefDate = coerceBigQueryDateToYyyyMmDd(row[C.street_transaction_date]);
  const streetRefAddr = str(row, C.street_transaction_source_address);

  const ppsf = num(row, C.final_specific_price_per_sqft);
  const neigh = neighborhoodScoreDisplay(row);
  const bldgRaw = str(row, C.building_type);
  const bldgDisplay = formatNycAppOutputBuildingTypeLabel(bldgRaw);

  const showStreetRef =
    !isNone &&
    confidenceRaw === "LOW" &&
    ((streetRefPrice != null && streetRefPrice > 0) || streetRefDate != null || (streetRefAddr?.length ?? 0) > 0);

  const shouldPrompt = !isNone && bool(row, C.requires_apartment_number);

  const out: Record<string, unknown> = {
    success: true,
    data_source: "us_nyc_app_output_v4",
    nyc_bq_row_matched: true,
    message: isNone ? "No Data Available" : null,
    nyc_display_hierarchy: isNone ? "NONE" : hierarchyRaw,
    nyc_match_confidence: isNone ? "NONE" : confidenceRaw,
    nyc_has_exact_transaction,
    nyc_show_street_reference: showStreetRef,
    nyc_street_reference: showStreetRef
      ? {
          price: streetRefPrice,
          date: streetRefDate,
          source_address: streetRefAddr,
        }
      : null,
    nyc_show_search_another_cta: isNone,
    nyc_neighborhood_score: isNone ? null : neigh,
    nyc_building_type_display: isNone ? null : bldgDisplay,
    address,
    estimated_value: isNone ? null : ev,
    latest_sale_price: isNone || !nyc_has_exact_transaction ? null : lastPrice,
    latest_sale_date: isNone || !nyc_has_exact_transaction ? null : lastDate,
    latest_sale_total_units: num(row, C.total_units),
    price_per_sqft: isNone ? null : ppsf,
    property_result: {
      exact_value: isNone ? null : ev,
      exact_value_message: isNone ? null : ev != null && ev > 0 ? null : "Unavailable",
      value_level: hierarchyToValueLevel(isNone ? "NONE" : hierarchyRaw),
      last_transaction: {
        amount: isNone || !nyc_has_exact_transaction ? 0 : (lastPrice ?? 0),
        date: isNone || !nyc_has_exact_transaction ? null : lastDate,
        message:
          isNone || !nyc_has_exact_transaction
            ? null
            : lastPrice != null && lastPrice > 0
              ? undefined
              : "No official sale recorded",
      },
      street_average: null,
      street_average_message: null,
      livability_rating: "FAIR",
    },
    should_prompt_for_unit: shouldPrompt,
    unit_prompt_reason: shouldPrompt ? "apartment_or_lot_required" : "not_multi_unit_or_unit_provided",
    unit_classification: shouldPrompt ? "multi_unit_building" : "single_property",
    unit_lookup_status: "not_requested" as const,
    unit_or_lot_submitted: unitSub,
  };

  if (nyc_has_exact_transaction && lastPrice != null && lastPrice > 0 && lastDate) {
    out.last_sale = { price: lastPrice, date: lastDate };
  }

  if (isBlockedCommercial) {
    out.status = "commercial_property";
  } else if (shouldPrompt) {
    out.status = "requires_unit";
  }

  return out;
}
