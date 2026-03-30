/**
 * NYC property card rows from BigQuery precomputed tables only:
 * `us_nyc_card_output_v5` + `us_nyc_last_transaction_engine_v3` (exact `full_address`).
 */

import { coerceBigQueryDateToYyyyMmDd } from "./us-bq-date";
import { getUSBigQueryClient } from "./bigquery-client";
import type { USNYCApiTruthResponse } from "./us-property-response-contract";
import {
  computeNycNeedsUnitPrompt,
  mapPrecomputedJoinRowToUSNYCApiTruthResponse,
  normalizeNycBuildingTypeKey,
  queryPrecomputedNycCardJoinRow,
  queryPrecomputedNycStreetFallbackRow,
  US_NYC_CARD_OUTPUT_V5_REFERENCE,
  US_NYC_LAST_TX_ENGINE_V3_REFERENCE,
  US_NYC_PRECOMPUTED_CARD_SQL_WHERE,
  US_NYC_PRECOMPUTED_JOIN_QUERY,
  US_NYC_STREET_FALLBACK_JOIN_QUERY,
} from "./us-nyc-precomputed-card";
import {
  buildNycTruthLookupCandidates,
  buildNycTruthLookupNormalizationDebug,
  NYC_CANDIDATE_GENERATOR_VERSION,
} from "./us-nyc-address-normalize";
import { shouldApplyNycDebugKnownAddressUnitPromptOverride } from "./us-nyc-debug-known-addresses";
import { augmentNycTruthCandidatesWithAddressMaster } from "./us-nyc-address-master";

/** @deprecated Use {@link US_NYC_CARD_OUTPUT_V5_REFERENCE}; kept for `/api/us/property-value` debug strings. */
export const US_NYC_API_TRUTH_TABLE_REFERENCE = US_NYC_CARD_OUTPUT_V5_REFERENCE;

export const US_NYC_API_TRUTH_SQL_WHERE = US_NYC_PRECOMPUTED_CARD_SQL_WHERE;

const NYC_TRUTH_QUERY_LOCATION = "EU";

const EMPTY_TRUTH: USNYCApiTruthResponse = {
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
  unit_lookup_status: "not_requested",
  unit_or_lot_submitted: null,
};

function collapseSpaces(s: string): string {
  return s.replace(/\s+/g, " ").trim();
}

function logNycPrecomputedLookupMiss(payload: Record<string, unknown>): void {
  if (process.env.NODE_ENV === "test") return;
  try {
    console.log("[NYC_PRECOMPUTED_LOOKUP_MISS]", JSON.stringify(payload));
  } catch {
    /* ignore */
  }
}

/**
 * Deterministic unit/lot token for `full_address` equality (uppercase, strip APT/UNIT/LOT/#, collapse space/hyphens).
 */
export function normalizeUnitOrLotForTruthLookup(raw: string): string {
  let s = raw.replace(/\s+/g, " ").trim();
  if (!s) return "";
  s = s.toUpperCase();
  let prev = "";
  while (prev !== s) {
    prev = s;
    s = s.replace(/^(APT|APARTMENT|UNIT|LOT)\s*\.?\s*#?\s*/i, "");
    s = s.replace(/^#\s*/, "");
  }
  s = s.replace(/[-–—]/g, " ");
  return collapseSpaces(s);
}

function withUnitLookup(
  r: USNYCApiTruthResponse,
  status: USNYCApiTruthResponse["unit_lookup_status"],
  submitted: string | null
): USNYCApiTruthResponse {
  return { ...r, unit_lookup_status: status, unit_or_lot_submitted: submitted };
}

/** TEMPORARY: remove after production debugging. */
export type USNYCApiTruthQueryDebug = {
  original_input: string;
  normalized_full_address: string;
  normalized_building_address: string;
  zip_from_input?: string | null;
  table_name_used: string;
  sql_where_used: string;
  rows_found_count: number;
  first_row_if_any: Record<string, unknown> | null;
  candidates_tried: readonly string[];
  attempts: readonly { candidate: string; rows_returned: number }[];
  /** First BigQuery candidate that returned a card row (exact `full_address` match). */
  final_selected_candidate?: string | null;
  matched_full_address?: string | null;
  precomputed_row_matched?: boolean;
  bigquery_location: string;
  full_sql_template: string;
  nyc_last_transaction_engine_table?: string;
  candidate_generator_version?: number;
  nyc_street_fallback_used?: boolean;
  nyc_street_fallback_patterns?: readonly string[];
  searched_address?: string;
  fallback_used?: boolean;
  fallback_type?: string | null;
  fallback_score_reason?: string | null;
  matched_building_type?: unknown;
  matched_unit_count?: unknown;
  should_prompt_for_unit?: boolean;
  nyc_pending_unit_prompt?: boolean;
  unit_prompt_reason?: string | null;
  same_street_pool?: boolean;
  /** `us_nyc_address_master_v1` + building_truth BBL bridge (suffix-normalized key). */
  address_master_normalized?: string | null;
  address_master_hint_full_addresses?: readonly string[];
};

function rowToJsonSafe(row: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(row)) {
    if (v instanceof Date) out[k] = v.toISOString().slice(0, 10);
    else if (typeof v === "bigint") out[k] = Number(v);
    else if (k === "latest_sale_date" || k.endsWith("_date")) {
      const d = coerceBigQueryDateToYyyyMmDd(v);
      out[k] = d ?? v;
    } else out[k] = v as unknown;
  }
  return out;
}

function parseUnitOrLotOptions(options?: { unitOrLot?: string | null }): {
  hasUnit: boolean;
  submitted: string | null;
} {
  const raw = options?.unitOrLot?.trim();
  if (!raw) return { hasUnit: false, submitted: null };
  const normalized = normalizeUnitOrLotForTruthLookup(raw);
  if (!normalized) return { hasUnit: false, submitted: null };
  return { hasUnit: true, submitted: normalized };
}

/** Street fallback: always prompt when card reports multiple units (even if building_type would skip). */
function nycRowUnitCountGtOne(row: Record<string, unknown>): boolean {
  const v = row.unit_count;
  if (v == null || v === "") return false;
  const n =
    typeof v === "number" && Number.isFinite(v) ? v : Number(String(v).replace(/,/g, ""));
  return Number.isFinite(n) && n > 1;
}

/** Multi-unit inventory / multifamily: prompt for unit on fallback when row clearly needs it. */
function nycFallbackLooksMultiUnit(
  row: Record<string, unknown>,
  addressLine: string,
  candidatesCount: number
): boolean {
  if (
    computeNycNeedsUnitPrompt(row, {
      addressLine,
      candidatesCount,
    })
  ) {
    return true;
  }
  if (nycRowUnitCountGtOne(row)) return true;
  const bt = normalizeNycBuildingTypeKey(String(row.building_type ?? ""));
  return ["condo", "co_op", "coop", "apartment", "large_multifamily", "small_multifamily", "mixed_use"].includes(
    bt
  );
}

function logNycFallbackDecision(payload: Record<string, unknown>): void {
  if (process.env.NODE_ENV === "test") return;
  try {
    console.log("[NYC_FALLBACK]", JSON.stringify(payload));
  } catch {
    /* ignore */
  }
}

async function queryNycStreetFallbackTruth(
  client: ReturnType<typeof getUSBigQueryClient>,
  rawInput: string | undefined,
  candidates: readonly string[],
  hasUnit: boolean,
  submitted: string | null,
  addressLineForPrompt: string
): Promise<{
  response: USNYCApiTruthResponse | null;
  patternsUsed: string[];
  rowsReturned: number;
  matchedRawRow: Record<string, unknown> | null;
  sameStreetPool: boolean;
}> {
  const norm = buildNycTruthLookupNormalizationDebug(rawInput ?? "");
  const line =
    norm?.normalized_building_address?.trim() ||
    norm?.normalized_full_address?.trim() ||
    (candidates[0] ?? "").trim();
  if (!line) {
    return { response: null, patternsUsed: [], rowsReturned: 0, matchedRawRow: null, sameStreetPool: false };
  }

  const {
    row,
    rowsReturned,
    patternsUsed,
    fallbackType,
    scoreReason,
    sameStreetPool,
    houseDistance,
  } = await queryPrecomputedNycStreetFallbackRow(client, line);
  if (!row || rowsReturned === 0) {
    // Log diagnostic info even on empty pool (helps confirm data coverage gap vs pattern issue).
    logNycFallbackDecision({
      searched_address: rawInput ?? "",
      fallback_used: false,
      fallback_type: null,
      fallback_score_reason: scoreReason,
      pool_size: rowsReturned,
      patterns_used: patternsUsed,
      normalized_line_used_for_patterns: line,
      matched_full_address: null,
      matched_building_type: null,
      matched_unit_count: null,
      house_distance: houseDistance,
      should_prompt_for_unit: false,
      nyc_pending_unit_prompt: false,
      unit_prompt_reason: null,
      note: rowsReturned === 0 ? "pool_empty_data_coverage_gap" : "pool_non_empty_ranking_returned_null",
    });
    return { response: null, patternsUsed, rowsReturned, matchedRawRow: null, sameStreetPool: false };
  }

  // Multi-unit inventory: never use another building's card row as a stand-in when the user has not
  // submitted a unit — prefer no match over anchoring prompts / badges to the wrong address.
  if (
    !hasUnit &&
    nycFallbackLooksMultiUnit(row, addressLineForPrompt || line, candidates.length) &&
    houseDistance != null &&
    houseDistance > 0
  ) {
    logNycFallbackDecision({
      searched_address: rawInput ?? "",
      fallback_used: false,
      fallback_type: fallbackType,
      fallback_score_reason: `${scoreReason};rejected:multi_unit_no_unit_requires_house_dist_0_got_${houseDistance}`,
      pool_size: rowsReturned,
      patterns_used: patternsUsed,
      normalized_line_used_for_patterns: line,
      matched_full_address: String(row.full_address ?? ""),
      matched_building_type: row.building_type,
      matched_unit_count: row.unit_count,
      house_distance: houseDistance,
      should_prompt_for_unit: false,
      nyc_pending_unit_prompt: false,
      unit_prompt_reason: null,
      note: "rejected_multi_unit_fallback_not_exact_house",
    });
    return { response: null, patternsUsed, rowsReturned, matchedRawRow: null, sameStreetPool: false };
  }

  const matchLevel = fallbackType ?? "street_fallback";

  let needPrompt =
    !hasUnit &&
    nycFallbackLooksMultiUnit(row, addressLineForPrompt || line, candidates.length);
  if (
    !needPrompt &&
    !hasUnit &&
    shouldApplyNycDebugKnownAddressUnitPromptOverride(rawInput ?? "", row, {
      addressLine: addressLineForPrompt || line,
      candidatesCount: candidates.length,
    })
  ) {
    needPrompt = true;
  }

  const mapOpts = {
    overrideFinalMatchLevel: matchLevel,
    ...(needPrompt ? { pendingUnitPrompt: true as const } : {}),
  };

  const mappedBase = mapPrecomputedJoinRowToUSNYCApiTruthResponse(row, mapOpts);
  const mapped = {
    ...mappedBase,
    nyc_fallback_used: true,
    nyc_fallback_type: fallbackType,
    nyc_fallback_score_reason: scoreReason,
  };

  logNycFallbackDecision({
    searched_address: rawInput ?? "",
    matched_full_address: mapped.nyc_card_full_address ?? mapped.sales_address,
    fallback_used: true,
    fallback_type: fallbackType,
    fallback_score_reason: scoreReason,
    same_street_pool: sameStreetPool,
    pool_size: rowsReturned,
    patterns_used: patternsUsed,
    normalized_line_used_for_patterns: line,
    matched_building_type: row.building_type,
    matched_unit_count: row.unit_count,
    house_distance: houseDistance,
    should_prompt_for_unit: needPrompt,
    nyc_pending_unit_prompt: needPrompt,
    unit_prompt_reason: needPrompt ? "apartment_or_lot_required" : null,
  });

  if (needPrompt) {
    return {
      response: withUnitLookup(
        {
          success: true,
          message: null,
          ...mapped,
        },
        "not_requested",
        null
      ),
      patternsUsed,
      rowsReturned,
      matchedRawRow: row,
      sameStreetPool,
    };
  }

  return {
    response: withUnitLookup(
      {
        success: true,
        message: null,
        ...mapped,
      },
      hasUnit ? "not_found" : "not_requested",
      submitted
    ),
    patternsUsed,
    rowsReturned,
    matchedRawRow: row,
    sameStreetPool,
  };
}

/**
 * Try each candidate in order (exact equality on `full_address` in precomputed card table).
 * When `unitOrLot` is set, tries `"{candidate}, {normalized unit}"` first, then building-level candidates.
 */
export async function queryUSNYCApiTruthWithCandidates(
  candidates: readonly string[],
  rawInput?: string,
  options?: { unitOrLot?: string | null }
): Promise<USNYCApiTruthResponse> {
  const client = getUSBigQueryClient();
  const normForMaster = buildNycTruthLookupNormalizationDebug(rawInput?.trim() || candidates[0] || "");
  let workCandidates = [...candidates];
  if (normForMaster) {
    const aug = await augmentNycTruthCandidatesWithAddressMaster(client, {
      ...normForMaster,
      candidates: workCandidates,
    });
    workCandidates = aug.candidates;
  }
  const { hasUnit, submitted } = parseUnitOrLotOptions(options);
  const attempts: { candidate: string; rows_returned: number }[] = [];

  if (hasUnit && submitted) {
    for (const address of workCandidates) {
      const trimmed = address.trim();
      if (!trimmed) continue;
      const suffixed = `${trimmed}, ${submitted}`;
      const { row, rowsReturned } = await queryPrecomputedNycCardJoinRow(client, suffixed);
      attempts.push({ candidate: suffixed, rows_returned: rowsReturned });
      if (row) {
        return withUnitLookup(
          {
            success: true,
            message: null,
            ...mapPrecomputedJoinRowToUSNYCApiTruthResponse(row),
          },
          "matched",
          submitted
        );
      }
    }
  }

  const addressLineForPrompt = (rawInput ?? "").trim() || (workCandidates[0] ?? "").trim();

  for (const address of workCandidates) {
    const trimmed = address.trim();
    if (!trimmed) continue;
    const { row, rowsReturned } = await queryPrecomputedNycCardJoinRow(client, trimmed);
    attempts.push({ candidate: trimmed, rows_returned: rowsReturned });
    if (row) {
      let needPrompt =
        !hasUnit &&
        computeNycNeedsUnitPrompt(row, {
          addressLine: addressLineForPrompt || trimmed,
          candidatesCount: workCandidates.length,
        });
      if (
        !needPrompt &&
        !hasUnit &&
        shouldApplyNycDebugKnownAddressUnitPromptOverride(rawInput ?? "", row, {
          addressLine: addressLineForPrompt || trimmed,
          candidatesCount: workCandidates.length,
        })
      ) {
        needPrompt = true;
      }

      if (needPrompt) {
        return withUnitLookup(
          {
            success: true,
            message: null,
            ...mapPrecomputedJoinRowToUSNYCApiTruthResponse(row, { pendingUnitPrompt: true }),
          },
          "not_requested",
          null
        );
      }

      return withUnitLookup(
        {
          success: true,
          message: null,
          ...mapPrecomputedJoinRowToUSNYCApiTruthResponse(row),
        },
        hasUnit ? "not_found" : "not_requested",
        submitted
      );
    }
  }

  const streetFb = await queryNycStreetFallbackTruth(
    client,
    rawInput,
    workCandidates,
    hasUnit,
    submitted,
    addressLineForPrompt
  );
  if (streetFb.response) {
    return streetFb.response;
  }

  logNycPrecomputedLookupMiss({
    searched_address: rawInput ?? "",
    generated_candidates: [...workCandidates],
    matched_full_address: null,
    final_selected_candidate: null,
    precomputed_row_matched: false,
    attempts,
  });

  return withUnitLookup({ ...EMPTY_TRUTH }, hasUnit ? "not_found" : "not_requested", submitted);
}

/**
 * Same as queryUSNYCApiTruthWithCandidates plus debug payload for /api/us/property-value.
 */
export async function queryUSNYCApiTruthWithCandidatesDebug(
  originalInput: string,
  norm: {
    normalized_full_address: string;
    normalized_building_address: string;
    candidates: readonly string[];
    zip_from_input?: string | null;
    candidate_generator_version?: number;
  },
  options?: { unitOrLot?: string | null }
): Promise<{ response: USNYCApiTruthResponse; debug: USNYCApiTruthQueryDebug }> {
  const cgv = norm.candidate_generator_version ?? NYC_CANDIDATE_GENERATOR_VERSION;
  const client = getUSBigQueryClient();
  const aug = await augmentNycTruthCandidatesWithAddressMaster(client, norm);
  const candidatesToUse = aug.candidates;
  const addrMasterDebug = {
    address_master_normalized: aug.masterNormalized,
    address_master_hint_full_addresses: aug.masterHintFullAddresses,
  };
  const attempts: { candidate: string; rows_returned: number }[] = [];
  let firstRow: Record<string, unknown> | null = null;
  const { hasUnit, submitted } = parseUnitOrLotOptions(options);

  if (hasUnit && submitted) {
    for (const address of candidatesToUse) {
      const trimmed = address.trim();
      if (!trimmed) continue;
      const suffixed = `${trimmed}, ${submitted}`;
      const { row, rowsReturned: n } = await queryPrecomputedNycCardJoinRow(client, suffixed);
      attempts.push({ candidate: suffixed, rows_returned: n });
      if (row) {
        firstRow = rowToJsonSafe(row);
        const response = withUnitLookup(
          {
            success: true,
            message: null,
            ...mapPrecomputedJoinRowToUSNYCApiTruthResponse(row),
          },
          "matched",
          submitted
        );
        const matchedAddr = firstRow.full_address != null ? String(firstRow.full_address) : null;
        return {
          response,
          debug: {
            original_input: originalInput,
            normalized_full_address: norm.normalized_full_address,
            normalized_building_address: norm.normalized_building_address,
            zip_from_input: norm.zip_from_input ?? null,
            table_name_used: US_NYC_CARD_OUTPUT_V5_REFERENCE,
            sql_where_used: US_NYC_PRECOMPUTED_CARD_SQL_WHERE,
            rows_found_count: n,
            first_row_if_any: firstRow,
            candidates_tried: candidatesToUse,
            attempts,
            final_selected_candidate: suffixed,
            matched_full_address: matchedAddr,
            precomputed_row_matched: true,
            bigquery_location: NYC_TRUTH_QUERY_LOCATION,
            full_sql_template: US_NYC_PRECOMPUTED_JOIN_QUERY,
            nyc_last_transaction_engine_table: US_NYC_LAST_TX_ENGINE_V3_REFERENCE,
            candidate_generator_version: cgv,
            ...addrMasterDebug,
          },
        };
      }
    }
  }

  const addressLineForPrompt =
    originalInput.trim() || norm.normalized_full_address.trim() || norm.normalized_building_address.trim();

  for (const address of candidatesToUse) {
    const trimmed = address.trim();
    if (!trimmed) continue;
    const { row, rowsReturned: n } = await queryPrecomputedNycCardJoinRow(client, trimmed);
    attempts.push({ candidate: trimmed, rows_returned: n });
    if (row) {
      firstRow = rowToJsonSafe(row);
      let needPrompt =
        !hasUnit &&
        computeNycNeedsUnitPrompt(row, {
          addressLine: addressLineForPrompt || trimmed,
          candidatesCount: candidatesToUse.length,
        });
      if (
        !needPrompt &&
        !hasUnit &&
        shouldApplyNycDebugKnownAddressUnitPromptOverride(originalInput, row, {
          addressLine: addressLineForPrompt || trimmed,
          candidatesCount: candidatesToUse.length,
        })
      ) {
        needPrompt = true;
      }

      const response = withUnitLookup(
        {
          success: true,
          message: null,
          ...mapPrecomputedJoinRowToUSNYCApiTruthResponse(
            row,
            needPrompt ? { pendingUnitPrompt: true } : undefined
          ),
        },
        needPrompt ? "not_requested" : hasUnit ? "not_found" : "not_requested",
        needPrompt ? null : submitted
      );
      const matchedAddr = firstRow.full_address != null ? String(firstRow.full_address) : null;
      return {
        response,
        debug: {
          original_input: originalInput,
          normalized_full_address: norm.normalized_full_address,
          normalized_building_address: norm.normalized_building_address,
          zip_from_input: norm.zip_from_input ?? null,
          table_name_used: US_NYC_CARD_OUTPUT_V5_REFERENCE,
          sql_where_used: US_NYC_PRECOMPUTED_CARD_SQL_WHERE,
          rows_found_count: n,
          first_row_if_any: firstRow,
          candidates_tried: candidatesToUse,
          attempts,
          final_selected_candidate: trimmed,
          matched_full_address: matchedAddr,
          precomputed_row_matched: true,
          bigquery_location: NYC_TRUTH_QUERY_LOCATION,
          full_sql_template: US_NYC_PRECOMPUTED_JOIN_QUERY,
          nyc_last_transaction_engine_table: US_NYC_LAST_TX_ENGINE_V3_REFERENCE,
          candidate_generator_version: cgv,
          ...addrMasterDebug,
        },
      };
    }
  }

  const addressLineForFallback =
    originalInput.trim() || norm.normalized_full_address.trim() || norm.normalized_building_address.trim();
  const streetFb = await queryNycStreetFallbackTruth(
    client,
    originalInput,
    candidatesToUse,
    hasUnit,
    submitted,
    addressLineForFallback
  );
  if (streetFb.response) {
    const matchedStr =
      streetFb.matchedRawRow?.full_address != null
        ? String(streetFb.matchedRawRow.full_address)
        : null;
    const firstRowFallback =
      streetFb.matchedRawRow != null ? rowToJsonSafe(streetFb.matchedRawRow) : null;
    const fbType = streetFb.response.nyc_fallback_type ?? null;
    const prompt = streetFb.response.nyc_pending_unit_prompt === true;
    return {
      response: streetFb.response,
      debug: {
        original_input: originalInput,
        normalized_full_address: norm.normalized_full_address,
        normalized_building_address: norm.normalized_building_address,
        zip_from_input: norm.zip_from_input ?? null,
        table_name_used: US_NYC_CARD_OUTPUT_V5_REFERENCE,
        sql_where_used: "street_fallback: UPPER(c.full_address) LIKE @pN (OR)",
        rows_found_count: streetFb.rowsReturned,
        first_row_if_any: firstRowFallback,
        candidates_tried: candidatesToUse,
        attempts,
        final_selected_candidate:
          matchedStr && fbType ? `FALLBACK:${fbType}:${matchedStr}` : matchedStr ? `FALLBACK:${matchedStr}` : null,
        matched_full_address: matchedStr,
        precomputed_row_matched: true,
        bigquery_location: NYC_TRUTH_QUERY_LOCATION,
        full_sql_template: US_NYC_STREET_FALLBACK_JOIN_QUERY,
        nyc_last_transaction_engine_table: US_NYC_LAST_TX_ENGINE_V3_REFERENCE,
        candidate_generator_version: cgv,
        nyc_street_fallback_used: true,
        nyc_street_fallback_patterns: streetFb.patternsUsed,
        searched_address: originalInput,
        fallback_used: true,
        fallback_type: fbType,
        fallback_score_reason: streetFb.response.nyc_fallback_score_reason ?? null,
        matched_building_type: streetFb.matchedRawRow?.building_type,
        matched_unit_count: streetFb.matchedRawRow?.unit_count,
        should_prompt_for_unit: prompt,
        nyc_pending_unit_prompt: prompt,
        unit_prompt_reason: prompt ? "apartment_or_lot_required" : null,
        same_street_pool: streetFb.sameStreetPool,
        ...addrMasterDebug,
      },
    };
  }

  logNycPrecomputedLookupMiss({
    searched_address: originalInput,
    generated_candidates: [...candidatesToUse],
    matched_full_address: null,
    final_selected_candidate: null,
    precomputed_row_matched: false,
    attempts,
  });

  return {
    response: withUnitLookup({ ...EMPTY_TRUTH }, hasUnit ? "not_found" : "not_requested", submitted),
    debug: {
      original_input: originalInput,
      normalized_full_address: norm.normalized_full_address,
      normalized_building_address: norm.normalized_building_address,
      zip_from_input: norm.zip_from_input ?? null,
      table_name_used: US_NYC_CARD_OUTPUT_V5_REFERENCE,
      sql_where_used: US_NYC_PRECOMPUTED_CARD_SQL_WHERE,
      rows_found_count: 0,
      first_row_if_any: null,
      candidates_tried: candidatesToUse,
      attempts,
      final_selected_candidate: null,
      matched_full_address: null,
      precomputed_row_matched: false,
      bigquery_location: NYC_TRUTH_QUERY_LOCATION,
      full_sql_template: US_NYC_PRECOMPUTED_JOIN_QUERY,
      nyc_last_transaction_engine_table: US_NYC_LAST_TX_ENGINE_V3_REFERENCE,
      candidate_generator_version: cgv,
      ...addrMasterDebug,
    },
  };
}

/** Builds NYC candidates then queries BigQuery (same as /api/us/property-value). */
export async function queryUSNYCApiTruthByAddress(address: string): Promise<USNYCApiTruthResponse> {
  const candidates = buildNycTruthLookupCandidates(address);
  if (candidates.length === 0) return { ...EMPTY_TRUTH };
  return queryUSNYCApiTruthWithCandidates(candidates, address);
}
