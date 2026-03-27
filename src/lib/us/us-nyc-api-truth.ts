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
  queryPrecomputedNycCardJoinRow,
  US_NYC_CARD_OUTPUT_V5_REFERENCE,
  US_NYC_LAST_TX_ENGINE_V3_REFERENCE,
  US_NYC_PRECOMPUTED_CARD_SQL_WHERE,
  US_NYC_PRECOMPUTED_JOIN_QUERY,
} from "./us-nyc-precomputed-card";
import { buildNycTruthLookupCandidates } from "./us-nyc-address-normalize";

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
  const { hasUnit, submitted } = parseUnitOrLotOptions(options);
  const attempts: { candidate: string; rows_returned: number }[] = [];

  if (hasUnit && submitted) {
    for (const address of candidates) {
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

  const addressLineForPrompt = (rawInput ?? "").trim() || (candidates[0] ?? "").trim();

  for (const address of candidates) {
    const trimmed = address.trim();
    if (!trimmed) continue;
    const { row, rowsReturned } = await queryPrecomputedNycCardJoinRow(client, trimmed);
    attempts.push({ candidate: trimmed, rows_returned: rowsReturned });
    if (row) {
      const needPrompt =
        !hasUnit &&
        computeNycNeedsUnitPrompt(row, {
          addressLine: addressLineForPrompt || trimmed,
          candidatesCount: candidates.length,
        });

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

  logNycPrecomputedLookupMiss({
    searched_address: rawInput ?? "",
    generated_candidates: [...candidates],
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
  },
  options?: { unitOrLot?: string | null }
): Promise<{ response: USNYCApiTruthResponse; debug: USNYCApiTruthQueryDebug }> {
  const client = getUSBigQueryClient();
  const attempts: { candidate: string; rows_returned: number }[] = [];
  let firstRow: Record<string, unknown> | null = null;
  const { hasUnit, submitted } = parseUnitOrLotOptions(options);

  if (hasUnit && submitted) {
    for (const address of norm.candidates) {
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
            candidates_tried: norm.candidates,
            attempts,
            final_selected_candidate: suffixed,
            matched_full_address: matchedAddr,
            precomputed_row_matched: true,
            bigquery_location: NYC_TRUTH_QUERY_LOCATION,
            full_sql_template: US_NYC_PRECOMPUTED_JOIN_QUERY,
            nyc_last_transaction_engine_table: US_NYC_LAST_TX_ENGINE_V3_REFERENCE,
          },
        };
      }
    }
  }

  const addressLineForPrompt =
    originalInput.trim() || norm.normalized_full_address.trim() || norm.normalized_building_address.trim();

  for (const address of norm.candidates) {
    const trimmed = address.trim();
    if (!trimmed) continue;
    const { row, rowsReturned: n } = await queryPrecomputedNycCardJoinRow(client, trimmed);
    attempts.push({ candidate: trimmed, rows_returned: n });
    if (row) {
      firstRow = rowToJsonSafe(row);
      const needPrompt =
        !hasUnit &&
        computeNycNeedsUnitPrompt(row, {
          addressLine: addressLineForPrompt || trimmed,
          candidatesCount: norm.candidates.length,
        });

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
          candidates_tried: norm.candidates,
          attempts,
          final_selected_candidate: trimmed,
          matched_full_address: matchedAddr,
          precomputed_row_matched: true,
          bigquery_location: NYC_TRUTH_QUERY_LOCATION,
          full_sql_template: US_NYC_PRECOMPUTED_JOIN_QUERY,
          nyc_last_transaction_engine_table: US_NYC_LAST_TX_ENGINE_V3_REFERENCE,
        },
      };
    }
  }

  logNycPrecomputedLookupMiss({
    searched_address: originalInput,
    generated_candidates: [...norm.candidates],
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
      candidates_tried: norm.candidates,
      attempts,
      final_selected_candidate: null,
      matched_full_address: null,
      precomputed_row_matched: false,
      bigquery_location: NYC_TRUTH_QUERY_LOCATION,
      full_sql_template: US_NYC_PRECOMPUTED_JOIN_QUERY,
      nyc_last_transaction_engine_table: US_NYC_LAST_TX_ENGINE_V3_REFERENCE,
    },
  };
}

/** Builds NYC candidates then queries BigQuery (same as /api/us/property-value). */
export async function queryUSNYCApiTruthByAddress(address: string): Promise<USNYCApiTruthResponse> {
  const candidates = buildNycTruthLookupCandidates(address);
  if (candidates.length === 0) return { ...EMPTY_TRUTH };
  return queryUSNYCApiTruthWithCandidates(candidates, address);
}
