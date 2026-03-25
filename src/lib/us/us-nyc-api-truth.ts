/**
 * NYC property rows from BigQuery source of truth only.
 * Table: streetiq-bigquery.streetiq_gold.us_nyc_api_truth
 */

import { getUSBigQueryClient } from "./bigquery-client";
import type { USNYCApiTruthResponse } from "./us-property-response-contract";
import { buildNycTruthLookupCandidates } from "./us-nyc-address-normalize";

/** Fully qualified table id (no backticks) — used in responses and logs. */
export const US_NYC_API_TRUTH_TABLE_REFERENCE = "streetiq-bigquery.streetiq_gold.us_nyc_api_truth";

const NYC_TRUTH_TABLE = `\`${US_NYC_API_TRUTH_TABLE_REFERENCE}\``;

export const US_NYC_API_TRUTH_SQL_WHERE = "pluto_address = @address OR sales_address = @address";

const NYC_TRUTH_QUERY_LOCATION = "EU";

const EMPTY_TRUTH: USNYCApiTruthResponse = {
  success: true,
  message: null,
  estimated_value: null,
  latest_sale_price: null,
  latest_sale_date: null,
  avg_street_price: null,
  avg_street_price_per_sqft: null,
  transaction_count: null,
  price_per_sqft: null,
  sales_address: null,
  pluto_address: null,
  street_name: null,
};

/** TEMPORARY: remove after production debugging. */
export type USNYCApiTruthQueryDebug = {
  original_input: string;
  normalized_full_address: string;
  normalized_building_address: string;
  table_name_used: string;
  sql_where_used: string;
  rows_found_count: number;
  first_row_if_any: Record<string, unknown> | null;
  candidates_tried: readonly string[];
  attempts: readonly { candidate: string; rows_returned: number }[];
  bigquery_location: string;
  full_sql_template: string;
};

function toNumberOrNull(v: unknown): number | null {
  if (v == null || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "bigint") {
    const x = Number(v);
    return Number.isFinite(x) ? x : null;
  }
  if (typeof v === "object" && v !== null && "toString" in v) {
    const x = Number(String(v));
    return Number.isFinite(x) ? x : null;
  }
  const x = Number(String(v));
  return Number.isFinite(x) ? x : null;
}

function toStringOrNull(v: unknown): string | null {
  if (v == null || v === "") return null;
  return String(v);
}

function toDateStringOrNull(v: unknown): string | null {
  if (v == null || v === "") return null;
  if (v instanceof Date && !Number.isNaN(v.getTime())) {
    return v.toISOString().slice(0, 10);
  }
  const s = String(v).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  return s.length > 0 ? s : null;
}

function mapTruthRow(row: Record<string, unknown>): Omit<USNYCApiTruthResponse, "success" | "message"> {
  return {
    estimated_value: toNumberOrNull(row.estimated_value),
    latest_sale_price: toNumberOrNull(row.latest_sale_price),
    latest_sale_date: toDateStringOrNull(row.latest_sale_date),
    avg_street_price: toNumberOrNull(row.avg_street_price),
    avg_street_price_per_sqft: toNumberOrNull(row.avg_street_price_per_sqft),
    transaction_count: toNumberOrNull(row.transaction_count),
    price_per_sqft: toNumberOrNull(row.price_per_sqft),
    sales_address: toStringOrNull(row.sales_address),
    pluto_address: toStringOrNull(row.pluto_address),
    street_name: toStringOrNull(row.street_name),
  };
}

function rowToJsonSafe(row: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(row)) {
    if (v instanceof Date) out[k] = v.toISOString();
    else if (typeof v === "bigint") out[k] = Number(v);
    else out[k] = v as unknown;
  }
  return out;
}

const SELECT_FIELDS = `
  estimated_value,
  latest_sale_price,
  latest_sale_date,
  avg_street_price,
  avg_street_price_per_sqft,
  transaction_count,
  price_per_sqft,
  sales_address,
  pluto_address,
  street_name
`.trim();

const MATCH_QUERY = `
  SELECT ${SELECT_FIELDS}
  FROM ${NYC_TRUTH_TABLE}
  WHERE ${US_NYC_API_TRUTH_SQL_WHERE}
  LIMIT 1
`.trim();

/**
 * Try each candidate in order (exact equality on pluto_address or sales_address).
 */
export async function queryUSNYCApiTruthWithCandidates(candidates: readonly string[]): Promise<USNYCApiTruthResponse> {
  const client = getUSBigQueryClient();
  for (const address of candidates) {
    const trimmed = address.trim();
    if (!trimmed) continue;
    const [rows] = await client.query({
      query: MATCH_QUERY,
      params: { address: trimmed },
      location: NYC_TRUTH_QUERY_LOCATION,
    });
    const row = (rows as Record<string, unknown>[] | null | undefined)?.[0];
    if (row) {
      return {
        success: true,
        message: null,
        ...mapTruthRow(row),
      };
    }
  }
  return { ...EMPTY_TRUTH };
}

/**
 * TEMPORARY: same as queryUSNYCApiTruthWithCandidates plus debug payload for /api/us/property-value.
 */
export async function queryUSNYCApiTruthWithCandidatesDebug(
  originalInput: string,
  norm: { normalized_full_address: string; normalized_building_address: string; candidates: readonly string[] }
): Promise<{ response: USNYCApiTruthResponse; debug: USNYCApiTruthQueryDebug }> {
  const client = getUSBigQueryClient();
  const attempts: { candidate: string; rows_returned: number }[] = [];
  let firstRow: Record<string, unknown> | null = null;

  for (const address of norm.candidates) {
    const trimmed = address.trim();
    if (!trimmed) continue;
    const [rows] = await client.query({
      query: MATCH_QUERY,
      params: { address: trimmed },
      location: NYC_TRUTH_QUERY_LOCATION,
    });
    const list = (rows as Record<string, unknown>[] | null | undefined) ?? [];
    const n = list.length;
    attempts.push({ candidate: trimmed, rows_returned: n });
    const row = list[0];
    if (row) {
      firstRow = rowToJsonSafe(row);
      return {
        response: {
          success: true,
          message: null,
          ...mapTruthRow(row),
        },
        debug: {
          original_input: originalInput,
          normalized_full_address: norm.normalized_full_address,
          normalized_building_address: norm.normalized_building_address,
          table_name_used: US_NYC_API_TRUTH_TABLE_REFERENCE,
          sql_where_used: US_NYC_API_TRUTH_SQL_WHERE,
          rows_found_count: n,
          first_row_if_any: firstRow,
          candidates_tried: norm.candidates,
          attempts,
          bigquery_location: NYC_TRUTH_QUERY_LOCATION,
          full_sql_template: MATCH_QUERY,
        },
      };
    }
  }

  return {
    response: { ...EMPTY_TRUTH },
    debug: {
      original_input: originalInput,
      normalized_full_address: norm.normalized_full_address,
      normalized_building_address: norm.normalized_building_address,
      table_name_used: US_NYC_API_TRUTH_TABLE_REFERENCE,
      sql_where_used: US_NYC_API_TRUTH_SQL_WHERE,
      rows_found_count: 0,
      first_row_if_any: null,
      candidates_tried: norm.candidates,
      attempts,
      bigquery_location: NYC_TRUTH_QUERY_LOCATION,
      full_sql_template: MATCH_QUERY,
    },
  };
}

/** Builds NYC candidates then queries BigQuery (same as /api/us/property-value). */
export async function queryUSNYCApiTruthByAddress(address: string): Promise<USNYCApiTruthResponse> {
  const candidates = buildNycTruthLookupCandidates(address);
  if (candidates.length === 0) return { ...EMPTY_TRUTH };
  return queryUSNYCApiTruthWithCandidates(candidates);
}
