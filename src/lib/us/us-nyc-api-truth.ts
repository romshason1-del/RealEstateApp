/**
 * NYC property rows from BigQuery source of truth only.
 * Table: streetiq-bigquery.streetiq_gold.us_nyc_api_truth
 */

import { coerceBigQueryDateToYyyyMmDd } from "./us-bq-date";
import { getUSBigQueryClient } from "./bigquery-client";
import type { USNYCApiTruthResponse } from "./us-property-response-contract";
import { normalizeStreetNameForDobQuery } from "./dob/dob-street-normalize";
import {
  buildNycTruthLookupCandidates,
  buildNycTruthLookupNormalizationDebug,
  type NycTruthNormalizationDebug,
} from "./us-nyc-address-normalize";
import {
  queryUSNycStreetPricingByStreetName,
  streetPricingQueryTemplate,
  US_NYC_STREET_PRICING_SQL_WHERE,
  US_NYC_STREET_PRICING_TABLE_REFERENCE,
} from "./us-nyc-street-pricing";

/** Fully qualified table id (no backticks) — used in responses and logs. */
export const US_NYC_API_TRUTH_TABLE_REFERENCE = "streetiq-bigquery.streetiq_gold.us_nyc_api_truth";

const NYC_TRUTH_TABLE = `\`${US_NYC_API_TRUTH_TABLE_REFERENCE}\``;

export const US_NYC_API_TRUTH_SQL_WHERE = "pluto_address = @address OR sales_address = @address";

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

/** Deterministic unit token for `pluto_address` / `sales_address` equality (same line as NYC truth rows). */
export function normalizeUnitOrLotForTruthLookup(raw: string): string {
  return collapseSpaces(raw.toUpperCase());
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
  table_name_used: string;
  sql_where_used: string;
  rows_found_count: number;
  first_row_if_any: Record<string, unknown> | null;
  candidates_tried: readonly string[];
  attempts: readonly { candidate: string; rows_returned: number }[];
  bigquery_location: string;
  full_sql_template: string;
  street_pricing_table_used?: string;
  street_pricing_sql_where?: string;
  street_pricing_street_name_used?: string;
  street_pricing_rows_found?: number;
  street_pricing_full_sql_template?: string;
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
  return coerceBigQueryDateToYyyyMmDd(v);
}

/** Leading NYC house number token (deterministic; street-only queries have no such token). */
function looksLikeHouseNumberToken(token: string): boolean {
  const t = token.trim();
  if (!t) return false;
  return /^\d+[A-Z]?$/i.test(t) || /^\d+-\d+$/.test(t) || /^\d+\/\d+$/.test(t);
}

/**
 * When the normalized line has no leading house number, the whole line is the street name for pricing.
 * When a house number is present, returns null (not a street-only query).
 */
function streetOnlyLineForPricing(
  norm: Pick<NycTruthNormalizationDebug, "normalized_full_address" | "normalized_building_address">
): string | null {
  const line = norm.normalized_building_address.trim() || norm.normalized_full_address.trim();
  if (!line) return null;
  const parts = line.split(/\s+/).filter(Boolean);
  if (parts.length === 0) return null;
  if (looksLikeHouseNumberToken(parts[0]!)) return null;
  return line;
}

/**
 * `us_nyc_street_pricing.street_name` keys omit ordinal suffixes (42ND → 42). This runs only for
 * street-pricing BigQuery lookups in this module (not ACRIS/DOB/UI).
 */
function stripOrdinalStreetNumberSuffixes(upper: string): string {
  return upper.replace(/\b(\d+)(ST|ND|RD|TH)\b/gi, "$1");
}

/**
 * DOB-style suffix abbreviations, then ordinal stripping for `us_nyc_street_pricing` equality match.
 */
function normalizeStreetNameForStreetPricingLookup(raw: string): string {
  const dob = normalizeStreetNameForDobQuery(raw);
  return stripOrdinalStreetNumberSuffixes(dob);
}

/**
 * After no `us_nyc_api_truth` match: if the request is street-only, query `us_nyc_street_pricing`
 * using {@link normalizeStreetNameForStreetPricingLookup}.
 */
async function streetOnlyTruthResponseFromNorm(
  norm: Pick<NycTruthNormalizationDebug, "normalized_full_address" | "normalized_building_address">
): Promise<USNYCApiTruthResponse | null> {
  const rawStreetLine = streetOnlyLineForPricing(norm);
  if (rawStreetLine == null) return null;

  const normalizedStreetName = normalizeStreetNameForStreetPricingLookup(rawStreetLine);
  const sp = await queryUSNycStreetPricingByStreetName(normalizedStreetName);
  if (!sp) {
    return {
      ...EMPTY_TRUTH,
      street_name: normalizedStreetName,
      unit_lookup_status: "not_requested",
      unit_or_lot_submitted: null,
    };
  }
  return {
    ...EMPTY_TRUTH,
    street_name: normalizedStreetName,
    avg_street_price: sp.avg_street_price,
    avg_street_price_per_sqft: sp.avg_street_price_per_sqft,
    transaction_count: sp.transaction_count,
    unit_lookup_status: "not_requested",
    unit_or_lot_submitted: null,
  };
}

async function applyStreetPricingTable(base: USNYCApiTruthResponse): Promise<{
  merged: USNYCApiTruthResponse;
  streetNameTried: string | null;
  streetPricingRowFound: boolean;
}> {
  if (!base.success) {
    return { merged: base, streetNameTried: null, streetPricingRowFound: false };
  }
  const sn = base.street_name?.trim() ?? null;
  if (!sn) {
    return { merged: base, streetNameTried: null, streetPricingRowFound: false };
  }
  const lookupKey = normalizeStreetNameForStreetPricingLookup(sn);
  const sp = await queryUSNycStreetPricingByStreetName(lookupKey);
  if (!sp) {
    return { merged: base, streetNameTried: lookupKey, streetPricingRowFound: false };
  }
  return {
    merged: {
      ...base,
      avg_street_price: sp.avg_street_price,
      avg_street_price_per_sqft: sp.avg_street_price_per_sqft,
    },
    streetNameTried: lookupKey,
    streetPricingRowFound: true,
  };
}

function mapTruthRow(row: Record<string, unknown>): Omit<USNYCApiTruthResponse, "success" | "message"> {
  const totalUnitsRaw =
    row.total_units ?? row.TOTAL_UNITS ?? row.latest_sale_total_units ?? row.LATEST_SALE_TOTAL_UNITS;
  return {
    has_truth_property_row: true,
    estimated_value: toNumberOrNull(row.estimated_value),
    latest_sale_price: toNumberOrNull(row.latest_sale_price),
    latest_sale_date: toDateStringOrNull(row.latest_sale_date),
    latest_sale_total_units: toNumberOrNull(totalUnitsRaw),
    avg_street_price: toNumberOrNull(row.avg_street_price),
    avg_street_price_per_sqft: toNumberOrNull(row.avg_street_price_per_sqft),
    transaction_count: toNumberOrNull(row.transaction_count),
    price_per_sqft: toNumberOrNull(row.price_per_sqft),
    sales_address: toStringOrNull(row.sales_address),
    pluto_address: toStringOrNull(row.pluto_address),
    street_name: toStringOrNull(row.street_name),
    unit_lookup_status: "not_requested",
    unit_or_lot_submitted: null,
  };
}

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

const SELECT_FIELDS = `
  estimated_value,
  latest_sale_price,
  latest_sale_date,
  total_units,
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

async function queryFirstTruthRowForAddress(
  client: ReturnType<typeof getUSBigQueryClient>,
  address: string
): Promise<{ row: Record<string, unknown> | null; rowsReturned: number }> {
  const trimmed = address.trim();
  if (!trimmed) return { row: null, rowsReturned: 0 };
  const [rows] = await client.query({
    query: MATCH_QUERY,
    params: { address: trimmed },
    location: NYC_TRUTH_QUERY_LOCATION,
  });
  const list = (rows as Record<string, unknown>[] | null | undefined) ?? [];
  return { row: list[0] ?? null, rowsReturned: list.length };
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
 * Try each candidate in order (exact equality on pluto_address or sales_address).
 * When `unitOrLot` is set, tries `"{candidate}, {normalized unit}"` first, then building-level candidates.
 */
export async function queryUSNYCApiTruthWithCandidates(
  candidates: readonly string[],
  rawInput?: string,
  options?: { unitOrLot?: string | null }
): Promise<USNYCApiTruthResponse> {
  const client = getUSBigQueryClient();
  const { hasUnit, submitted } = parseUnitOrLotOptions(options);

  if (hasUnit && submitted) {
    for (const address of candidates) {
      const trimmed = address.trim();
      if (!trimmed) continue;
      const suffixed = `${trimmed}, ${submitted}`;
      const { row } = await queryFirstTruthRowForAddress(client, suffixed);
      if (row) {
        const { merged } = await applyStreetPricingTable(
          withUnitLookup(
            {
              success: true,
              message: null,
              ...mapTruthRow(row),
            },
            "matched",
            submitted
          )
        );
        return merged;
      }
    }
  }

  for (const address of candidates) {
    const trimmed = address.trim();
    if (!trimmed) continue;
    const { row } = await queryFirstTruthRowForAddress(client, trimmed);
    if (row) {
      const { merged } = await applyStreetPricingTable(
        withUnitLookup(
          {
            success: true,
            message: null,
            ...mapTruthRow(row),
          },
          hasUnit ? "not_found" : "not_requested",
          submitted
        )
      );
      return merged;
    }
  }
  if (rawInput) {
    const norm = buildNycTruthLookupNormalizationDebug(rawInput);
    if (norm) {
      const streetOnly = await streetOnlyTruthResponseFromNorm(norm);
      if (streetOnly) return withUnitLookup(streetOnly, hasUnit ? "not_found" : "not_requested", submitted);
    }
  }
  return withUnitLookup({ ...EMPTY_TRUTH }, hasUnit ? "not_found" : "not_requested", submitted);
}

/**
 * TEMPORARY: same as queryUSNYCApiTruthWithCandidates plus debug payload for /api/us/property-value.
 */
export async function queryUSNYCApiTruthWithCandidatesDebug(
  originalInput: string,
  norm: { normalized_full_address: string; normalized_building_address: string; candidates: readonly string[] },
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
      const { row, rowsReturned: n } = await queryFirstTruthRowForAddress(client, suffixed);
      attempts.push({ candidate: suffixed, rows_returned: n });
      if (row) {
        firstRow = rowToJsonSafe(row);
        const truthResponse: USNYCApiTruthResponse = withUnitLookup(
          {
            success: true,
            message: null,
            ...mapTruthRow(row),
          },
          "matched",
          submitted
        );
        const { merged: response, streetNameTried, streetPricingRowFound } = await applyStreetPricingTable(truthResponse);

        return {
          response,
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
            ...(streetNameTried
              ? {
                  street_pricing_table_used: US_NYC_STREET_PRICING_TABLE_REFERENCE,
                  street_pricing_sql_where: US_NYC_STREET_PRICING_SQL_WHERE,
                  street_pricing_street_name_used: streetNameTried,
                  street_pricing_rows_found: streetPricingRowFound ? 1 : 0,
                  street_pricing_full_sql_template: streetPricingQueryTemplate(),
                }
              : {}),
          },
        };
      }
    }
  }

  for (const address of norm.candidates) {
    const trimmed = address.trim();
    if (!trimmed) continue;
    const { row, rowsReturned: n } = await queryFirstTruthRowForAddress(client, trimmed);
    attempts.push({ candidate: trimmed, rows_returned: n });
    if (row) {
      firstRow = rowToJsonSafe(row);
      const truthResponse: USNYCApiTruthResponse = withUnitLookup(
        {
          success: true,
          message: null,
          ...mapTruthRow(row),
        },
        hasUnit ? "not_found" : "not_requested",
        submitted
      );
      const { merged: response, streetNameTried, streetPricingRowFound } = await applyStreetPricingTable(truthResponse);

      return {
        response,
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
          ...(streetNameTried
            ? {
                street_pricing_table_used: US_NYC_STREET_PRICING_TABLE_REFERENCE,
                street_pricing_sql_where: US_NYC_STREET_PRICING_SQL_WHERE,
                street_pricing_street_name_used: streetNameTried,
                street_pricing_rows_found: streetPricingRowFound ? 1 : 0,
                street_pricing_full_sql_template: streetPricingQueryTemplate(),
              }
            : {}),
        },
      };
    }
  }

  const streetOnlyResponse = await streetOnlyTruthResponseFromNorm(norm);
  if (streetOnlyResponse) {
    const response = withUnitLookup(streetOnlyResponse, hasUnit ? "not_found" : "not_requested", submitted);
    const pricingFound = response.avg_street_price != null || response.avg_street_price_per_sqft != null;
    return {
      response,
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
        street_pricing_table_used: US_NYC_STREET_PRICING_TABLE_REFERENCE,
        street_pricing_sql_where: US_NYC_STREET_PRICING_SQL_WHERE,
        street_pricing_street_name_used: response.street_name ?? undefined,
        street_pricing_rows_found: pricingFound ? 1 : 0,
        street_pricing_full_sql_template: streetPricingQueryTemplate(),
      },
    };
  }

  return {
    response: withUnitLookup({ ...EMPTY_TRUTH }, hasUnit ? "not_found" : "not_requested", submitted),
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
  return queryUSNYCApiTruthWithCandidates(candidates, address);
}
