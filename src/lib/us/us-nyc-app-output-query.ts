/**
 * Lookup rows in `us_nyc_app_output_final_v4` (see {@link getNycAppOutputTableReference}) by normalized address candidates.
 * US only — not used for France.
 *
 * Match order (exact normalized equality only; no fuzzy search):
 * 1) `lookup_address` — all candidates in one batched query (priority = candidate order).
 * 2) `property_address` — same, only if step 1 found no row.
 */

import type { BigQuery } from "@google-cloud/bigquery";
import { buildNycTruthLookupNormalizationDebug } from "@/lib/us/us-nyc-address-normalize";
import { applyNyLongIslandCityToQueensInAddressLine, preserveQueensInAddressLineIfUserTypedQueens } from "@/lib/us/us-nyc-preserve-queens";
import { normalizeUSAddressLine } from "@/lib/us/us-address-normalize";
import {
  getNycAppOutputTableReference,
  getNycAppOutputTableResolutionForLog,
  US_NYC_APP_OUTPUT_ADDRESS_COL,
} from "@/lib/us/us-nyc-app-output-constants";
import { NYC_APP_OUTPUT_V4_COL } from "@/lib/us/us-nyc-app-output-schema";

function collapseSpaces(s: string): string {
  return s.replace(/\s+/g, " ").trim();
}

function normalizeAddrForMatch(s: string): string {
  return collapseSpaces(s).toUpperCase();
}

/** Same spellings as `listNycCardUnitSuffixCandidates` in us-nyc-api-truth (kept local to avoid a heavy import graph). */
function listNycCardUnitSuffixCandidates(submitted: string): string[] {
  const u = submitted.replace(/\s+/g, " ").trim();
  if (!u) return [];
  const out: string[] = [u];
  if (!/^UNIT\s/i.test(u)) out.push(`UNIT ${u}`);
  if (!/^#/u.test(u)) out.push(`#${u}`);
  if (!/^UNIT\s/i.test(u) && !/^#/u.test(u)) out.push(`UNIT #${u}`);
  return [...new Set(out)].filter((x) => x.length > 0);
}

function uniqPreserveOrder(cands: string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const c of cands) {
    const t = c.trim();
    if (!t || seen.has(t)) continue;
    seen.add(t);
    out.push(t);
  }
  return out;
}

/**
 * When a unit was submitted, try the same building candidates with common unit suffix spellings.
 */
function expandCandidatesWithUnit(baseCandidates: string[], unitRaw: string | null): string[] {
  if (!unitRaw?.trim()) return baseCandidates;
  const unit = unitRaw.replace(/\s+/g, " ").trim();
  const suffixes = listNycCardUnitSuffixCandidates(unit);
  const extra: string[] = [];
  for (const b of baseCandidates) {
    for (const suf of suffixes) {
      extra.push(`${b}, ${suf}`);
      extra.push(`${b} ${suf}`);
    }
  }
  return uniqPreserveOrder([...baseCandidates, ...extra]);
}

export type NycAppOutputQueryDebug = {
  table: string;
  /** @deprecated use lookup_column — kept for older debug readers */
  address_column: string;
  lookup_column: string;
  property_column: string;
  /** Exact user-entered address string (trimmed) before NYC normalization. */
  raw_input: string;
  /** Line fed into {@link buildNycTruthLookupNormalizationDebug} (after LIC/Queens + PRK/uppercase). */
  normalized_pipeline_input: string;
  candidates_tried: string[];
  /** Normalized equality keys (uppercase, collapsed spaces) in same order as candidates. */
  norm_keys_tried: string[];
  /** First candidate that returned a row, if any. */
  matched_candidate: string | null;
  /** Normalized key that matched the stored column value. */
  matched_norm_key: string | null;
  /** Which column matched when row_found. */
  match_column: "lookup_address" | "property_address" | null;
  /** Stored DB value for the matched column (trimmed string). */
  matched_stored_lookup_address: string | null;
  matched_stored_property_address: string | null;
  row_found: boolean;
};

function sqlExactNormColumn(col: string): string {
  return `UPPER(TRIM(REGEXP_REPLACE(COALESCE(\`${col}\`, ''), r'\\s+', ' ')))`;
}

/**
 * User-entered line for NYC v4 matching: safe rewrites only (Queens/LIC, PRK→PARK, uppercase).
 * All street-type / ordinal variants are generated inside {@link buildNycTruthLookupNormalizationDebug}.
 */
export function buildNycAppOutputLookupPipelineInput(addressRaw: string): string {
  const trimmed = addressRaw.trim();
  if (!trimmed) return "";
  const preserved = preserveQueensInAddressLineIfUserTypedQueens(trimmed);
  const lic = applyNyLongIslandCityToQueensInAddressLine(preserved);
  const { line } = normalizeUSAddressLine(lic);
  return line ?? "";
}

function buildFirstMatchByLookupOrPropertyQuery(table: string, column: string): string {
  const normExpr = sqlExactNormColumn(column);
  return `
    SELECT * EXCEPT(cand_ord)
    FROM (
      SELECT t.*, cand_ord
      FROM \`${table}\` t
      CROSS JOIN UNNEST(@norms) AS norm WITH OFFSET cand_ord
      WHERE ${normExpr} = norm
      ORDER BY cand_ord ASC
      LIMIT 1
    ) sub
  `;
}

function stripSyntheticOrdField(row: Record<string, unknown>): Record<string, unknown> {
  const { cand_ord: _o, ...rest } = row as Record<string, unknown> & { cand_ord?: unknown };
  return rest;
}

/**
 * Returns first matching row (SELECT *) or null.
 */
export async function queryNycAppOutputFinalV4Row(
  client: BigQuery,
  addressRaw: string,
  unitOrLot: string | null
): Promise<{ row: Record<string, unknown> | null; debug: NycAppOutputQueryDebug }> {
  const table = getNycAppOutputTableReference();
  const resolution = getNycAppOutputTableResolutionForLog();
  console.log(
    "[NYC_BQ_V4_RESOLUTION]",
    JSON.stringify({
      ...resolution,
      sql_from_clause_target: `\`${table}\``,
      query_batch: "lookup_address_then_property_address",
    })
  );
  const lookupCol = US_NYC_APP_OUTPUT_ADDRESS_COL;
  const propertyCol = NYC_APP_OUTPUT_V4_COL.property_address;
  const rawInput = addressRaw.trim();

  const lineForNorm = buildNycAppOutputLookupPipelineInput(addressRaw);
  const norm = buildNycTruthLookupNormalizationDebug(lineForNorm);
  let candidates = norm?.candidates?.length ? [...norm.candidates] : lineForNorm ? [lineForNorm] : [];
  candidates = expandCandidatesWithUnit(candidates, unitOrLot);
  candidates = uniqPreserveOrder(candidates);
  const normKeys = candidates.map((c) => normalizeAddrForMatch(c));

  const debug: NycAppOutputQueryDebug = {
    table,
    address_column: lookupCol,
    lookup_column: lookupCol,
    property_column: propertyCol,
    raw_input: rawInput,
    normalized_pipeline_input: lineForNorm,
    candidates_tried: candidates,
    norm_keys_tried: normKeys,
    matched_candidate: null,
    matched_norm_key: null,
    match_column: null,
    matched_stored_lookup_address: null,
    matched_stored_property_address: null,
    row_found: false,
  };

  if (candidates.length === 0 || normKeys.length === 0) {
    return { row: null, debug };
  }

  const queryLookupBatch = buildFirstMatchByLookupOrPropertyQuery(table, lookupCol);
  const queryPropertyBatch = buildFirstMatchByLookupOrPropertyQuery(table, propertyCol);

  const [lookupRows] = await client.query({
    query: queryLookupBatch,
    params: { norms: normKeys },
    location: "US",
  });
  const lookupFirst = lookupRows[0] as Record<string, unknown> | undefined;
  if (lookupFirst) {
    const row = stripSyntheticOrdField(lookupFirst);
    const la = String(row[lookupCol] ?? "").trim();
    const pa = String(row[propertyCol] ?? "").trim();
    const matchedNormKey = normalizeAddrForMatch(la);
    const idx = normKeys.findIndex((k) => k === matchedNormKey);
    debug.row_found = true;
    debug.match_column = "lookup_address";
    debug.matched_norm_key = matchedNormKey;
    debug.matched_candidate = idx >= 0 ? candidates[idx]! : candidates[0]!;
    debug.matched_stored_lookup_address = la || null;
    debug.matched_stored_property_address = pa || null;
    return { row, debug };
  }

  const [propertyRows] = await client.query({
    query: queryPropertyBatch,
    params: { norms: normKeys },
    location: "US",
  });
  const propFirst = propertyRows[0] as Record<string, unknown> | undefined;
  if (propFirst) {
    const row = stripSyntheticOrdField(propFirst);
    const la = String(row[lookupCol] ?? "").trim();
    const pa = String(row[propertyCol] ?? "").trim();
    const matchedNormKey = normalizeAddrForMatch(pa);
    const idx = normKeys.findIndex((k) => k === matchedNormKey);
    debug.row_found = true;
    debug.match_column = "property_address";
    debug.matched_norm_key = matchedNormKey;
    debug.matched_candidate = idx >= 0 ? candidates[idx]! : candidates[0]!;
    debug.matched_stored_lookup_address = la || null;
    debug.matched_stored_property_address = pa || null;
    return { row, debug };
  }

  return { row: null, debug };
}
