/**
 * Lookup rows in {@link getNycAppOutputTableReference} (default `us_nyc_property_ui_production_v10`) by normalized
 * address and optional unit. US only — not used for France.
 *
 * Match order (exact normalized equality only; no fuzzy search):
 * When `unit_or_lot` is provided:
 *   1) `lookup_address` + unit vs non-empty `normalized_unit_number`
 *   2) `lookup_address` + empty `normalized_unit_number` + `final_match_scope` in BUILDING / BUILDING_RECENT_SALES / EXACT_HOUSE
 * When no unit:
 *   1) same building/house branch
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
function collapseSpaces(s: string): string {
  return s.replace(/\s+/g, " ").trim();
}

function normalizeAddrForMatch(s: string): string {
  return collapseSpaces(s).toUpperCase();
}

/** Aligns with SQL normalize on `normalized_unit_number` (v10 has no `unit` column). */
function normalizeNycUnitForMatch(unitRaw: string): string {
  return normalizeAddrForMatch(unitRaw);
}

/** A few spellings so the user’s "4D" / "UNIT 4D" / "#4D" match the stored normalized unit. */
function listNycUnitNormalizationCandidates(unitRaw: string): string[] {
  const u = unitRaw.replace(/\s+/g, " ").trim();
  if (!u) return [];
  const variants = [u, u.replace(/^UNIT\s+/i, "").trim(), u.replace(/^#\s*/, "").trim()];
  return uniqPreserveOrder(variants.map((v) => normalizeNycUnitForMatch(v)).filter(Boolean));
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
  /** Normalized unit keys tried when a unit was submitted (empty if none). */
  unit_norm_keys_tried: string[];
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
  /** v5: EXACT_UNIT vs BUILDING row, or null when no row. */
  match_tier: "exact_unit" | "building" | null;
};

function sqlExactNormColumn(col: string): string {
  return `UPPER(TRIM(REGEXP_REPLACE(COALESCE(\`${col}\`, ''), r'\\s+', ' ')))`;
}

function sqlNormUnitExpr(): string {
  return `UPPER(TRIM(REGEXP_REPLACE(COALESCE(CAST(t.normalized_unit_number AS STRING), ''), r'\\s+', ' ')))`;
}

/**
 * User-entered line for NYC v5 matching: safe rewrites only (Queens/LIC, PRK→PARK, uppercase).
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

function buildExactUnitMatchQuery(table: string, column: string): string {
  const normAddrExpr = sqlExactNormColumn(column);
  return `
    SELECT * EXCEPT(cand_ord, unit_ord)
    FROM (
      SELECT t.*, cand_ord, unit_ord
      FROM \`${table}\` t
      CROSS JOIN UNNEST(@norms) AS norm WITH OFFSET cand_ord
      CROSS JOIN UNNEST(@unit_norms) AS unit_norm WITH OFFSET unit_ord
      WHERE ${normAddrExpr} = norm
        AND ${sqlNormUnitExpr()} = unit_norm
        AND t.normalized_unit_number IS NOT NULL
        AND TRIM(CAST(t.normalized_unit_number AS STRING)) != ''
      ORDER BY cand_ord ASC, unit_ord ASC
      LIMIT 1
    ) sub
  `;
}

function buildBuildingScopeMatchQuery(table: string, column: string): string {
  const normAddrExpr = sqlExactNormColumn(column);
  return `
    SELECT * EXCEPT(cand_ord)
    FROM (
      SELECT t.*, cand_ord
      FROM \`${table}\` t
      CROSS JOIN UNNEST(@norms) AS norm WITH OFFSET cand_ord
      WHERE ${normAddrExpr} = norm
        AND (t.normalized_unit_number IS NULL OR TRIM(CAST(t.normalized_unit_number AS STRING)) = '')
        AND UPPER(TRIM(COALESCE(t.final_match_scope, ''))) IN (
          'BUILDING',
          'BUILDING_RECENT_SALES',
          'EXACT_HOUSE'
        )
      ORDER BY cand_ord ASC
      LIMIT 1
    ) sub
  `;
}

function stripSyntheticOrdField(row: Record<string, unknown>): Record<string, unknown> {
  const { cand_ord: _o, unit_ord: _u, ...rest } = row as Record<string, unknown> & {
    cand_ord?: unknown;
    unit_ord?: unknown;
  };
  return rest;
}

/**
 * Returns first matching row (SELECT *) or null.
 * v5: EXACT_UNIT (address + unit) → BUILDING fallback → no row.
 */
export async function queryNycAppOutputFinalV5Row(
  client: BigQuery,
  addressRaw: string,
  unitOrLot: string | null
): Promise<{ row: Record<string, unknown> | null; debug: NycAppOutputQueryDebug }> {
  const table = getNycAppOutputTableReference();
  const resolution = getNycAppOutputTableResolutionForLog();
  console.log(
    "[NYC_BQ_V5_RESOLUTION]",
    JSON.stringify({
      ...resolution,
      sql_from_clause_target: `\`${table}\``,
      query_batch: "exact_unit_then_building",
    })
  );
  const lookupCol = US_NYC_APP_OUTPUT_ADDRESS_COL;
  const rawInput = addressRaw.trim();

  const lineForNorm = buildNycAppOutputLookupPipelineInput(addressRaw);
  const norm = buildNycTruthLookupNormalizationDebug(lineForNorm);
  let candidates = norm?.candidates?.length ? [...norm.candidates] : lineForNorm ? [lineForNorm] : [];
  candidates = uniqPreserveOrder(candidates);
  const normKeys = candidates.map((c) => normalizeAddrForMatch(c));
  const unitTrim = unitOrLot?.trim() ?? "";
  const unitNormKeys = unitTrim ? listNycUnitNormalizationCandidates(unitTrim) : [];

  const debug: NycAppOutputQueryDebug = {
    table,
    address_column: lookupCol,
    lookup_column: lookupCol,
    property_column: lookupCol,
    raw_input: rawInput,
    normalized_pipeline_input: lineForNorm,
    candidates_tried: candidates,
    norm_keys_tried: normKeys,
    unit_norm_keys_tried: unitNormKeys,
    matched_candidate: null,
    matched_norm_key: null,
    match_column: null,
    matched_stored_lookup_address: null,
    matched_stored_property_address: null,
    row_found: false,
    match_tier: null,
  };

  if (candidates.length === 0 || normKeys.length === 0) {
    return { row: null, debug };
  }

  const queryExactLookup = buildExactUnitMatchQuery(table, lookupCol);
  const queryBuildingLookup = buildBuildingScopeMatchQuery(table, lookupCol);

  const applyMatch = (
    row: Record<string, unknown> | undefined,
    tier: "exact_unit" | "building"
  ): { row: Record<string, unknown>; debug: NycAppOutputQueryDebug } | null => {
    if (!row) return null;
    const r = stripSyntheticOrdField(row);
    const la = String(r[lookupCol] ?? "").trim();
    const matchedNormKey = normalizeAddrForMatch(la);
    const idx = normKeys.findIndex((k) => k === matchedNormKey);
    debug.row_found = true;
    debug.match_column = "lookup_address";
    debug.match_tier = tier;
    debug.matched_norm_key = matchedNormKey;
    debug.matched_candidate = idx >= 0 ? candidates[idx]! : candidates[0]!;
    debug.matched_stored_lookup_address = la || null;
    debug.matched_stored_property_address = null;
    return { row: r, debug: { ...debug } };
  };

  const runWithUnit = async (hasUnit: boolean) => {
    if (hasUnit && unitNormKeys.length > 0) {
      const [lookupExactRows] = await client.query({
        query: queryExactLookup,
        params: { norms: normKeys, unit_norms: unitNormKeys },
        location: "US",
      });
      const hit = applyMatch(lookupExactRows[0] as Record<string, unknown> | undefined, "exact_unit");
      if (hit) return hit;
    }

    const [lookupBldRows] = await client.query({
      query: queryBuildingLookup,
      params: { norms: normKeys },
      location: "US",
    });
    const hitB = applyMatch(lookupBldRows[0] as Record<string, unknown> | undefined, "building");
    if (hitB) return hitB;

    return null;
  };

  const result = await runWithUnit(unitNormKeys.length > 0);
  if (result) {
    return { row: result.row, debug: result.debug };
  }

  return { row: null, debug };
}

/** @deprecated Use {@link queryNycAppOutputFinalV5Row}. */
export const queryNycAppOutputFinalV4Row = queryNycAppOutputFinalV5Row;
