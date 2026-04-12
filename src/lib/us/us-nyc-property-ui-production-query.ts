/**
 * Single-row lookup for `us_nyc_property_ui_production_v10` by normalized `lookup_address`
 * and optional `normalized_unit_number` (user `unit_or_lot`).
 *
 * Uses BigQuery columns: `lookup_address`, `normalized_unit_number`, `final_match_scope` only
 * (no `match_scope`, no `unit` column on this table).
 *
 * US only — not used for France.
 */

import type { BigQuery } from "@google-cloud/bigquery";
import { buildNycTruthLookupNormalizationDebug } from "@/lib/us/us-nyc-address-normalize";
import {
  getNycAppOutputTableReference,
  getNycAppOutputTableResolutionForLog,
  US_NYC_APP_OUTPUT_ADDRESS_COL,
} from "@/lib/us/us-nyc-app-output-constants";
import {
  buildNycAppOutputLookupPipelineInput,
  type NycAppOutputQueryDebug,
} from "@/lib/us/us-nyc-app-output-query";
export type { NycAppOutputQueryDebug };

function collapseSpaces(s: string): string {
  return s.replace(/\s+/g, " ").trim();
}

function normalizeAddrForMatch(s: string): string {
  return collapseSpaces(s).toUpperCase();
}

function normalizeNycUnitForMatch(unitRaw: string): string {
  return normalizeAddrForMatch(unitRaw);
}

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

function sqlExactNormColumn(col: string): string {
  return `UPPER(TRIM(REGEXP_REPLACE(COALESCE(\`${col}\`, ''), r'\\s+', ' ')))`;
}

/** Normalized unit key from `normalized_unit_number` only (no `unit` column in v10). */
function sqlNormUnitExpr(): string {
  return `UPPER(TRIM(REGEXP_REPLACE(COALESCE(CAST(t.normalized_unit_number AS STRING), ''), r'\\s+', ' ')))`;
}

/** Address + non-empty unit on row (EXACT_UNIT_*, etc.). */
function buildLookupWithUnitMatchQuery(table: string, column: string): string {
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

/**
 * Building / house / aggregate rows: empty `normalized_unit_number` and allowed `final_match_scope`.
 */
function buildBuildingOrHouseMatchQuery(table: string, column: string): string {
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

/**
 * User did not pass `unit_or_lot` but the table only has unit-level rows at this `lookup_address`.
 * Pick a deterministic row so the API can set `requires_unit` / apartment prompt instead of "no record".
 */
function buildAddressWithAnyUnitRowQuery(table: string, column: string): string {
  const normAddrExpr = sqlExactNormColumn(column);
  return `
    SELECT * EXCEPT(cand_ord)
    FROM (
      SELECT t.*, cand_ord
      FROM \`${table}\` t
      CROSS JOIN UNNEST(@norms) AS norm WITH OFFSET cand_ord
      WHERE ${normAddrExpr} = norm
        AND t.normalized_unit_number IS NOT NULL
        AND TRIM(CAST(t.normalized_unit_number AS STRING)) != ''
      ORDER BY cand_ord ASC, TRIM(CAST(t.normalized_unit_number AS STRING)) ASC
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

const LOOKUP_COL = US_NYC_APP_OUTPUT_ADDRESS_COL;

export async function queryNycPropertyUiProductionV10Row(
  client: BigQuery,
  addressRaw: string,
  unitOrLot: string | null
): Promise<{ row: Record<string, unknown> | null; debug: NycAppOutputQueryDebug }> {
  const table = getNycAppOutputTableReference();
  const resolution = getNycAppOutputTableResolutionForLog();
  console.log(
    "[NYC_BQ_PROPERTY_UI_V10]",
    JSON.stringify({
      ...resolution,
      sql_from_clause_target: `\`${table}\``,
      query_batch: "with_unit_then_building_then_unit_prompt_placeholder",
    })
  );

  const rawInput = addressRaw.trim();
  const lineForNorm = buildNycAppOutputLookupPipelineInput(addressRaw);
  const norm = buildNycTruthLookupNormalizationDebug(lineForNorm);
  let candidates = norm?.candidates?.length ? [...norm.candidates] : lineForNorm ? [lineForNorm] : [];
  candidates = uniqPreserveOrder(candidates);
  const normKeys = candidates.map((c) => normalizeAddrForMatch(c));
  const unitTrim = unitOrLot?.trim() ?? "";
  const unitNormKeys = unitTrim ? listNycUnitNormalizationCandidates(unitTrim) : [];

  const sqlAttempts: NycAppOutputQueryDebug["sql_attempts"] = [];
  const debug: NycAppOutputQueryDebug = {
    table,
    address_column: LOOKUP_COL,
    lookup_column: LOOKUP_COL,
    property_column: LOOKUP_COL,
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
    sql_attempts: sqlAttempts,
    no_match_reason: null,
    unit_or_lot_param: unitOrLot?.trim() ? unitOrLot.trim() : null,
  };

  if (candidates.length === 0 || normKeys.length === 0) {
    debug.no_match_reason = "no_lookup_candidates_after_normalization";
    return { row: null, debug };
  }

  const queryWithUnit = buildLookupWithUnitMatchQuery(table, LOOKUP_COL);
  const queryBuilding = buildBuildingOrHouseMatchQuery(table, LOOKUP_COL);
  const queryAnyUnitAtAddress = buildAddressWithAnyUnitRowQuery(table, LOOKUP_COL);

  const applyMatch = (
    row: Record<string, unknown> | undefined,
    tier: "exact_unit" | "building" | "needs_unit_prompt"
  ): { row: Record<string, unknown>; debug: NycAppOutputQueryDebug } | null => {
    if (!row) return null;
    const r = stripSyntheticOrdField(row);
    const la = String(r[LOOKUP_COL] ?? "").trim();
    const matchedNormKey = normalizeAddrForMatch(la);
    const idx = normKeys.findIndex((k) => k === matchedNormKey);
    debug.row_found = true;
    debug.match_column = "lookup_address";
    debug.match_tier = tier;
    debug.matched_norm_key = matchedNormKey;
    debug.matched_candidate = idx >= 0 ? candidates[idx]! : candidates[0]!;
    debug.matched_stored_lookup_address = la || null;
    debug.matched_stored_property_address = null;
    debug.no_match_reason = null;
    return { row: r, debug: { ...debug, sql_attempts: [...sqlAttempts] } };
  };

  if (unitNormKeys.length > 0) {
    sqlAttempts.push("with_user_unit");
    const [withUnitRows] = await client.query({
      query: queryWithUnit,
      params: { norms: normKeys, unit_norms: unitNormKeys },
      location: "US",
    });
    const hit = applyMatch(withUnitRows[0] as Record<string, unknown> | undefined, "exact_unit");
    if (hit) return { row: hit.row, debug: hit.debug };
  }

  sqlAttempts.push("building_or_house");
  const [buildingRows] = await client.query({
    query: queryBuilding,
    params: { norms: normKeys },
    location: "US",
  });
  const hitB = applyMatch(buildingRows[0] as Record<string, unknown> | undefined, "building");
  if (hitB) return { row: hitB.row, debug: hitB.debug };

  if (unitNormKeys.length === 0) {
    sqlAttempts.push("address_with_unit_prompt_placeholder");
    const [anyUnitRows] = await client.query({
      query: queryAnyUnitAtAddress,
      params: { norms: normKeys },
      location: "US",
    });
    const hitP = applyMatch(anyUnitRows[0] as Record<string, unknown> | undefined, "needs_unit_prompt");
    if (hitP) return { row: hitP.row, debug: hitP.debug };
  }

  debug.no_match_reason =
    unitNormKeys.length > 0
      ? "no_row_for_submitted_unit_and_no_building_row"
      : "no_building_row_and_no_unitized_row_at_address";
  return { row: null, debug };
}
