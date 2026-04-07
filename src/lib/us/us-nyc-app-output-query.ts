/**
 * Lookup rows in `real_estate_us.us_nyc_app_output_final_v4` by normalized address candidates.
 * US only — not used for France.
 *
 * Match order (no fuzzy search):
 * 1) `lookup_address` — try every candidate with exact normalized equality.
 * 2) `property_address` — same candidates if step 1 found no row.
 */

import type { BigQuery } from "@google-cloud/bigquery";
import { buildNycTruthLookupNormalizationDebug } from "@/lib/us/us-nyc-address-normalize";
import { applyNyLongIslandCityToQueensInAddressLine, preserveQueensInAddressLineIfUserTypedQueens } from "@/lib/us/us-nyc-preserve-queens";
import { normalizeUSAddressLine } from "@/lib/us/us-address-normalize";
import { getNycAppOutputTableReference, US_NYC_APP_OUTPUT_ADDRESS_COL } from "@/lib/us/us-nyc-app-output-constants";
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
  candidates_tried: string[];
  /** First candidate that returned a row, if any. */
  matched_candidate: string | null;
  /** Which column matched when row_found. */
  match_column: "lookup_address" | "property_address" | null;
  row_found: boolean;
};

function sqlExactNormColumn(col: string): string {
  return `UPPER(TRIM(REGEXP_REPLACE(COALESCE(\`${col}\`, ''), r'\\s+', ' ')))`;
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
  const lookupCol = US_NYC_APP_OUTPUT_ADDRESS_COL;
  const propertyCol = NYC_APP_OUTPUT_V4_COL.property_address;

  const { line } = normalizeUSAddressLine(
    applyNyLongIslandCityToQueensInAddressLine(preserveQueensInAddressLineIfUserTypedQueens(addressRaw))
  );
  const lineForNorm = line ?? "";
  const norm = buildNycTruthLookupNormalizationDebug(lineForNorm);
  let candidates = norm?.candidates?.length ? [...norm.candidates] : lineForNorm ? [lineForNorm] : [];
  candidates = expandCandidatesWithUnit(candidates, unitOrLot);
  candidates = uniqPreserveOrder(candidates);

  const debug: NycAppOutputQueryDebug = {
    table,
    address_column: lookupCol,
    lookup_column: lookupCol,
    property_column: propertyCol,
    candidates_tried: candidates,
    matched_candidate: null,
    match_column: null,
    row_found: false,
  };

  if (candidates.length === 0) {
    return { row: null, debug };
  }

  const queryLookup = `
    SELECT *
    FROM \`${table}\`
    WHERE ${sqlExactNormColumn(lookupCol)} = @norm
    LIMIT 1
  `;
  const queryProperty = `
    SELECT *
    FROM \`${table}\`
    WHERE ${sqlExactNormColumn(propertyCol)} = @norm
    LIMIT 1
  `;

  for (const cand of candidates) {
    const normKey = normalizeAddrForMatch(cand);
    const [rows] = await client.query({
      query: queryLookup,
      params: { norm: normKey },
      location: "US",
    });
    const first = rows[0] as Record<string, unknown> | undefined;
    if (first) {
      debug.row_found = true;
      debug.matched_candidate = cand;
      debug.match_column = "lookup_address";
      return { row: first, debug };
    }
  }

  for (const cand of candidates) {
    const normKey = normalizeAddrForMatch(cand);
    const [rows] = await client.query({
      query: queryProperty,
      params: { norm: normKey },
      location: "US",
    });
    const first = rows[0] as Record<string, unknown> | undefined;
    if (first) {
      debug.row_found = true;
      debug.matched_candidate = cand;
      debug.match_column = "property_address";
      return { row: first, debug };
    }
  }

  return { row: null, debug };
}
