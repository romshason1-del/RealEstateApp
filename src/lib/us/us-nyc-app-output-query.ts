/**
 * Lookup rows in `real_estate_us.us_nyc_app_output_final_v4` by normalized address candidates.
 * US only — not used for France.
 */

import type { BigQuery } from "@google-cloud/bigquery";
import { buildNycTruthLookupNormalizationDebug } from "@/lib/us/us-nyc-address-normalize";
import { applyNyLongIslandCityToQueensInAddressLine, preserveQueensInAddressLineIfUserTypedQueens } from "@/lib/us/us-nyc-preserve-queens";
import { normalizeUSAddressLine } from "@/lib/us/us-address-normalize";
import { getNycAppOutputTableReference, US_NYC_APP_OUTPUT_ADDRESS_COL } from "@/lib/us/us-nyc-app-output-constants";

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
  address_column: string;
  candidates_tried: string[];
  /** First candidate that returned a row, if any. */
  matched_candidate: string | null;
  row_found: boolean;
};

/**
 * Returns first matching row (SELECT *) or null.
 */
export async function queryNycAppOutputFinalV4Row(
  client: BigQuery,
  addressRaw: string,
  unitOrLot: string | null
): Promise<{ row: Record<string, unknown> | null; debug: NycAppOutputQueryDebug }> {
  const table = getNycAppOutputTableReference();
  const col = US_NYC_APP_OUTPUT_ADDRESS_COL;

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
    address_column: col,
    candidates_tried: candidates,
    matched_candidate: null,
    row_found: false,
  };

  if (candidates.length === 0) {
    return { row: null, debug };
  }

  const query = `
    SELECT *
    FROM \`${table}\`
    WHERE UPPER(TRIM(REGEXP_REPLACE(COALESCE(\`${col}\`, ''), r'\\s+', ' '))) = @norm
    LIMIT 1
  `;

  for (const cand of candidates) {
    const normKey = normalizeAddrForMatch(cand);
    const [rows] = await client.query({
      query,
      params: { norm: normKey },
      location: "US",
    });
    const first = rows[0] as Record<string, unknown> | undefined;
    if (first) {
      debug.row_found = true;
      debug.matched_candidate = cand;
      return { row: first, debug };
    }
  }

  return { row: null, debug };
}
