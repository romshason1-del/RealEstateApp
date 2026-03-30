/**
 * NYC unified address master (`us_nyc_address_master_v1`) — US / BigQuery only.
 * Normalization must stay in lockstep with `nyc_normalize_address_master_v1` in
 * scripts/us/nyc/sql/build-us-nyc-address-master-v1.sql
 */

import { getUSBigQueryClient } from "./bigquery-client";

export const US_NYC_ADDRESS_MASTER_V1_REFERENCE = "streetiq-bigquery.streetiq_gold.us_nyc_address_master_v1";
export const US_NYC_BUILDING_TRUTH_V3_REFERENCE = "streetiq-bigquery.streetiq_gold.us_nyc_building_truth_v3";

const MASTER = `\`${US_NYC_ADDRESS_MASTER_V1_REFERENCE}\``;
const TRUTH = `\`${US_NYC_BUILDING_TRUTH_V3_REFERENCE}\``;
const NYC_BQ_LOCATION = "EU";

/**
 * Mirrors `streetiq_gold.nyc_normalize_address_master_v1` (suffix abbreviations + directionals).
 */
export function normalizeNycAddressMasterV1Line(raw: string): string {
  let s = raw.replace(/\s+/g, " ").trim().toUpperCase();
  if (!s) return "";
  const pairs: [RegExp, string][] = [
    [/\bSTREET\b/g, "ST"],
    [/\bAVENUE\b/g, "AVE"],
    [/\bBOULEVARD\b/g, "BLVD"],
    [/\bDRIVE\b/g, "DR"],
    [/\bPLACE\b/g, "PL"],
    [/\bCOURT\b/g, "CT"],
    [/\bLANE\b/g, "LN"],
    [/\bROAD\b/g, "RD"],
    [/\bTERRACE\b/g, "TER"],
    [/\bPARKWAY\b/g, "PKY"],
    [/\bHIGHWAY\b/g, "HWY"],
    [/\bEXPRESSWAY\b/g, "EXPY"],
    [/\bNORTH\b/g, "N"],
    [/\bSOUTH\b/g, "S"],
    [/\bEAST\b/g, "E"],
    [/\bWEST\b/g, "W"],
  ];
  for (const [re, rep] of pairs) {
    s = s.replace(re, rep);
  }
  return s.replace(/\s+/g, " ").trim();
}

function uniqPreserveOrder(lines: readonly string[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const x of lines) {
    const t = x.replace(/\s+/g, " ").trim();
    if (!t || seen.has(t)) continue;
    seen.add(t);
    out.push(t);
  }
  return out;
}

const BUILDING_TRUTH_FULL_ADDRESS_FROM_MASTER_SQL = `
SELECT DISTINCT t.full_address AS full_address
FROM ${TRUTH} AS t
WHERE t.bbl IN (
  SELECT m.bbl
  FROM ${MASTER} AS m
  WHERE m.normalized_address = @norm AND m.bbl IS NOT NULL
)
LIMIT 25
`.trim();

/**
 * Returns `full_address` values from building_truth for any BBL that shares the master key.
 */
export async function queryBuildingTruthFullAddressesFromAddressMaster(
  client: ReturnType<typeof getUSBigQueryClient>,
  normalizedMasterLine: string
): Promise<string[]> {
  const n = normalizedMasterLine.trim();
  if (!n) return [];
  try {
    const [rows] = await client.query({
      query: BUILDING_TRUTH_FULL_ADDRESS_FROM_MASTER_SQL,
      params: { norm: n },
      location: NYC_BQ_LOCATION,
    });
    const list = (rows as { full_address?: string }[] | null | undefined) ?? [];
    return list.map((r) => String(r.full_address ?? "").trim()).filter(Boolean);
  } catch {
    return [];
  }
}

export type NycAddressMasterAugmentResult = {
  candidates: string[];
  masterNormalized: string | null;
  masterHintFullAddresses: readonly string[];
};

/**
 * Prepends building_truth `full_address` hints (via address master BBL) ahead of precomputed candidates.
 */
export async function augmentNycTruthCandidatesWithAddressMaster(
  client: ReturnType<typeof getUSBigQueryClient>,
  norm: {
    normalized_building_address: string;
    normalized_full_address: string;
    candidates: readonly string[];
  }
): Promise<NycAddressMasterAugmentResult> {
  const line =
    norm.normalized_building_address.trim() ||
    norm.normalized_full_address.split(",")[0]?.replace(/\s+/g, " ").trim() ||
    "";
  const masterNorm = normalizeNycAddressMasterV1Line(line);
  if (!masterNorm) {
    return {
      candidates: [...norm.candidates],
      masterNormalized: null,
      masterHintFullAddresses: [],
    };
  }
  const hints = await queryBuildingTruthFullAddressesFromAddressMaster(client, masterNorm);
  return {
    candidates: uniqPreserveOrder([...hints, ...norm.candidates]),
    masterNormalized: masterNorm,
    masterHintFullAddresses: hints,
  };
}
