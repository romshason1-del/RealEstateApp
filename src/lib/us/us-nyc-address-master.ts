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
  let input = raw.split(",")[0].trim().toUpperCase();
  // Street name alias corrections (Google Autocomplete variants)
  const streetAliases: Record<string, string> = {
    "CENTRAL PRK": "CENTRAL PARK",
    "CENT PARK": "CENTRAL PARK",
    "CPW": "CENTRAL PARK W",
    "AVE OF AMERICAS": "6 AVE",
    "AVENUE OF THE AMERICAS": "6 AVE",
    "LENOX AVE": "MALCOLM X BLVD",
    "6TH AVE": "6 AVE",
  };
  for (const [alias, correct] of Object.entries(streetAliases)) {
    if (input.includes(alias)) {
      input = input.replace(alias, correct);
      break;
    }
  }
  let s = input.replace(/\s+/g, " ").trim();
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

const MASTER_ROW_FOR_GATE_SQL = `
SELECT
  normalized_address,
  unitstotal,
  bbl,
  zmcode,
  bldgclass
FROM (
  SELECT
    m.normalized_address AS normalized_address,
    m.unitstotal AS unitstotal,
    m.bbl AS bbl,
    m.zipcode AS zmcode,
    m.bldgclass AS bldgclass,
    ROW_NUMBER() OVER (
      ORDER BY CASE WHEN m.source = 'PLUTO' THEN 0 ELSE 1 END, m.unitstotal DESC NULLS LAST
    ) AS rn
  FROM ${MASTER} AS m
  WHERE m.normalized_address = @norm
)
WHERE rn = 1
`.trim();

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

export type BuildingTruthFromMasterResult =
  | {
      requiresUnit: true;
      isCommercial: false;
      message: string;
      buildingData: {
        address: string;
        unitstotal: number;
        bbl: string | null;
        zmcode?: string | null;
      };
      /** PLUTO unitstotal from master row (for route logging). */
      unitstotal?: number | null;
      bldgclass?: string | null;
    }
  | {
      requiresUnit: false;
      fullAddresses: string[];
      isCommercial: boolean;
      /** Present when row had bldgclass (for gates/logging). */
      bldgclass?: string | null;
      /** PLUTO unitstotal from master row (for route logging). */
      unitstotal?: number | null;
    };

function numOrNull(v: unknown): number | null {
  if (v == null || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "bigint") return Number(v);
  const n = Number(String(v).replace(/,/g, ""));
  return Number.isFinite(n) ? n : null;
}

/** Building-truth `full_address` hints for a normalized master key (no unit gate — used by truth augment). */
export async function queryFullAddressesForNormalizedKey(
  client: ReturnType<typeof getUSBigQueryClient>,
  norm: string
): Promise<string[]> {
  try {
    const [rows] = await client.query({
      query: BUILDING_TRUTH_FULL_ADDRESS_FROM_MASTER_SQL,
      params: { norm },
      location: NYC_BQ_LOCATION,
    });
    const list = (rows as { full_address?: string }[] | null | undefined) ?? [];
    return list.map((r) => String(r.full_address ?? "").trim()).filter(Boolean);
  } catch {
    return [];
  }
}

/**
 * Returns `full_address` values from building_truth for any BBL that shares the master key,
 * or a unit gate when PLUTO reports multiple units and no unit was supplied.
 */
export async function queryBuildingTruthFullAddressesFromAddressMaster(
  client: ReturnType<typeof getUSBigQueryClient>,
  normalizedMasterLine: string,
  unitNumber?: string | null
): Promise<BuildingTruthFromMasterResult> {
  console.log("[MASTER_GATE_CALLED] masterNorm:", normalizedMasterLine);
  const n = normalizedMasterLine.trim();
  if (!n) {
    return { requiresUnit: false, fullAddresses: [], isCommercial: false, unitstotal: null, bldgclass: null };
  }

  const unitTrim = typeof unitNumber === "string" ? unitNumber.trim() : "";

  type MasterGateRow = {
    normalized_address?: unknown;
    unitstotal?: unknown;
    bbl?: unknown;
    zmcode?: unknown;
    bldgclass?: unknown;
  };
  let row: MasterGateRow | null = null;

  try {
    const [rows] = await client.query({
      query: MASTER_ROW_FOR_GATE_SQL,
      params: { norm: n },
      location: NYC_BQ_LOCATION,
    });
    const rowsArr = (rows as MasterGateRow[] | null | undefined) ?? [];
    console.log("[MASTER_RAW_ROW]", JSON.stringify(rowsArr?.[0] ?? "NO ROWS"));
    const list = rowsArr;
    row = list[0] ?? null;
  } catch {
    row = null;
  }

  console.log("[GATE DEBUG] bldgclass=", row?.bldgclass, "unitstotal=", row?.unitstotal);

  const bldgClassStr = (row?.bldgclass ?? "").toString();
  // Always evaluate from PLUTO bldgclass — independent of whether a unit was submitted.
  const commercialPrefixes = ["O", "C", "K"];
  const isCommercial = commercialPrefixes.some((p) => bldgClassStr.toUpperCase().startsWith(p));

  const unitstotal = row != null ? numOrNull(row.unitstotal) : null;

  if (isCommercial) {
    return {
      requiresUnit: false,
      isCommercial: true,
      fullAddresses: [],
      bldgclass: bldgClassStr.trim() !== "" ? bldgClassStr : null,
      unitstotal,
    };
  }

  const bblStr = row?.bbl != null && row.bbl !== "" ? String(row.bbl) : null;
  const zm = row?.zmcode != null && row.zmcode !== "" ? String(row.zmcode) : null;
  const normAddr = row?.normalized_address != null ? String(row.normalized_address) : n;

  // Multi-unit residential: require a unit unless one was supplied (commercial already excluded above).
  const requiresUnit =
    unitstotal != null && unitstotal > 1 && !isCommercial && !unitTrim;

  if (requiresUnit) {
    return {
      requiresUnit: true,
      isCommercial: false,
      message: "Please enter a unit number to see specific valuation and sales history",
      unitstotal,
      bldgclass: bldgClassStr.trim() !== "" ? bldgClassStr : null,
      buildingData: {
        address: normAddr,
        unitstotal,
        bbl: bblStr,
        zmcode: zm,
      },
    };
  }

  const fullAddresses = await queryFullAddressesForNormalizedKey(client, n);
  return {
    requiresUnit: false,
    fullAddresses,
    isCommercial: false,
    bldgclass: bldgClassStr.trim() !== "" ? bldgClassStr : null,
    unitstotal,
  };
}

const UNIT_FROM_MASTER_SQL = `
SELECT
  m.normalized_address AS normalized_address,
  m.raw_address AS raw_address,
  m.bbl AS bbl,
  m.zipcode AS zmcode
FROM ${MASTER} AS m
WHERE m.normalized_address = @norm
LIMIT 100
`.trim();

function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Normalizes unit token for loose matching in raw_address (digits / alphanumeric). */
function normalizeUnitToken(raw: string): string {
  return raw.replace(/\s+/g, " ").trim().toUpperCase();
}

/**
 * Looks up master rows for a normalized key and tries to associate a unit with raw_address text.
 * No dedicated unit column yet — match is heuristic on raw_address.
 * When `zmcode` is set, prefers rows whose `zmcode` matches (USPS-style ZIP from input).
 */
export async function queryUnitFromAddressMaster(
  client: ReturnType<typeof getUSBigQueryClient>,
  normalizedAddress: string,
  unitNumber: string,
  zmcode?: string | null
): Promise<
  | { matched: true; raw_address: string; bbl: string | null; zmcode: string | null }
  | { matched: false; unitNotFound: true; bbl: string | null; zmcode: string | null }
> {
  const norm = normalizedAddress.trim();
  const u = normalizeUnitToken(unitNumber);
  if (!norm || !u) {
    return { matched: false, unitNotFound: true, bbl: null, zmcode: null };
  }

  let rows: { raw_address?: string; bbl?: unknown; zmcode?: unknown }[] = [];
  try {
    const [r] = await client.query({
      query: UNIT_FROM_MASTER_SQL,
      params: { norm },
      location: NYC_BQ_LOCATION,
    });
    rows = (r as typeof rows | null | undefined) ?? [];
  } catch {
    return { matched: false, unitNotFound: true, bbl: null, zmcode: null };
  }

  const zmTrim = typeof zmcode === "string" ? zmcode.trim() : "";
  if (zmTrim) {
    const byZm = rows.filter((row) => String(row.zmcode ?? "").trim() === zmTrim);
    if (byZm.length > 0) rows = byZm;
  }

  const digitMatch = u.match(/\d+/);
  const digit = digitMatch ? digitMatch[0] : null;
  const patterns: RegExp[] = [];
  if (digit) {
    patterns.push(
      new RegExp(`(?:^|[,\\s#])(?:APT|APARTMENT|UNIT|STE|SUITE|FL|FLOOR|RM|ROOM)\\.?\\s*${escapeRegExp(digit)}\\b`, "i"),
      new RegExp(`(?:^|[,\\s#])#\\s*${escapeRegExp(digit)}\\b`, "i"),
      new RegExp(`\\b${escapeRegExp(digit)}\\s*(?:APT|APARTMENT|UNIT)\\b`, "i")
    );
  }
  patterns.push(new RegExp(`\\b${escapeRegExp(u)}\\b`, "i"));

  let fallbackBbl: string | null = null;
  let fallbackZm: string | null = null;
  for (const row of rows) {
    const raw = String(row.raw_address ?? "");
    const bbl = row.bbl != null && row.bbl !== "" ? String(row.bbl) : null;
    const zm = row.zmcode != null && row.zmcode !== "" ? String(row.zmcode) : null;
    if (fallbackBbl == null && bbl) fallbackBbl = bbl;
    if (fallbackZm == null && zm) fallbackZm = zm;
    const upper = raw.toUpperCase();
    for (const re of patterns) {
      if (re.test(upper)) {
        return { matched: true, raw_address: raw.trim(), bbl, zmcode: zm };
      }
    }
  }

  return {
    matched: false,
    unitNotFound: true,
    bbl: fallbackBbl,
    zmcode: fallbackZm,
  };
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
  const hints = await queryFullAddressesForNormalizedKey(client, masterNorm);
  return {
    candidates: uniqPreserveOrder([...hints, ...norm.candidates]),
    masterNormalized: masterNorm,
    masterHintFullAddresses: hints,
  };
}
