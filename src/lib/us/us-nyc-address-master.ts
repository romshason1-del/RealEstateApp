/**
 * LEGACY — not used by the current NYC production path (`/api/us/nyc-app-output` → v4).
 * NYC unified address master (`us_nyc_address_master_v1`) — US / BigQuery only; lockstep with
 * `scripts/us/nyc/sql/build-us-nyc-address-master-v1.sql`. Borough hints via `us-nyc-address-normalize.ts`.
 * Kept for the pre-v4 truth pipeline in scripts / internal references.
 */

import { getUSBigQueryClient } from "./bigquery-client";

export { extractPreferredNycBoroughFromUserInput } from "./us-nyc-address-normalize";

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
    [/\bCENTRAL PRK\b/g, "CENTRAL PARK"],
    [/ PRK /g, " PARK "],
    [/ PRK$/g, " PARK"],
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

export function uniqPreserveOrder(lines: readonly string[]): string[] {
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

/** When exact @norm misses (e.g. BLVD vs BOULEVARD in gold), match house + street tokens in normalized_address. */
const MASTER_ROW_FOR_GATE_SQL_LIKE = `
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
  WHERE CONTAINS_SUBSTR(UPPER(m.normalized_address), UPPER(@houseToken))
    AND CONTAINS_SUBSTR(UPPER(m.normalized_address), UPPER(@streetToken))
)
WHERE rn = 1
`.trim();

/**
 * Expand abbreviated street tokens for a second pass against `us_nyc_address_master_v1`
 * (ST↔STREET, BLVD↔BOULEVARD, etc.) when exact normalized keys disagree with PLUTO.
 */
export function expandNycStreetSuffixTokensForPlutoLookup(headLine: string): string {
  let s = headLine.replace(/\s+/g, " ").trim().toUpperCase();
  const pairs: [RegExp, string][] = [
    [/\bBLVD\b/g, "BOULEVARD"],
    [/\bAVE\b/g, "AVENUE"],
    [/\bPKY\b/g, "PARKWAY"],
    [/\bHWY\b/g, "HIGHWAY"],
    [/\bEXPY\b/g, "EXPRESSWAY"],
    [/\bDR\b/g, "DRIVE"],
    [/\bRD\b/g, "ROAD"],
    [/\bCT\b/g, "COURT"],
    [/\bLN\b/g, "LANE"],
    [/\bPL\b/g, "PLACE"],
    [/\bTER\b/g, "TERRACE"],
    [/\bST\b(?=\s*(,|$))/g, "STREET"],
  ];
  for (const [re, rep] of pairs) {
    s = s.replace(re, rep);
  }
  return s.replace(/\s+/g, " ").trim();
}

/** Deterministic alternate keys: primary line and expanded first-segment variant (before comma). */
export function listNycMasterNormLookupKeys(primary: string): string[] {
  const base = primary.trim();
  if (!base) return [];
  const comma = base.indexOf(",");
  const head = comma === -1 ? base : base.slice(0, comma).trim();
  const tail = comma === -1 ? "" : base.slice(comma);
  const headExp = expandNycStreetSuffixTokensForPlutoLookup(head);
  const alt = headExp !== head ? headExp + tail : "";
  return [...new Set([base, alt].filter((x) => x.length > 0))];
}

type MasterGateRow = {
  normalized_address?: unknown;
  unitstotal?: unknown;
  bbl?: unknown;
  zmcode?: unknown;
  bldgclass?: unknown;
};

async function fetchMasterRowForGateWithFallbacks(
  client: ReturnType<typeof getUSBigQueryClient>,
  n: string
): Promise<MasterGateRow | null> {
  const keys = listNycMasterNormLookupKeys(n);
  for (const norm of keys) {
    try {
      const [rows] = await client.query({
        query: MASTER_ROW_FOR_GATE_SQL,
        params: { norm },
        location: NYC_BQ_LOCATION,
      });
      const rowsArr = (rows as MasterGateRow[] | null | undefined) ?? [];
      if (rowsArr[0]) return rowsArr[0]!;
    } catch {
      /* try next key */
    }
  }
  const firstSeg = n.split(",")[0]?.trim() ?? "";
  const toks = firstSeg.split(/\s+/).filter(Boolean);
  if (toks.length < 2) return null;
  const houseToken = toks[0]!;
  const streetTok = toks.slice(1).join(" ");
  const streetVariants = [...new Set([streetTok, expandNycStreetSuffixTokensForPlutoLookup(streetTok)])].filter(
    (s) => s.length >= 2
  );
  for (const streetToken of streetVariants) {
    try {
      const [rows] = await client.query({
        query: MASTER_ROW_FOR_GATE_SQL_LIKE,
        params: { houseToken, streetToken },
        location: NYC_BQ_LOCATION,
      });
      const rowsArr = (rows as MasterGateRow[] | null | undefined) ?? [];
      if (rowsArr[0]) return rowsArr[0]!;
    } catch {
      /* try next */
    }
  }
  return null;
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

const BUILDING_TRUTH_FULL_ADDRESS_BY_BBL_SQL = `
SELECT DISTINCT t.full_address AS full_address
FROM ${TRUTH} AS t
WHERE t.bbl = @bbl
LIMIT 50
`.trim();

export type BuildingTruthFromMasterResult =
  | {
      requiresUnit: true;
      isCommercial: false;
      message: string;
      buildingData: {
        address: string;
        unitstotal: number | null;
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
      /** BBL from resolved master row (unit lookup + card candidate hints). */
      bbl?: string | null;
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

/** All distinct `full_address` rows in building_truth for one BBL (card alias coverage). */
export async function queryFullAddressesByBbl(
  client: ReturnType<typeof getUSBigQueryClient>,
  bbl: string
): Promise<string[]> {
  const b = bbl.trim();
  if (!b) return [];
  try {
    const [rows] = await client.query({
      query: BUILDING_TRUTH_FULL_ADDRESS_BY_BBL_SQL,
      params: { bbl: b },
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
    return { requiresUnit: false, fullAddresses: [], isCommercial: false, unitstotal: null, bldgclass: null, bbl: null };
  }

  const unitTrim = typeof unitNumber === "string" ? unitNumber.trim() : "";

  const row = await fetchMasterRowForGateWithFallbacks(client, n);
  console.log("[MASTER_RAW_ROW]", JSON.stringify(row ?? "NO ROWS"));

  console.log("[GATE DEBUG] bldgclass=", row?.bldgclass, "unitstotal=", row?.unitstotal);

  const bldgClassStr = (row?.bldgclass ?? "").toString();
  const bldgUpper = bldgClassStr.toUpperCase();
  // Office / bank — not residential inventory. PLUTO "C*" is often walk-up apartments; do not treat as commercial here.
  const commercialPrefixes = ["O", "K"];
  const isCommercial = commercialPrefixes.some((p) => bldgUpper.startsWith(p));

  const unitstotal = row != null ? numOrNull(row.unitstotal) : null;

  if (isCommercial) {
    return {
      requiresUnit: false,
      isCommercial: true,
      fullAddresses: [],
      bldgclass: bldgClassStr.trim() !== "" ? bldgClassStr : null,
      unitstotal,
      bbl: row?.bbl != null && row.bbl !== "" ? String(row.bbl) : null,
    };
  }

  const bblStr = row?.bbl != null && row.bbl !== "" ? String(row.bbl) : null;
  const zm = row?.zmcode != null && row.zmcode !== "" ? String(row.zmcode) : null;
  const normAddr = row?.normalized_address != null ? String(row.normalized_address).trim() : n;
  const normForTruthKeys = uniqPreserveOrder([
    ...(row?.normalized_address != null && String(row.normalized_address).trim() !== ""
      ? [String(row.normalized_address).trim()]
      : []),
    ...listNycMasterNormLookupKeys(n),
  ]);

  // Multi-unit residential: PLUTO D/R/C inventory or unitstotal>1 (commercial excluded above).
  const requiresUnit =
    !unitTrim &&
    !isCommercial &&
    ((unitstotal != null && unitstotal > 1) || /^[DRC]/i.test(bldgClassStr));

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

  let fromNorm: string[] = [];
  for (const nk of normForTruthKeys) {
    const part = await queryFullAddressesForNormalizedKey(client, nk);
    fromNorm = uniqPreserveOrder([...fromNorm, ...part]);
  }
  const fromBbl = bblStr ? await queryFullAddressesByBbl(client, bblStr) : [];
  const fullAddresses = uniqPreserveOrder([...fromNorm, ...fromBbl]);
  return {
    requiresUnit: false,
    fullAddresses,
    isCommercial: false,
    bldgclass: bldgClassStr.trim() !== "" ? bldgClassStr : null,
    unitstotal,
    bbl: bblStr,
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

const UNIT_FROM_MASTER_BY_BBL_SQL = `
SELECT
  m.normalized_address AS normalized_address,
  m.raw_address AS raw_address,
  m.bbl AS bbl,
  m.zipcode AS zmcode
FROM ${MASTER} AS m
WHERE m.bbl = @bbl
LIMIT 500
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
function dedupeMasterUnitRows(
  rows: { raw_address?: string; bbl?: unknown; zmcode?: unknown; normalized_address?: unknown }[]
): { raw_address?: string; bbl?: unknown; zmcode?: unknown }[] {
  const seen = new Set<string>();
  const out: typeof rows = [];
  for (const row of rows) {
    const k = `${String(row.normalized_address ?? "")}|${String(row.bbl ?? "")}|${String(row.raw_address ?? "")}`;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(row);
  }
  return out;
}

export async function queryUnitFromAddressMaster(
  client: ReturnType<typeof getUSBigQueryClient>,
  normalizedAddress: string,
  unitNumber: string,
  zmcode?: string | null,
  bblHint?: string | null
): Promise<
  | { matched: true; raw_address: string; bbl: string | null; zmcode: string | null }
  | { matched: false; unitNotFound: true; bbl: string | null; zmcode: string | null }
> {
  const norm = normalizedAddress.trim();
  const u = normalizeUnitToken(unitNumber);
  if (!norm || !u) {
    return { matched: false, unitNotFound: true, bbl: null, zmcode: null };
  }

  let rows: { raw_address?: string; bbl?: unknown; zmcode?: unknown; normalized_address?: unknown }[] = [];
  for (const nKey of listNycMasterNormLookupKeys(norm)) {
    try {
      const [r] = await client.query({
        query: UNIT_FROM_MASTER_SQL,
        params: { norm: nKey },
        location: NYC_BQ_LOCATION,
      });
      const got = (r as typeof rows | null | undefined) ?? [];
      rows = dedupeMasterUnitRows([...rows, ...got]);
      if (got.length > 0) break;
    } catch {
      /* try next normalized key */
    }
  }
  if (rows.length === 0 && typeof bblHint === "string" && bblHint.trim() !== "") {
    try {
      const [r] = await client.query({
        query: UNIT_FROM_MASTER_BY_BBL_SQL,
        params: { bbl: bblHint.trim() },
        location: NYC_BQ_LOCATION,
      });
      rows = dedupeMasterUnitRows((r as typeof rows | null | undefined) ?? []);
    } catch {
      /* BBL query failed — fall through */
    }
  }
  if (rows.length === 0) {
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
