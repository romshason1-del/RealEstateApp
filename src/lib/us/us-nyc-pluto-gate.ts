/**
 * First-pass PLUTO lookup (`us_nyc_pluto_normalized`) for NYC residential multi-unit gating (US / BigQuery only).
 * Must run before card / sales fetches when enforcing strict unit selection.
 *
 * Center Blvd (Queens) / PLUTO range nuance: official PLUTO may list "45-45 CENTER BOULEVARD" while users type
 * "47-20 CENTER BLVD" — we map known cases to BBL `4000210020` and fetch by BBL.
 */

import { getUSBigQueryClient } from "./bigquery-client";
import { expandNycStreetSuffixTokensForPlutoLookup } from "./us-nyc-address-master";

export const US_NYC_PLUTO_NORMALIZED_REFERENCE = "streetiq-bigquery.streetiq_gold.us_nyc_pluto_normalized";

/** Known LIC / Center Boulevard inventory — force unit selection before any card lookup. */
export const NYC_CENTER_BLVD_LICENSE_PLUTO_BBL = "4000210020";

const PLUTO = `\`${US_NYC_PLUTO_NORMALIZED_REFERENCE}\``;
const NYC_BQ_LOCATION = "EU";

const PLUTO_BY_BBL_SQL = `
SELECT
  TRIM(CAST(bbl AS STRING)) AS bbl,
  TRIM(UPPER(CAST(bldgclass AS STRING))) AS bldgclass,
  SAFE_CAST(unitstotal AS INT64) AS unitstotal
FROM ${PLUTO} AS p
WHERE TRIM(CAST(p.bbl AS STRING)) = @bbl
LIMIT 1
`.trim();

const PLUTO_RESIDENTIAL_PROBE_SQL = `
SELECT
  TRIM(CAST(bbl AS STRING)) AS bbl,
  TRIM(UPPER(CAST(bldgclass AS STRING))) AS bldgclass,
  SAFE_CAST(unitstotal AS INT64) AS unitstotal
FROM ${PLUTO} AS p
WHERE p.address IS NOT NULL
  AND TRIM(CAST(p.address AS STRING)) != ''
  AND CONTAINS_SUBSTR(UPPER(COALESCE(p.address, '')), UPPER(@houseToken))
  AND (
    CONTAINS_SUBSTR(UPPER(COALESCE(p.address, '')), UPPER(@streetToken))
    OR CONTAINS_SUBSTR(UPPER(COALESCE(p.address, '')), UPPER(@streetTokenExpanded))
  )
  AND (
    @zipFilter IS NULL
    OR TRIM(REGEXP_REPLACE(CAST(p.postcode AS STRING), r'\\.0\\s*$', '')) = @zipFilter
  )
QUALIFY ROW_NUMBER() OVER (
  ORDER BY
    CASE WHEN REGEXP_CONTAINS(UPPER(CAST(bldgclass AS STRING)), r'^[ABCD]') THEN 0 ELSE 1 END,
    SAFE_CAST(unitstotal AS INT64) DESC NULLS LAST
) = 1
LIMIT 1
`.trim();

/** BLVD → BOULEVARD before any PLUTO match (aligns with PLUTO text). */
export function normalizeBlvdToBoulevardForPluto(line: string): string {
  return line.replace(/\s+/g, " ").replace(/\bBLVD\.?\b/gi, "BOULEVARD").replace(/\s+/g, " ").trim();
}

function firstSegmentBeforeComma(line: string): string {
  const c = line.indexOf(",");
  return c === -1 ? line.trim() : line.slice(0, c).trim();
}

function normalizeHouseToken(token: string): string {
  return token.replace(/[–—]/g, "-").trim();
}

function parseHouseAndStreetTokens(firstSegment: string): { houseToken: string; streetTok: string } | null {
  const seg = normalizeBlvdToBoulevardForPluto(firstSegment);
  const toks = seg.replace(/\s+/g, " ").trim().split(/\s+/).filter(Boolean);
  if (toks.length < 2) return null;
  return { houseToken: normalizeHouseToken(toks[0]!), streetTok: toks.slice(1).join(" ") };
}

/** Maps tokens like 47-20 to a single comparable int for range columns (4720). */
export function houseTokenToComparableRangeInt(token: string): number | null {
  const t = normalizeHouseToken(token);
  const hyphen = t.match(/^(\d+)-(\d+)$/);
  if (hyphen) {
    const a = parseInt(hyphen[1]!, 10);
    const b = parseInt(hyphen[2]!, 10);
    if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
    return a * 100 + b;
  }
  const digits = parseInt(t.replace(/\D/g, ""), 10);
  return Number.isFinite(digits) ? digits : null;
}

function isResidentialAbcd(bldgclass: string): boolean {
  return /^[ABCD]/i.test(bldgclass.trim());
}

export type PlutoResidentialMultiUnitGateResult =
  | {
      hit: true;
      /** True → block card/sales until unit_or_lot is supplied. */
      strictMultiUnitResidential: boolean;
      bbl: string | null;
      bldgclass: string;
      unitstotal: number | null;
      match_kind: "manual_override" | "center_blvd_license_bbl" | "generic_probe";
    }
  | { hit: false };

/**
 * Hard override: PLUTO may list "45-45 CENTER BOULEVARD" while users type "47-20 CENTER BLVD".
 * Do not require a perfect token match — substring match on the combined input + normalized line.
 */
export function resolveManualLicenseBblOverride(fullAddressText: string): string | null {
  const u = fullAddressText.replace(/[–—]/g, "-").toUpperCase();
  const hasCenter = u.includes("CENTER");
  if (!hasCenter) return null;
  if (u.includes("47-20") && u.includes("CENTER")) return NYC_CENTER_BLVD_LICENSE_PLUTO_BBL;
  if (u.includes("45-45") && u.includes("CENTER")) return NYC_CENTER_BLVD_LICENSE_PLUTO_BBL;
  return null;
}

async function fetchPlutoRowByBbl(
  client: ReturnType<typeof getUSBigQueryClient>,
  bbl: string
): Promise<{ bbl?: unknown; bldgclass?: unknown; unitstotal?: unknown } | null> {
  try {
    const [rows] = await client.query({
      query: PLUTO_BY_BBL_SQL,
      params: { bbl },
      location: NYC_BQ_LOCATION,
    });
    return (rows as { bbl?: unknown; bldgclass?: unknown; unitstotal?: unknown }[] | null | undefined)?.[0] ?? null;
  } catch {
    return null;
  }
}

async function fetchPlutoRow(
  client: ReturnType<typeof getUSBigQueryClient>,
  houseToken: string,
  streetTok: string,
  streetExpanded: string,
  zipFilter: string | null
): Promise<{ bbl?: unknown; bldgclass?: unknown; unitstotal?: unknown } | null> {
  const [rows] = await client.query({
    query: PLUTO_RESIDENTIAL_PROBE_SQL,
    params: {
      houseToken,
      streetToken: streetTok,
      streetTokenExpanded: streetExpanded,
      zipFilter,
    },
    location: NYC_BQ_LOCATION,
  });
  return (rows as { bbl?: unknown; bldgclass?: unknown; unitstotal?: unknown }[] | null | undefined)?.[0] ?? null;
}

function rowFromPluto(
  row: { bbl?: unknown; bldgclass?: unknown; unitstotal?: unknown },
  match_kind: "manual_override" | "center_blvd_license_bbl" | "generic_probe"
): PlutoResidentialMultiUnitGateResult {
  const bbl = row.bbl != null && String(row.bbl).trim() !== "" ? String(row.bbl).trim() : null;
  const bldgclass = String(row.bldgclass ?? "").trim();
  const utRaw = row.unitstotal;
  const ut =
    utRaw != null && utRaw !== ""
      ? Number(utRaw)
      : Number.NaN;
  const unitstotal = Number.isFinite(ut) ? ut : null;

  const strict = computeStrictRequiresUnit(bbl, bldgclass, unitstotal);
  return {
    hit: true,
    strictMultiUnitResidential: strict,
    bbl,
    bldgclass,
    unitstotal,
    match_kind,
  };
}

/** BBL 4000210020 always requires a unit; otherwise A–D with unitstotal > 1. */
export function computeStrictRequiresUnit(
  bbl: string | null,
  bldgclass: string,
  unitstotal: number | null
): boolean {
  if (bbl === NYC_CENTER_BLVD_LICENSE_PLUTO_BBL) return true;
  return isResidentialAbcd(bldgclass) && unitstotal != null && unitstotal > 1;
}

/**
 * Query PLUTO before card/sales. BLVD is normalized to BOULEVARD before tokenization.
 * Manual overrides (`addressRawForOverrides` + normalized line) run first for known Center Blvd BBLs.
 */
export async function queryPlutoResidentialMultiUnitGate(
  client: ReturnType<typeof getUSBigQueryClient>,
  normalizedBuildingOrCoreLine: string,
  zipFromInput: string | null,
  addressRawForOverrides?: string | null
): Promise<PlutoResidentialMultiUnitGateResult> {
  const combinedForOverride = `${addressRawForOverrides ?? ""} ${normalizedBuildingOrCoreLine}`.replace(/\s+/g, " ").trim();
  const manualBbl = resolveManualLicenseBblOverride(combinedForOverride);
  if (manualBbl) {
    try {
      const row = await fetchPlutoRowByBbl(client, manualBbl);
      return rowFromPluto(
        row ?? { bbl: manualBbl, bldgclass: "", unitstotal: null },
        row ? "center_blvd_license_bbl" : "manual_override"
      );
    } catch {
      return rowFromPluto({ bbl: manualBbl, bldgclass: "", unitstotal: null }, "manual_override");
    }
  }

  const firstRaw = firstSegmentBeforeComma(normalizedBuildingOrCoreLine);
  const first = normalizeBlvdToBoulevardForPluto(firstRaw);
  const parsed = parseHouseAndStreetTokens(first);
  if (!parsed) return { hit: false };

  const streetExpanded = expandNycStreetSuffixTokensForPlutoLookup(parsed.streetTok);
  const zipFilter =
    typeof zipFromInput === "string" && zipFromInput.trim() !== "" ? zipFromInput.trim() : null;

  try {
    let row = await fetchPlutoRow(client, parsed.houseToken, parsed.streetTok, streetExpanded, zipFilter);
    if (!row && zipFilter) {
      row = await fetchPlutoRow(client, parsed.houseToken, parsed.streetTok, streetExpanded, null);
    }
    if (!row) return { hit: false };

    return rowFromPluto(row, "generic_probe");
  } catch {
    return { hit: false };
  }
}
