/**
 * First-pass PLUTO lookup (`us_nyc_pluto_normalized`) for NYC residential multi-unit gating (US / BigQuery only).
 * Must run before card / sales fetches when enforcing strict unit selection.
 */

import { getUSBigQueryClient } from "./bigquery-client";
import { expandNycStreetSuffixTokensForPlutoLookup } from "./us-nyc-address-master";

export const US_NYC_PLUTO_NORMALIZED_REFERENCE = "streetiq-bigquery.streetiq_gold.us_nyc_pluto_normalized";

const PLUTO = `\`${US_NYC_PLUTO_NORMALIZED_REFERENCE}\``;
const NYC_BQ_LOCATION = "EU";

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

function firstSegmentBeforeComma(line: string): string {
  const c = line.indexOf(",");
  return c === -1 ? line.trim() : line.slice(0, c).trim();
}

function parseHouseAndStreetTokens(firstSegment: string): { houseToken: string; streetTok: string } | null {
  const toks = firstSegment.replace(/\s+/g, " ").trim().split(/\s+/).filter(Boolean);
  if (toks.length < 2) return null;
  return { houseToken: toks[0]!, streetTok: toks.slice(1).join(" ") };
}

function isResidentialAbcd(bldgclass: string): boolean {
  return /^[ABCD]/i.test(bldgclass.trim());
}

export type PlutoResidentialMultiUnitGateResult =
  | {
      hit: true;
      /** True when PLUTO class is A–D and unitstotal > 1 — block card/sales until unit is supplied. */
      strictMultiUnitResidential: boolean;
      bbl: string | null;
      bldgclass: string;
      unitstotal: number | null;
    }
  | { hit: false };

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

/**
 * Query PLUTO before card/sales. If a row matches house+street (BLVD/BOULEVARD via @streetTokenExpanded)
 * and is residential class A–D with unitstotal > 1, callers must require a unit before any valuation fetch.
 */
export async function queryPlutoResidentialMultiUnitGate(
  client: ReturnType<typeof getUSBigQueryClient>,
  normalizedBuildingOrCoreLine: string,
  zipFromInput: string | null
): Promise<PlutoResidentialMultiUnitGateResult> {
  const first = firstSegmentBeforeComma(normalizedBuildingOrCoreLine);
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

    const bbl = row.bbl != null && String(row.bbl).trim() !== "" ? String(row.bbl).trim() : null;
    const bldgclass = String(row.bldgclass ?? "").trim();
    const utRaw = row.unitstotal;
    const ut =
      utRaw != null && utRaw !== ""
        ? Number(utRaw)
        : Number.NaN;
    const unitstotal = Number.isFinite(ut) ? ut : null;

    const strict = isResidentialAbcd(bldgclass) && unitstotal != null && unitstotal > 1;
    return {
      hit: true,
      strictMultiUnitResidential: strict,
      bbl,
      bldgclass,
      unitstotal,
    };
  } catch {
    return { hit: false };
  }
}
