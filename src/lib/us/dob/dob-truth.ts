/**
 * NYC DOB-derived building insights (US only). Data layer only — no UI or routes.
 */

import { fetchDobFilingsByAddress, type DobFilingRow } from "./dob-filings";
import { normalizeStreetNameForDobQuery } from "./dob-street-normalize";

export type DobNycBuildingInsightsSuccess = {
  success: true;
  address_key: string;
  has_filings: boolean;
  filing_count: number;
  building_type: string | null;
  existing_units: number | null;
  proposed_units: number | null;
};

export type FetchDobNycBuildingInsightsResult =
  | DobNycBuildingInsightsSuccess
  | { success: false; error: string; status?: number };

function buildDobAddressKey(houseNumber: string, streetName: string): string {
  const h = houseNumber.trim();
  const s = normalizeStreetNameForDobQuery(streetName);
  return `${h}|${s}`;
}

function parseDwellingUnits(s: string | null): number | null {
  if (s == null) return null;
  const t = String(s).trim().replace(/,/g, "");
  if (!t) return null;
  const n = Number(t);
  if (!Number.isFinite(n) || n < 0) return null;
  return n;
}

function maxDwellingUnits(rows: DobFilingRow[], field: "existing_units" | "proposed_units"): number | null {
  let max: number | null = null;
  for (const r of rows) {
    const n = parseDwellingUnits(r[field]);
    if (n == null) continue;
    if (max == null || n > max) max = n;
  }
  return max;
}

/** Most common non-empty `building_type`; ties broken by lexicographic sort (stable). */
function modeBuildingType(rows: DobFilingRow[]): string | null {
  const counts = new Map<string, number>();
  for (const r of rows) {
    const b = r.building_type;
    if (b == null) continue;
    const k = b.trim();
    if (!k) continue;
    counts.set(k, (counts.get(k) ?? 0) + 1);
  }
  if (counts.size === 0) return null;

  let best = -1;
  const tied: string[] = [];
  for (const [k, c] of counts) {
    if (c > best) {
      best = c;
      tied.length = 0;
      tied.push(k);
    } else if (c === best) {
      tied.push(k);
    }
  }
  tied.sort();
  return tied[0] ?? null;
}

/**
 * Fetch DOB filings for an NYC address and return a deterministic summary.
 */
export async function fetchDobNycBuildingInsights(params: {
  houseNumber: string;
  streetName: string;
  signal?: AbortSignal;
}): Promise<FetchDobNycBuildingInsightsResult> {
  const address_key = buildDobAddressKey(params.houseNumber, params.streetName);

  const fetched = await fetchDobFilingsByAddress({
    houseNumber: params.houseNumber,
    streetName: params.streetName,
    signal: params.signal,
  });

  if (!fetched.success) {
    return { success: false, error: fetched.error, status: fetched.status };
  }

  const rows = fetched.rows;
  const filing_count = rows.length;

  if (filing_count === 0) {
    return {
      success: true,
      address_key,
      has_filings: false,
      filing_count: 0,
      building_type: null,
      existing_units: null,
      proposed_units: null,
    };
  }

  return {
    success: true,
    address_key,
    has_filings: true,
    filing_count,
    building_type: modeBuildingType(rows),
    existing_units: maxDwellingUnits(rows, "existing_units"),
    proposed_units: maxDwellingUnits(rows, "proposed_units"),
  };
}
