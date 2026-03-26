/**
 * DOB Job Application Filings — lookup by house number + street name (US only).
 * Street line uses DOB-specific suffix abbreviation; house uses trim only.
 *
 * Note: NYC Socrata exposes the house number column as `house__` (not `house_no`).
 */

import { DOB_FILINGS_DEFAULT_LIMIT, dobJobFilingsResourcePath } from "./dob-config";
import { dobEscapeSoqlString, dobSocrataGet } from "./dob-client";
import { normalizeStreetNameForDobQuery } from "./dob-street-normalize";

/** Subset of columns returned to callers (mapped from Socrata field names). */
export type DobFilingRow = {
  job_type: string | null;
  job_status: string | null;
  building_type: string | null;
  existing_units: string | null;
  proposed_units: string | null;
};

export type FetchDobFilingsResult =
  | { success: true; rows: DobFilingRow[] }
  | { success: false; error: string; status?: number };

type SocrataDobFilingRaw = {
  job_type?: string;
  job_status?: string;
  building_type?: string;
  existing_dwelling_units?: string;
  proposed_dwelling_units?: string;
};

function strOrNull(v: unknown): string | null {
  if (v == null) return null;
  const s = String(v).trim();
  return s ? s : null;
}

function normalizeFilingRow(raw: SocrataDobFilingRaw): DobFilingRow {
  return {
    job_type: strOrNull(raw.job_type),
    job_status: strOrNull(raw.job_status),
    building_type: strOrNull(raw.building_type),
    existing_units: strOrNull(raw.existing_dwelling_units),
    proposed_units: strOrNull(raw.proposed_dwelling_units),
  };
}

/**
 * Trim house number; normalize street for DOB (uppercase + abbreviated suffixes), then SoQL
 * `upper(street_name) = upper('...')`.
 */
function normalizeForDobAddressQuery(houseNumber: string, streetName: string): {
  house: string;
  street: string;
} {
  return {
    house: houseNumber.trim(),
    street: normalizeStreetNameForDobQuery(streetName),
  };
}

const DOB_FILINGS_SELECT_FIELDS =
  "job_type,job_status,building_type,existing_dwelling_units,proposed_dwelling_units";

/**
 * Fetch DOB job application filings for a street address.
 * @param limit — capped at {@link DOB_FILINGS_DEFAULT_LIMIT} (50).
 */
export async function fetchDobFilingsByAddress(params: {
  houseNumber: string;
  streetName: string;
  limit?: number;
  signal?: AbortSignal;
}): Promise<FetchDobFilingsResult> {
  const { house, street } = normalizeForDobAddressQuery(params.houseNumber, params.streetName);
  if (!house || !street) {
    return { success: false, error: "houseNumber and streetName are required" };
  }

  const cap = Math.min(DOB_FILINGS_DEFAULT_LIMIT, Math.max(1, params.limit ?? DOB_FILINGS_DEFAULT_LIMIT));

  const escH = dobEscapeSoqlString(house);
  const escS = dobEscapeSoqlString(street);
  /** `house__` is the official Socrata field name for house number on this dataset. */
  const where = `house__ = '${escH}' AND upper(street_name) = upper('${escS}')`;

  const res = await dobSocrataGet<SocrataDobFilingRaw[]>(
    dobJobFilingsResourcePath(),
    {
      $select: DOB_FILINGS_SELECT_FIELDS,
      $where: where,
      $limit: String(cap),
    },
    { signal: params.signal }
  );

  if (!res.ok) {
    return { success: false, error: res.error, status: res.status };
  }

  const data = Array.isArray(res.data) ? res.data : [];
  const rows: DobFilingRow[] = data.map((raw) => normalizeFilingRow(raw));
  return { success: true, rows };
}
