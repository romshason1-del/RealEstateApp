/**
 * ACRIS Real Property Legals — lookup by street number + street name (US only).
 */

import { acrisDefaultLimitParam, acrisEscapeSoqlString, acrisSocrataGet } from "./acris-client";
import { acrisLegalsResourcePath } from "./acris-config";

/** Normalized row used downstream (subset of Socrata columns). */
export type AcrisLegalRow = {
  document_id: string;
  borough: number;
  block: number;
  lot: number;
  property_type: string | null;
  street_number: string | null;
  street_name: string | null;
};

type SocrataLegalRaw = {
  document_id?: string;
  borough?: string | number;
  block?: string | number;
  lot?: string | number;
  property_type?: string;
  street_number?: string;
  street_name?: string;
};

function toNum(v: string | number | undefined): number {
  if (v === undefined || v === null) return NaN;
  const n = typeof v === "number" ? v : Number(String(v).replace(/,/g, ""));
  return Number.isFinite(n) ? n : NaN;
}

function normalizeLegalRow(raw: SocrataLegalRaw): AcrisLegalRow | null {
  const document_id = String(raw.document_id ?? "").trim();
  if (!document_id) return null;
  const borough = toNum(raw.borough);
  const block = toNum(raw.block);
  const lot = toNum(raw.lot);
  if (!Number.isFinite(borough) || !Number.isFinite(block) || !Number.isFinite(lot)) return null;
  return {
    document_id,
    borough,
    block,
    lot,
    property_type: raw.property_type != null && String(raw.property_type).trim() ? String(raw.property_type).trim() : null,
    street_number: raw.street_number != null && String(raw.street_number).trim() ? String(raw.street_number).trim() : null,
    street_name: raw.street_name != null && String(raw.street_name).trim() ? String(raw.street_name).trim() : null,
  };
}

export type FetchAcrisLegalsByStreetResult =
  | { success: true; rows: AcrisLegalRow[] }
  | { success: false; error: string; status?: number };

/**
 * Query Legals where `street_number` and `street_name` match (case-insensitive on street name).
 * When `unit` is set, also requires ACRIS `unit` (Unit Number for BBL) to match — condo/co-op unit deeds.
 * Caller supplies values as recorded by ACRIS when possible (e.g. street name uppercase).
 */
export async function fetchAcrisLegalsByStreetAddress(params: {
  streetNumber: string;
  streetName: string;
  limit?: number;
  signal?: AbortSignal;
  /** Optional: filter to this unit/apartment (matches Legals `unit` column). */
  unit?: string | null;
}): Promise<FetchAcrisLegalsByStreetResult> {
  const sn = params.streetNumber.trim();
  const st = params.streetName.trim();
  if (!sn || !st) {
    return { success: false, error: "streetNumber and streetName are required" };
  }

  const escN = acrisEscapeSoqlString(sn);
  const escS = acrisEscapeSoqlString(st);
  let where = `street_number = '${escN}' AND upper(street_name) = upper('${escS}')`;
  const unitRaw = typeof params.unit === "string" ? params.unit.trim() : "";
  if (unitRaw) {
    const escU = acrisEscapeSoqlString(unitRaw);
    where += ` AND upper(trim(unit)) = upper('${escU}')`;
  }

  const res = await acrisSocrataGet<SocrataLegalRaw[]>(
    acrisLegalsResourcePath(),
    {
      $where: where,
      $limit: acrisDefaultLimitParam(params.limit),
      $order: "document_id ASC",
    },
    { signal: params.signal }
  );

  if (!res.ok) {
    return { success: false, error: res.error, status: res.status };
  }

  const data = Array.isArray(res.data) ? res.data : [];
  const rows: AcrisLegalRow[] = [];
  for (const raw of data) {
    const row = normalizeLegalRow(raw);
    if (row) rows.push(row);
  }
  return { success: true, rows };
}
