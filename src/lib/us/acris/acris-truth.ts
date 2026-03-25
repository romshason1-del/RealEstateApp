/**
 * NYC ACRIS → deterministic deed history for truth-layer enrichment (US only).
 * Not wired to API/UI. No apartment or street-average logic.
 */

import { fetchAcrisLegalsByStreetAddress } from "./acris-legals";
import { fetchAcrisMasterDeedsByDocumentId, type AcrisMasterDeedRow } from "./acris-master";
import { acrisMasterRowsByDocumentId, joinAcrisLegalsWithMasterDeeds, type AcrisJoinedRow } from "./acris-join";

export type AcrisTruthDeed = {
  document_id: string;
  borough: number;
  block: number;
  lot: number;
  property_type: string;
  document_date: string | null;
  document_amt: number;
};

export type AcrisTruthDeedHistoryOk = {
  success: true;
  address_key: string;
  deeds: AcrisTruthDeed[];
  latest_deed: AcrisTruthDeed | null;
  has_multiple_deeds: boolean;
};

export type AcrisTruthDeedHistoryErr = {
  success: false;
  address_key: string;
  error: string;
  status?: number;
};

export type AcrisTruthDeedHistoryResult = AcrisTruthDeedHistoryOk | AcrisTruthDeedHistoryErr;

/** Stable key for caching / joins (trimmed; street name uppercased). */
export function buildAcrisNycAddressKey(streetNumber: string, streetName: string): string {
  const n = streetNumber.trim();
  const s = streetName.trim().toUpperCase();
  return `${n}|${s}`;
}

function documentDateSortMs(iso: string | null): number {
  if (iso == null || !String(iso).trim()) return Number.NEGATIVE_INFINITY;
  const t = Date.parse(String(iso));
  return Number.isFinite(t) ? t : Number.NEGATIVE_INFINITY;
}

/** Newest `document_date` first; missing dates last; ties broken deterministically. */
function sortDeedsNewestFirstStable(a: AcrisTruthDeed, b: AcrisTruthDeed): number {
  const tb = documentDateSortMs(b.document_date);
  const ta = documentDateSortMs(a.document_date);
  if (tb !== ta) return tb - ta;
  if (a.document_id !== b.document_id) return a.document_id < b.document_id ? -1 : a.document_id > b.document_id ? 1 : 0;
  if (a.borough !== b.borough) return a.borough - b.borough;
  if (a.block !== b.block) return a.block - b.block;
  if (a.lot !== b.lot) return a.lot - b.lot;
  if (a.property_type !== b.property_type) return a.property_type < b.property_type ? -1 : a.property_type > b.property_type ? 1 : 0;
  return a.document_amt - b.document_amt;
}

function joinedRowToTruthDeed(row: AcrisJoinedRow): AcrisTruthDeed {
  return {
    document_id: row.document_id,
    borough: row.borough,
    block: row.block,
    lot: row.lot,
    property_type: row.property_type,
    document_date: row.document_date,
    document_amt: row.document_amt,
  };
}

/**
 * Legals → master (DEED, amt > 0) per `document_id` → join → deed list sorted newest-first.
 */
export async function fetchAcrisNycTruthDeedHistory(params: {
  streetNumber: string;
  streetName: string;
  signal?: AbortSignal;
  legalsLimit?: number;
}): Promise<AcrisTruthDeedHistoryResult> {
  const address_key = buildAcrisNycAddressKey(params.streetNumber, params.streetName);

  const legalsResult = await fetchAcrisLegalsByStreetAddress({
    streetNumber: params.streetNumber,
    streetName: params.streetName,
    limit: params.legalsLimit,
    signal: params.signal,
  });

  if (!legalsResult.success) {
    return {
      success: false,
      address_key,
      error: legalsResult.error,
      status: legalsResult.status,
    };
  }

  const { rows: legals } = legalsResult;
  const uniqueDocIds = [...new Set(legals.map((r) => r.document_id))].sort();

  const allMasterRows: AcrisMasterDeedRow[] = [];
  for (const documentId of uniqueDocIds) {
    const masterResult = await fetchAcrisMasterDeedsByDocumentId({
      documentId,
      signal: params.signal,
    });
    if (!masterResult.success) {
      return {
        success: false,
        address_key,
        error: `Master fetch failed for ${documentId}: ${masterResult.error}`,
        status: masterResult.status,
      };
    }
    allMasterRows.push(...masterResult.rows);
  }

  const masterByDocumentId = acrisMasterRowsByDocumentId(allMasterRows);
  const joined = joinAcrisLegalsWithMasterDeeds(legals, masterByDocumentId);

  const deeds = joined.map(joinedRowToTruthDeed).sort(sortDeedsNewestFirstStable);

  const latest_deed = deeds.length > 0 ? deeds[0]! : null;
  const has_multiple_deeds = deeds.length > 1;

  return {
    success: true,
    address_key,
    deeds,
    latest_deed,
    has_multiple_deeds,
  };
}
