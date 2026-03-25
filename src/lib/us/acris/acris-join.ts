/**
 * Deterministic join: Legals × Master (DEED, amount > 0) on `document_id`. US only.
 */

import type { AcrisLegalRow } from "./acris-legals";
import type { AcrisMasterDeedRow } from "./acris-master";

/** One row per (legal line × matching master deed line), sorted for stable output. */
export type AcrisJoinedRow = {
  document_id: string;
  borough: number;
  block: number;
  lot: number;
  street_number: string;
  street_name: string;
  property_type: string;
  document_date: string | null;
  document_amt: number;
  doc_type: string;
};

function sortKey(r: AcrisJoinedRow): string {
  const d = r.document_date ?? "";
  return `${r.document_id}\t${d}\t${r.borough}\t${r.block}\t${r.lot}\t${r.street_number}\t${r.street_name}`;
}

/**
 * Join legal rows to master DEED rows by `document_id`.
 * - Each legal row is paired with every matching master row in `masterByDocumentId.get(document_id)`.
 * - Rows with no master match are skipped (no guessing).
 * - Output order: lexicographic by `sortKey` for repeatability.
 */
export function joinAcrisLegalsWithMasterDeeds(
  legals: AcrisLegalRow[],
  masterByDocumentId: ReadonlyMap<string, readonly AcrisMasterDeedRow[]>
): AcrisJoinedRow[] {
  const out: AcrisJoinedRow[] = [];

  for (const L of legals) {
    const masters = masterByDocumentId.get(L.document_id);
    if (!masters?.length) continue;

    const street_number = L.street_number ?? "";
    const street_name = L.street_name ?? "";
    const property_type = L.property_type ?? "";

    for (const M of masters) {
      out.push({
        document_id: L.document_id,
        borough: L.borough,
        block: L.block,
        lot: L.lot,
        street_number,
        street_name,
        property_type,
        document_date: M.document_date,
        document_amt: M.document_amt,
        doc_type: M.doc_type,
      });
    }
  }

  out.sort((a, b) => (sortKey(a) < sortKey(b) ? -1 : sortKey(a) > sortKey(b) ? 1 : 0));
  return out;
}

/** Build a map from `document_id` to deed rows (multiple rows per id if Socrata returns duplicates). */
export function acrisMasterRowsByDocumentId(rows: readonly AcrisMasterDeedRow[]): Map<string, AcrisMasterDeedRow[]> {
  const map = new Map<string, AcrisMasterDeedRow[]>();
  for (const r of rows) {
    const list = map.get(r.document_id);
    if (list) list.push(r);
    else map.set(r.document_id, [r]);
  }
  return map;
}
