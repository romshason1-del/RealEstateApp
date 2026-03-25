/**
 * TEMPORARY debug harness for NYC ACRIS (US only). Not for production or UI.
 * Run from Node/tsx by importing and calling `runAcrisRoyceStreetDebugTest()`.
 */

import { fetchAcrisLegalsByStreetAddress } from "./acris-legals";
import { fetchAcrisMasterDeedsByDocumentId, type AcrisMasterDeedRow } from "./acris-master";
import { acrisMasterRowsByDocumentId, joinAcrisLegalsWithMasterDeeds } from "./acris-join";

const DEBUG_STREET_NUMBER = "2154";
const DEBUG_STREET_NAME = "ROYCE STREET";

/**
 * Hardcoded address: 2154 ROYCE STREET → legals → unique document_ids → master DEED rows → join → log.
 */
export async function runAcrisRoyceStreetDebugTest(): Promise<void> {
  console.log("[acris-debug] Legals lookup:", DEBUG_STREET_NUMBER, DEBUG_STREET_NAME);

  const legalsResult = await fetchAcrisLegalsByStreetAddress({
    streetNumber: DEBUG_STREET_NUMBER,
    streetName: DEBUG_STREET_NAME,
  });

  if (!legalsResult.success) {
    console.error("[acris-debug] Legals failed:", legalsResult.error, legalsResult.status);
    return;
  }

  const { rows: legals } = legalsResult;
  console.log("[acris-debug] Legals row count:", legals.length);

  const uniqueDocIds = [...new Set(legals.map((r) => r.document_id))].sort();
  console.log("[acris-debug] Unique document_id count:", uniqueDocIds.length);

  const allMasterRows: AcrisMasterDeedRow[] = [];

  for (const documentId of uniqueDocIds) {
    const masterResult = await fetchAcrisMasterDeedsByDocumentId({ documentId });
    if (!masterResult.success) {
      console.error("[acris-debug] Master failed for", documentId, masterResult.error, masterResult.status);
      continue;
    }
    console.log("[acris-debug] Master DEED+amt rows for", documentId, ":", masterResult.rows.length);
    allMasterRows.push(...masterResult.rows);
  }

  const masterByDocumentId = acrisMasterRowsByDocumentId(allMasterRows);
  const joined = joinAcrisLegalsWithMasterDeeds(legals, masterByDocumentId);

  console.log("[acris-debug] Final joined row count:", joined.length);
  console.log("[acris-debug] Final joined rows:", JSON.stringify(joined, null, 2));
}
