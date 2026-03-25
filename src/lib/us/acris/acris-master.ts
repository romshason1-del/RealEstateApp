/**
 * ACRIS Real Property Master — DEED rows with positive document amount (US only).
 */

import { acrisDefaultLimitParam, acrisEscapeSoqlString, acrisSocrataGet } from "./acris-client";
import { acrisMasterResourcePath } from "./acris-config";

export type AcrisMasterDeedRow = {
  document_id: string;
  doc_type: string;
  document_date: string | null;
  document_amt: number;
};

type SocrataMasterRaw = {
  document_id?: string;
  doc_type?: string;
  document_date?: string;
  document_amt?: string | number;
};

function parseDocumentAmt(raw: string | number | undefined): number | null {
  if (raw === undefined || raw === null) return null;
  if (typeof raw === "number") return Number.isFinite(raw) && raw > 0 ? raw : null;
  const s = String(raw).trim().replace(/,/g, "");
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function normalizeDeedRow(raw: SocrataMasterRaw): AcrisMasterDeedRow | null {
  const document_id = String(raw.document_id ?? "").trim();
  const doc_type = String(raw.doc_type ?? "").trim();
  if (!document_id || doc_type.toUpperCase() !== "DEED") return null;
  const document_amt = parseDocumentAmt(raw.document_amt);
  if (document_amt == null) return null;
  const document_date =
    raw.document_date != null && String(raw.document_date).trim() ? String(raw.document_date).trim() : null;
  return {
    document_id,
    doc_type,
    document_date,
    document_amt,
  };
}

export type FetchAcrisMasterDeedsResult =
  | { success: true; rows: AcrisMasterDeedRow[] }
  | { success: false; error: string; status?: number };

/**
 * Master rows for one `document_id` where `doc_type` is DEED and `document_amt` > 0 (enforced in SoQL and again when parsing).
 */
export async function fetchAcrisMasterDeedsByDocumentId(params: {
  documentId: string;
  limit?: number;
  signal?: AbortSignal;
}): Promise<FetchAcrisMasterDeedsResult> {
  const id = params.documentId.trim();
  if (!id) {
    return { success: false, error: "documentId is required" };
  }

  const esc = acrisEscapeSoqlString(id);
  const where = `document_id = '${esc}' AND doc_type = 'DEED' AND document_amt > 0`;

  const res = await acrisSocrataGet<SocrataMasterRaw[]>(
    acrisMasterResourcePath(),
    {
      $where: where,
      $limit: acrisDefaultLimitParam(params.limit ?? 100),
      $order: "document_date DESC",
    },
    { signal: params.signal }
  );

  if (!res.ok) {
    return { success: false, error: res.error, status: res.status };
  }

  const data = Array.isArray(res.data) ? res.data : [];
  const rows: AcrisMasterDeedRow[] = [];
  for (const raw of data) {
    const row = normalizeDeedRow(raw);
    if (row) rows.push(row);
  }
  return { success: true, rows };
}
