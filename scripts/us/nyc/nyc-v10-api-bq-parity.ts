/**
 * NYC v10: verify `/api/us/nyc-app-output` `row` matches BigQuery for sampled rows (12 cases: 3×4 ui_card_type buckets).
 *
 * Usage:
 *   npx tsx scripts/us/nyc/nyc-v10-api-bq-parity.ts
 *   VALIDATE_API_BASE=http://localhost:3000 npx tsx scripts/us/nyc/nyc-v10-api-bq-parity.ts
 *
 * US only. Requires BigQuery credentials. HTTP step is optional (set VALIDATE_API_BASE).
 */

import { parseUSAddressFromFullString } from "../../../src/lib/address-parse";
import { getUSBigQueryClient } from "../../../src/lib/us/bigquery-client";
import { isUSBigQueryConfigured } from "../../../src/lib/us/us-bigquery";
import { getNycAppOutputTableReference } from "../../../src/lib/us/us-nyc-app-output-constants";
import { adaptNycPropertyUiProductionRowToPropertyPayload } from "../../../src/lib/us/us-nyc-property-ui-production-adapter";
import { queryNycPropertyUiProductionV10Row } from "../../../src/lib/us/us-nyc-property-ui-production-query";
import { NYC_PROPERTY_UI_PRODUCTION_V10_BQ_COLUMNS } from "../../../src/lib/us/us-nyc-property-ui-production-schema";

const TABLE = getNycAppOutputTableReference();
const API_BASE = (process.env.VALIDATE_API_BASE ?? "").trim().replace(/\/$/, "");

const CARD_TYPES = [
  "FULL_DEAL_CARD",
  "WEAK_DEAL_CARD",
  "PROPERTY_INSIGHT_CARD",
  "NON_RESIDENTIAL_BLOCKED",
];

type Case = { lookup_address: string; normalized_unit_number: string | null; ui_card_type: string };

function normCell(v: unknown): string {
  if (v === undefined || v === null) return "null";
  if (typeof v === "object" && v !== null && "value" in v) {
    return normCell((v as { value: unknown }).value);
  }
  if (typeof v === "bigint") return String(v);
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function cellsEqual(a: unknown, b: unknown): boolean {
  return normCell(a) === normCell(b);
}

async function fetchCases(client: ReturnType<typeof getUSBigQueryClient>): Promise<Case[]> {
  const typesSql = CARD_TYPES.map((t) => `'${t}'`).join(", ");
  const sql = `
    SELECT lookup_address, normalized_unit_number, ui_card_type
    FROM \`${TABLE}\`
    WHERE REPLACE(UPPER(TRIM(COALESCE(ui_card_type, ''))), ' ', '_') IN (${typesSql})
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY REPLACE(UPPER(TRIM(COALESCE(ui_card_type, ''))), ' ', '_')
      ORDER BY lookup_address, normalized_unit_number
    ) <= 3
    ORDER BY ui_card_type, lookup_address, normalized_unit_number
  `;
  const [rows] = await client.query({ query: sql, location: "US" });
  return (rows as Case[]).map((r) => ({
    lookup_address: String(r.lookup_address ?? "").trim(),
    normalized_unit_number:
      r.normalized_unit_number == null || String(r.normalized_unit_number).trim() === ""
        ? null
        : String(r.normalized_unit_number).trim(),
    ui_card_type: String(r.ui_card_type ?? ""),
  }));
}

async function fetchApiRow(address: string, unit: string | null): Promise<Record<string, unknown> | null> {
  if (!API_BASE) return null;
  const u = new URL("/api/us/nyc-app-output", API_BASE);
  u.searchParams.set("address", address);
  if (unit) u.searchParams.set("unit_or_lot", unit);
  const res = await fetch(u.toString(), { headers: { Accept: "application/json" } });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} ${await res.text()}`);
  }
  const body = (await res.json()) as { row?: Record<string, unknown> | null };
  return body.row ?? null;
}

function compareBqToApiRow(
  bq: Record<string, unknown>,
  apiRow: Record<string, unknown> | null
): { match: boolean; mismatches: string[] } {
  const mismatches: string[] = [];
  if (!apiRow) {
    return { match: false, mismatches: ["(no row in API response)"] };
  }
  for (const col of NYC_PROPERTY_UI_PRODUCTION_V10_BQ_COLUMNS) {
    if (!cellsEqual(bq[col], apiRow[col])) {
      mismatches.push(
        `${col}: BQ=${normCell(bq[col])} API=${normCell(apiRow[col])}`
      );
    }
  }
  return { match: mismatches.length === 0, mismatches };
}

async function main() {
  if (!isUSBigQueryConfigured()) {
    console.error("BigQuery not configured.");
    process.exit(1);
  }
  const client = getUSBigQueryClient();
  const cases = await fetchCases(client);
  if (cases.length === 0) {
    console.error("No rows returned for ui_card_type filter — check table / types.");
    process.exit(1);
  }

  console.log(JSON.stringify({ table: TABLE, case_count: cases.length, api_base: API_BASE || null }, null, 2));
  console.log("---\n");

  let anyFail = false;

  for (let i = 0; i < cases.length; i++) {
    const c = cases[i]!;
    const unitOrLot = c.normalized_unit_number;
    const { row: bqRow } = await queryNycPropertyUiProductionV10Row(client, c.lookup_address, unitOrLot);

    const parsed = parseUSAddressFromFullString(c.lookup_address);
    const payload = adaptNycPropertyUiProductionRowToPropertyPayload(
      bqRow,
      { city: parsed.city, street: parsed.street, houseNumber: parsed.houseNumber },
      { unitOrLotSubmitted: unitOrLot }
    );

    let apiRowFromHttp: Record<string, unknown> | null = null;
    let httpError: string | null = null;
    if (API_BASE) {
      try {
        apiRowFromHttp = await fetchApiRow(c.lookup_address, unitOrLot);
      } catch (e) {
        httpError = e instanceof Error ? e.message : String(e);
      }
    }

    const productionSlice = Object.fromEntries(
      NYC_PROPERTY_UI_PRODUCTION_V10_BQ_COLUMNS.map((col) => [col, payload[col]])
    ) as Record<string, unknown>;

    const adapterVsBq =
      bqRow == null
        ? { match: false, mismatches: ["(no BQ row from query path)"] }
        : compareBqToApiRow(bqRow, productionSlice);

    const httpVsBq =
      bqRow == null
        ? { match: false, mismatches: ["(no BQ row)"] }
        : httpError != null
          ? { match: false, mismatches: [httpError] }
          : !API_BASE
            ? { match: true, mismatches: [] }
            : apiRowFromHttp
              ? compareBqToApiRow(bqRow, apiRowFromHttp)
              : { match: false, mismatches: ["(HTTP response had no row)"] };

    const summary = {
      index: i + 1,
      address: c.lookup_address,
      unit_or_lot: unitOrLot,
      ui_card_type: c.ui_card_type,
      bq_row_found: bqRow != null,
      payload_production_columns_match_bq: adapterVsBq.match,
      payload_field_gaps: adapterVsBq.mismatches,
      http_row_match_bq: API_BASE ? httpVsBq.match : null,
      http_field_gaps: API_BASE ? httpVsBq.mismatches : null,
    };

    if (!adapterVsBq.match || (API_BASE && !httpVsBq.match)) anyFail = true;

    console.log("### Case", summary.index);
    console.log(JSON.stringify(summary, null, 2));
    console.log("--- FULL API-shaped payload (adapter, same as route minus `row` envelope) ---");
    console.log(JSON.stringify(payload, null, 2));
    if (bqRow) {
      console.log("--- BQ row (reference) ---");
      console.log(JSON.stringify(bqRow, (_, v) => (typeof v === "bigint" ? v.toString() : v), 2));
    }
    console.log("\n");
  }

  process.exit(anyFail ? 2 : 0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
