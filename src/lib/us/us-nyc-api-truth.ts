/**
 * NYC property rows from BigQuery source of truth only.
 * Table: streetiq-bigquery.streetiq_gold.us_nyc_api_truth
 */

import { getBigQueryClient } from "@/lib/bigquery-client";
import type { USNYCApiTruthResponse } from "./us-property-response-contract";

const NYC_TRUTH_TABLE = "`streetiq-bigquery.streetiq_gold.us_nyc_api_truth`";

function nycQueryLocation(): string {
  return process.env.US_NYC_BIGQUERY_LOCATION?.trim() || "US";
}

function toNumberOrNull(v: unknown): number | null {
  if (v == null || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "bigint") {
    const x = Number(v);
    return Number.isFinite(x) ? x : null;
  }
  if (typeof v === "object" && v !== null && "toString" in v) {
    const x = Number(String(v));
    return Number.isFinite(x) ? x : null;
  }
  const x = Number(String(v));
  return Number.isFinite(x) ? x : null;
}

function toStringOrNull(v: unknown): string | null {
  if (v == null || v === "") return null;
  return String(v);
}

function toDateStringOrNull(v: unknown): string | null {
  if (v == null || v === "") return null;
  if (v instanceof Date && !Number.isNaN(v.getTime())) {
    return v.toISOString().slice(0, 10);
  }
  const s = String(v).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  return s.length > 0 ? s : null;
}

function mapTruthRow(row: Record<string, unknown>): Omit<USNYCApiTruthResponse, "success" | "message"> {
  return {
    estimated_value: toNumberOrNull(row.estimated_value),
    latest_sale_price: toNumberOrNull(row.latest_sale_price),
    latest_sale_date: toDateStringOrNull(row.latest_sale_date),
    avg_street_price: toNumberOrNull(row.avg_street_price),
    avg_street_price_per_sqft: toNumberOrNull(row.avg_street_price_per_sqft),
    transaction_count: toNumberOrNull(row.transaction_count),
    price_per_sqft: toNumberOrNull(row.price_per_sqft),
    sales_address: toStringOrNull(row.sales_address),
    pluto_address: toStringOrNull(row.pluto_address),
    street_name: toStringOrNull(row.street_name),
  };
}

const SELECT_FIELDS = `
  estimated_value,
  latest_sale_price,
  latest_sale_date,
  avg_street_price,
  avg_street_price_per_sqft,
  transaction_count,
  price_per_sqft,
  sales_address,
  pluto_address,
  street_name
`.trim();

/**
 * Exact match: input must equal pluto_address or sales_address (after caller trim only).
 */
export async function queryUSNYCApiTruthByAddress(address: string): Promise<USNYCApiTruthResponse> {
  const client = getBigQueryClient();
  const location = nycQueryLocation();
  const query = `
    SELECT ${SELECT_FIELDS}
    FROM ${NYC_TRUTH_TABLE}
    WHERE pluto_address = @address OR sales_address = @address
    LIMIT 1
  `;
  const params = { address };
  const [rows] = await client.query({ query, params, location });
  const row = (rows as Record<string, unknown>[] | null | undefined)?.[0];
  if (!row) {
    return {
      success: true,
      message: null,
      estimated_value: null,
      latest_sale_price: null,
      latest_sale_date: null,
      avg_street_price: null,
      avg_street_price_per_sqft: null,
      transaction_count: null,
      price_per_sqft: null,
      sales_address: null,
      pluto_address: null,
      street_name: null,
    };
  }
  return {
    success: true,
    message: null,
    ...mapTruthRow(row),
  };
}
