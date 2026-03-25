/**
 * NYC street-level aggregates from BigQuery `us_nyc_street_pricing` (US only).
 */

import { getUSBigQueryClient } from "./bigquery-client";

export const US_NYC_STREET_PRICING_TABLE_REFERENCE = "streetiq-bigquery.streetiq_gold.us_nyc_street_pricing";

const STREET_TABLE = `\`${US_NYC_STREET_PRICING_TABLE_REFERENCE}\``;

export const US_NYC_STREET_PRICING_SQL_WHERE = "street_name = @street_name";

const NYC_QUERY_LOCATION = "EU";

const STREET_SELECT = `
  avg_street_price,
  avg_street_price_per_sqft,
  transaction_count
`.trim();

const STREET_QUERY = `
  SELECT ${STREET_SELECT}
  FROM ${STREET_TABLE}
  WHERE ${US_NYC_STREET_PRICING_SQL_WHERE}
  LIMIT 1
`.trim();

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

export type NycStreetPricingFields = {
  avg_street_price: number | null;
  avg_street_price_per_sqft: number | null;
  transaction_count: number | null;
};

/**
 * Exact match on `street_name` only. Returns null if no row.
 */
export async function queryUSNycStreetPricingByStreetName(streetName: string): Promise<NycStreetPricingFields | null> {
  const trimmed = streetName.trim();
  if (!trimmed) return null;

  const client = getUSBigQueryClient();
  const [rows] = await client.query({
    query: STREET_QUERY,
    params: { street_name: trimmed },
    location: NYC_QUERY_LOCATION,
  });
  const row = (rows as Record<string, unknown>[] | null | undefined)?.[0];
  if (!row) return null;

  return {
    avg_street_price: toNumberOrNull(row.avg_street_price),
    avg_street_price_per_sqft: toNumberOrNull(row.avg_street_price_per_sqft),
    transaction_count: toNumberOrNull(row.transaction_count),
  };
}

export function streetPricingQueryTemplate(): string {
  return STREET_QUERY;
}
