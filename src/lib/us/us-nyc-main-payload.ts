/**
 * Maps `/api/us/property-value` NYC truth JSON into the shape the main property-value UI expects.
 * US-only — do not use for France.
 */

import { coerceBigQueryDateToYyyyMmDd } from "./us-bq-date";

function num(v: unknown): number | null {
  if (v == null || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  const x = Number(String(v));
  return Number.isFinite(x) ? x : null;
}

function str(v: unknown): string | null {
  if (v == null || v === "") return null;
  return String(v);
}

/**
 * When `success` is true, merges truth fields + `address` + `property_result` for PropertyValueCard.
 * Preserves `us_nyc_debug` if present.
 */
export function adaptUsNycTruthJsonForMainPropertyValueRoute(
  us: Record<string, unknown>,
  ctx: { city: string; street: string; houseNumber: string }
): Record<string, unknown> {
  const city = ctx.city.trim() || "—";
  const street = ctx.street.trim() || "—";
  const houseNumber = ctx.houseNumber.trim() || "—";

  const estimated_value = num(us.estimated_value);
  const latest_sale_price = num(us.latest_sale_price);
  const latest_sale_date = coerceBigQueryDateToYyyyMmDd(us.latest_sale_date);
  const avg_street_price = num(us.avg_street_price);
  const avg_street_price_per_sqft = num(us.avg_street_price_per_sqft);
  const transaction_count = num(us.transaction_count);
  const price_per_sqft = num(us.price_per_sqft);
  const sales_address = str(us.sales_address);
  const pluto_address = str(us.pluto_address);
  const street_name = str(us.street_name);

  const lastAmt = latest_sale_price != null && latest_sale_price > 0 ? latest_sale_price : 0;

  const property_result = {
    exact_value: estimated_value,
    exact_value_message:
      estimated_value != null && estimated_value > 0
        ? null
        : lastAmt > 0
          ? null
          : "No separate estimate in NYC truth table for this row",
    value_level: "property-level" as const,
    last_transaction: {
      amount: lastAmt,
      date: latest_sale_date,
      message: lastAmt > 0 ? undefined : "No recorded sale amount in NYC truth table",
    },
    street_average: avg_street_price,
    street_average_message: avg_street_price != null && avg_street_price > 0 ? null : "No street average in NYC truth table",
    livability_rating: "FAIR" as const,
  };

  const out: Record<string, unknown> = {
    success: us.success === true,
    data_source: "us_nyc_truth",
    message: null,
    address: { city, street, house_number: houseNumber },
    estimated_value,
    latest_sale_price,
    latest_sale_date,
    avg_street_price,
    avg_street_price_per_sqft,
    transaction_count,
    price_per_sqft,
    sales_address,
    pluto_address,
    street_name,
    property_result,
  };

  if (lastAmt > 0 && latest_sale_date) {
    out.last_sale = { price: lastAmt, date: latest_sale_date };
  }
  if (us.us_nyc_debug != null) {
    out.us_nyc_debug = us.us_nyc_debug;
  }

  return out;
}
