/**
 * NYC property card — precomputed BigQuery tables only (US).
 * Joins card output v5 with last-transaction engine v3 on `full_address`.
 */

import { coerceBigQueryDateToYyyyMmDd } from "./us-bq-date";
import { getUSBigQueryClient } from "./bigquery-client";
import type { USNYCApiTruthResponse } from "./us-property-response-contract";

export const US_NYC_CARD_OUTPUT_V5_REFERENCE = "streetiq-bigquery.streetiq_gold.us_nyc_card_output_v5";
export const US_NYC_LAST_TX_ENGINE_V3_REFERENCE = "streetiq-bigquery.streetiq_gold.us_nyc_last_transaction_engine_v3";

/**
 * Primary lookup predicate (card table) — one candidate per query.
 * Callers iterate {@link buildNycTruthLookupCandidates} in deterministic order until the first row matches.
 */
export const US_NYC_PRECOMPUTED_CARD_SQL_WHERE = "c.full_address = @address";

const CARD = `\`${US_NYC_CARD_OUTPUT_V5_REFERENCE}\``;
const ENGINE = `\`${US_NYC_LAST_TX_ENGINE_V3_REFERENCE}\``;

const NYC_PRECOMPUTED_LOCATION = "EU";

/** Template for debug; runtime matching is sequential equality on each generated candidate string. */
export const US_NYC_PRECOMPUTED_JOIN_QUERY = `
SELECT
  c.full_address AS full_address,
  c.badge_1 AS badge_1,
  c.badge_2 AS badge_2,
  c.badge_3 AS badge_3,
  c.badge_4 AS badge_4,
  c.estimated_value AS estimated_value,
  c.estimated_value_subtext AS estimated_value_subtext,
  c.price_per_sqft_text AS price_per_sqft_text,
  c.final_match_level AS final_match_level,
  c.building_type AS building_type,
  c.unit_count AS unit_count,
  e.final_last_transaction_text AS final_last_transaction_text,
  e.final_transaction_match_level AS final_transaction_match_level
FROM ${CARD} c
LEFT JOIN ${ENGINE} e
  ON c.full_address = e.full_address
WHERE c.full_address = @address
LIMIT 1
`.trim();

function toNumberOrNull(v: unknown): number | null {
  if (v == null || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "bigint") {
    const x = Number(v);
    return Number.isFinite(x) ? x : null;
  }
  const x = Number(String(v).replace(/,/g, ""));
  return Number.isFinite(x) ? x : null;
}

function toStringOrNull(v: unknown): string | null {
  if (v == null || v === "") return null;
  return String(v);
}

const MULTIFAMILY_BUILDING_TYPES = new Set(["large_multifamily", "small_multifamily", "mixed_use"]);

/** Normalize `building_type` (handles hyphen, space, underscore). */
function normalizeNycBuildingTypeKey(raw: string): string {
  return raw.replace(/[-\s]+/g, "_").toLowerCase().trim();
}

/** NYC `us_nyc_card_output_v5.building_type` — condo / co-op / apartment inventory (must ask for unit before unit-specific results). */
const UNIT_INVENTORY_BUILDING_TYPES = new Set(["condo", "co_op", "coop", "apartment"]);

function addressSuggestsUnitInventory(line: string): boolean {
  if (!line.trim()) return false;
  return /\b(CONDO|CO-OP|COOP|CONDOMINIUM|APARTMENT\s+BUILDING|APT\s+BUILDING|COOPERATIVE)\b/i.test(line);
}

function rowBadgesSuggestUnits(row: Record<string, unknown>): boolean {
  const parts = [row.badge_1, row.badge_2, row.badge_3, row.badge_4].map((x) => String(x ?? ""));
  return parts.some((p) => /\b(CONDO|CO-OP|COOP|MULTI|APARTMENT|RENTAL|UNIT|COOPERATIVE)\b/i.test(p));
}

/**
 * Whether to require apartment/lot before treating last sale as unit-specific (deterministic rules).
 */
export function computeNycNeedsUnitPrompt(
  row: Record<string, unknown>,
  opts: { addressLine: string; candidatesCount: number }
): boolean {
  const bt = normalizeNycBuildingTypeKey(String(row.building_type ?? ""));
  if (bt === "single_family") return false;
  if (bt === "two_family") return false;

  if (UNIT_INVENTORY_BUILDING_TYPES.has(bt)) return true;

  const uc = toNumberOrNull(row.unit_count);
  if (uc != null && uc > 1) return true;

  if (MULTIFAMILY_BUILDING_TYPES.has(bt)) return true;

  if (addressSuggestsUnitInventory(opts.addressLine)) return true;

  if (rowBadgesSuggestUnits(row)) return true;

  if (opts.candidatesCount > 1) return true;

  return false;
}

function transactionLevelIsSimilarOnly(row: Record<string, unknown>): boolean {
  return String(row.final_transaction_match_level ?? "").toLowerCase().includes("similar");
}

/** First USD amount in free text (for engine `final_last_transaction_text`). */
export function parseFirstUsdAmountFromText(text: string | null | undefined): number | null {
  if (!text?.trim()) return null;
  const m = text.match(/\$[\d,]+(?:\.\d{2})?/);
  if (!m) return null;
  const n = Number(m[0].replace(/[$,]/g, ""));
  return Number.isFinite(n) && n > 0 ? n : null;
}

/** First YYYY-MM-DD or common date substring in text. */
export function parseSaleDateFromTransactionText(text: string | null | undefined): string | null {
  if (!text?.trim()) return null;
  const iso = text.match(/\b(\d{4}-\d{2}-\d{2})\b/);
  if (iso) return iso[1];
  const d = coerceBigQueryDateToYyyyMmDd(text);
  return d;
}

function classifyLastSaleMatchKind(
  row: Record<string, unknown>,
  latestSalePrice: number | null
): "exact" | "similar" | "none" {
  const lvl = String(row.final_transaction_match_level ?? "").toLowerCase();
  if (lvl.includes("similar")) return "similar";
  if (lvl.includes("exact")) return "exact";
  if (latestSalePrice != null) return "exact";
  return "none";
}

/** Server-only: concise JSON line after a precomputed row match (no response/UI change). */
function logNycPrecomputedLookup(
  row: Record<string, unknown>,
  mapped: Omit<USNYCApiTruthResponse, "success" | "message">
): void {
  if (process.env.NODE_ENV === "test") return;
  try {
    const payload = {
      matched_full_address: toStringOrNull(row.full_address),
      nyc_final_match_level: toStringOrNull(row.final_match_level) ?? mapped.nyc_final_match_level,
      nyc_final_transaction_match_level:
        toStringOrNull(row.final_transaction_match_level) ?? mapped.nyc_final_transaction_match_level,
      estimated_value_source: "us_nyc_card_output_v5.estimated_value",
      last_sale_match_kind: classifyLastSaleMatchKind(row, mapped.latest_sale_price),
    };
    console.log("[NYC precomputed]", JSON.stringify(payload));
  } catch {
    /* ignore logging failures */
  }
}

/** Parse a numeric $/sq ft from precomputed text when present. */
export function parsePricePerSqftNumber(text: string | null | undefined): number | null {
  if (!text?.trim()) return null;
  const lower = text.toLowerCase();
  const slash = lower.match(/([\d,]+(?:\.\d+)?)\s*(?:\/|per)\s*sq/i);
  if (slash) {
    const n = Number(slash[1].replace(/,/g, ""));
    return Number.isFinite(n) && n > 0 ? n : null;
  }
  const any = text.match(/([\d,]+(?:\.\d+)?)/);
  if (any) {
    const n = Number(any[1].replace(/,/g, ""));
    return Number.isFinite(n) && n > 0 ? n : null;
  }
  return null;
}

export type MapPrecomputedJoinOptions = {
  /** Building match before user supplied unit/lot — withhold similar-only sales from top-level until unit is known. */
  pendingUnitPrompt?: boolean;
};

export function mapPrecomputedJoinRowToUSNYCApiTruthResponse(
  row: Record<string, unknown>,
  options?: MapPrecomputedJoinOptions
): Omit<USNYCApiTruthResponse, "success" | "message"> {
  const fullAddress = toStringOrNull(row.full_address);
  let txText = toStringOrNull(row.final_last_transaction_text);
  let priceFromText = parseFirstUsdAmountFromText(txText);
  let dateFromText = parseSaleDateFromTransactionText(txText);
  const estimated = toNumberOrNull(row.estimated_value);
  const ppsfText = toStringOrNull(row.price_per_sqft_text);
  const ppsfNum = parsePricePerSqftNumber(ppsfText);

  let latestSalePrice = priceFromText;
  let lastTxUnavailable: string | null = null;

  if (options?.pendingUnitPrompt === true && transactionLevelIsSimilarOnly(row)) {
    latestSalePrice = null;
    dateFromText = null;
    txText = null;
    lastTxUnavailable = "similar_property_not_exact_unit";
  }

  const mapped: Omit<USNYCApiTruthResponse, "success" | "message"> = {
    has_truth_property_row: true,
    estimated_value: estimated,
    latest_sale_price: latestSalePrice,
    latest_sale_date: dateFromText,
    latest_sale_total_units: null,
    avg_street_price: null,
    avg_street_price_per_sqft: null,
    transaction_count: null,
    price_per_sqft: ppsfNum,
    sales_address: fullAddress,
    pluto_address: fullAddress,
    street_name: null,
    unit_lookup_status: "not_requested",
    unit_or_lot_submitted: null,
    nyc_precomputed_card: true,
    nyc_card_full_address: fullAddress,
    nyc_card_badge_1: toStringOrNull(row.badge_1),
    nyc_card_badge_2: toStringOrNull(row.badge_2),
    nyc_card_badge_3: toStringOrNull(row.badge_3),
    nyc_card_badge_4: toStringOrNull(row.badge_4),
    nyc_estimated_value_subtext: toStringOrNull(row.estimated_value_subtext),
    nyc_price_per_sqft_text: ppsfText,
    nyc_final_match_level: toStringOrNull(row.final_match_level),
    nyc_final_last_transaction_text: txText,
    nyc_final_transaction_match_level: toStringOrNull(row.final_transaction_match_level),
    nyc_pending_unit_prompt: options?.pendingUnitPrompt === true,
    nyc_last_transaction_unavailable_reason: lastTxUnavailable,
  };
  logNycPrecomputedLookup(row, mapped);
  return mapped;
}

export async function queryPrecomputedNycCardJoinRow(
  client: ReturnType<typeof getUSBigQueryClient>,
  address: string
): Promise<{ row: Record<string, unknown> | null; rowsReturned: number }> {
  const trimmed = address.trim();
  if (!trimmed) return { row: null, rowsReturned: 0 };
  const [rows] = await client.query({
    query: US_NYC_PRECOMPUTED_JOIN_QUERY,
    params: { address: trimmed },
    location: NYC_PRECOMPUTED_LOCATION,
  });
  const list = (rows as Record<string, unknown>[] | null | undefined) ?? [];
  return { row: list[0] ?? null, rowsReturned: list.length };
}
