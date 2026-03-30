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
export function normalizeNycBuildingTypeKey(raw: string): string {
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
      nyc_final_match_level: mapped.nyc_final_match_level ?? toStringOrNull(row.final_match_level),
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
  /** When set (e.g. street fallback), overrides card `final_match_level` in the API response. */
  overrideFinalMatchLevel?: string | null;
};

export function mapPrecomputedJoinRowToUSNYCApiTruthResponse(
  row: Record<string, unknown>,
  options?: MapPrecomputedJoinOptions
): Omit<USNYCApiTruthResponse, "success" | "message"> {
  const fullAddress = toStringOrNull(row.full_address);
  let txText = toStringOrNull(row.final_last_transaction_text);
  let priceFromText = parseFirstUsdAmountFromText(txText);
  let dateFromText = parseSaleDateFromTransactionText(txText);
  let estimated: number | null = toNumberOrNull(row.estimated_value);
  const ppsfTextRaw = toStringOrNull(row.price_per_sqft_text);
  let ppsfText: string | null = ppsfTextRaw;
  let ppsfNum: number | null = parsePricePerSqftNumber(ppsfTextRaw);
  let estSubtext: string | null = toStringOrNull(row.estimated_value_subtext);

  let latestSalePrice = priceFromText;
  let lastTxUnavailable: string | null = null;

  if (options?.pendingUnitPrompt === true) {
    // Suppress ALL unit-sensitive display values until the user selects a specific unit/apartment.
    // Showing building-level values (estimated value, last sale, $/sqft, subtext) as if they
    // belong to a particular apartment before unit selection would be misleading.
    estimated = null;
    estSubtext = null;
    latestSalePrice = null;
    dateFromText = null;
    txText = null;
    ppsfNum = null;
    ppsfText = null;
    lastTxUnavailable = "pending_unit_selection";
  } else if (transactionLevelIsSimilarOnly(row)) {
    // Non-pending: suppress similar-only sale references at building level.
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
    nyc_estimated_value_subtext: estSubtext,
    nyc_price_per_sqft_text: ppsfText,
    nyc_final_match_level: options?.overrideFinalMatchLevel ?? toStringOrNull(row.final_match_level),
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

/** Template for debug; runtime WHERE clause is built from LIKE patterns. */
export const US_NYC_STREET_FALLBACK_JOIN_QUERY = `
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
WHERE __STREET_FALLBACK_WHERE__
LIMIT 200
`.trim();

/** Leading house number (first token) for distance scoring vs fallback rows. */
function parseLeadingHouseNumberFromFullAddress(fa: string): number | null {
  const m = fa.trim().match(/^(\d{1,7})\b/);
  if (!m) return null;
  const n = Number(m[1]);
  return Number.isFinite(n) ? n : null;
}

/**
 * Comparable street segment (house stripped) for Manhattan-style + Central Park addresses.
 * Used to prefer candidates on the same normalized street before house-number distance.
 */
export function normalizeManhattanStreetPortion(line: string): string {
  let t = line.split(",")[0]!.trim().toUpperCase();
  t = t.replace(/^(\d+[A-Z]?)\s+/, "");
  t = t.replace(/\s+/g, " ");
  t = t.replace(/\bCENTRAL\s+PARK\s+WEST\b/, "CENTRAL PARK W");
  t = t.replace(/\bCENTRAL\s+PARK\s+EAST\b/, "CENTRAL PARK E");
  t = t.replace(/\bWEST\b/g, "W");
  t = t.replace(/\bEAST\b/g, "E");
  t = t.replace(/\bNORTH\b/g, "N");
  t = t.replace(/\bSOUTH\b/g, "S");
  t = t.replace(/\b(\d{1,3})(ST|ND|RD|TH)\b/g, "$1");
  t = t.replace(/\bSTREET\b/g, "ST");
  t = t.replace(/\bAVENUE\b/g, "AVE");
  t = t.replace(/\bBOULEVARD\b/g, "BLVD");
  return t.trim();
}

/** Higher = more desirable for residential / multifamily fallback. Vacant / unknown sink to bottom. */
function nycBuildingTypeResidentialTier(raw: string): number {
  const b = normalizeNycBuildingTypeKey(raw);
  if (!b || b === "unknown") return 5;
  if (b.includes("vacant")) return 0;
  if (["large_multifamily", "small_multifamily", "mixed_use"].includes(b)) return 100;
  if (["condo", "co_op", "coop", "apartment"].includes(b)) return 95;
  if (["single_family", "two_family"].includes(b)) return 50;
  return 25;
}

/**
 * Max |requested − candidate| house number for same-normalized-street street_fallback.
 * Above this: reject (e.g. 245 vs 210 at dist 35).
 */
export const NYC_FALLBACK_MAX_HOUSE_DIST_SAME_STREET = 10;

/**
 * Stricter cap when the pool is cross-street best-effort (no same-street rows).
 */
export const NYC_FALLBACK_MAX_HOUSE_DIST_CROSS_STREET = 5;

export type NycFallbackRankResult = {
  row: Record<string, unknown> | null;
  fallbackType: "building_fallback" | "street_fallback" | null;
  scoreReason: string;
  sameStreetPool: boolean;
  /** Parsed distance when both sides have a leading house number; else null. */
  houseDistance: number | null;
};

type ScoredFallback = {
  row: Record<string, unknown>;
  sameStreet: boolean;
  houseDist: number;
  tier: number;
  uc: number;
};

function passesHouseDistanceGate(
  s: ScoredFallback,
  sameStreetPool: boolean,
  requestedHouseNumber: number | null
): boolean {
  if (requestedHouseNumber == null) return true;
  if (s.houseDist >= 9999990) return true;
  const max = sameStreetPool ? NYC_FALLBACK_MAX_HOUSE_DIST_SAME_STREET : NYC_FALLBACK_MAX_HOUSE_DIST_CROSS_STREET;
  return s.houseDist <= max;
}

/**
 * Rank fallback rows: same normalized street first, then closest house number, then residential tier, then unit_count.
 * Drops candidates whose house-number distance exceeds NYC caps (no misleading far-away buildings).
 */
export function rankNycFallbackRows(
  rows: Record<string, unknown>[],
  normalizedAddressLine: string,
  requestedHouseNumber: number | null
): NycFallbackRankResult {
  if (rows.length === 0) {
    return { row: null, fallbackType: null, scoreReason: "no_rows", sameStreetPool: false, houseDistance: null };
  }

  const searchStreet = normalizeManhattanStreetPortion(normalizedAddressLine);
  const scored: ScoredFallback[] = rows.map((row) => {
    const fa = String(row.full_address ?? "");
    const candStreet = normalizeManhattanStreetPortion(fa);
    const sameStreet = searchStreet.length > 0 && candStreet === searchStreet;
    const rh = parseLeadingHouseNumberFromFullAddress(fa);
    const houseDist =
      requestedHouseNumber != null && rh != null ? Math.abs(requestedHouseNumber - rh) : 9999999;
    const tier = nycBuildingTypeResidentialTier(String(row.building_type ?? ""));
    const uc = toNumberOrNull(row.unit_count) ?? 0;
    return { row, sameStreet, houseDist, tier, uc };
  });

  const sameStreetRows = scored.filter((s) => s.sameStreet);
  const pool = sameStreetRows.length > 0 ? sameStreetRows : scored;
  const sameStreetPool = sameStreetRows.length > 0;

  const maxAllowed = sameStreetPool ? NYC_FALLBACK_MAX_HOUSE_DIST_SAME_STREET : NYC_FALLBACK_MAX_HOUSE_DIST_CROSS_STREET;
  const eligible = pool.filter((s) => passesHouseDistanceGate(s, sameStreetPool, requestedHouseNumber));

  if (eligible.length === 0) {
    pool.sort((a, b) => {
      if (a.houseDist !== b.houseDist) return a.houseDist - b.houseDist;
      if (b.tier !== a.tier) return b.tier - a.tier;
      return b.uc - a.uc;
    });
    const wouldBe = pool[0]!;
    const hd =
      wouldBe.houseDist < 9999990 && requestedHouseNumber != null ? wouldBe.houseDist : null;
    return {
      row: null,
      fallbackType: null,
      scoreReason: `rejected:min_house_dist_${wouldBe.houseDist < 9999990 ? wouldBe.houseDist : "unknown"}_exceeds_max_${maxAllowed}_${sameStreetPool ? "same_street" : "cross_street"}`,
      sameStreetPool,
      houseDistance: hd,
    };
  }

  eligible.sort((a, b) => {
    if (a.houseDist !== b.houseDist) return a.houseDist - b.houseDist;
    if (b.tier !== a.tier) return b.tier - a.tier;
    return b.uc - a.uc;
  });

  const best = eligible[0]!;
  const houseDistance =
    best.houseDist < 9999990 && requestedHouseNumber != null ? best.houseDist : null;

  let fallbackType: "building_fallback" | "street_fallback";
  let scoreReason: string;

  if (sameStreetPool && best.houseDist === 0) {
    fallbackType = "building_fallback";
    scoreReason = `same_normalized_street+house_number_match;ranked_by_residential_tier(${best.tier})_then_unit_count(${best.uc})`;
  } else if (sameStreetPool) {
    fallbackType = "street_fallback";
    scoreReason = `same_normalized_street+closest_house(dist=${best.houseDist};max=${NYC_FALLBACK_MAX_HOUSE_DIST_SAME_STREET});tier_then_unit_count`;
  } else {
    fallbackType = "street_fallback";
    scoreReason = `no_same_street_match_in_pool;best_effort(dist=${best.houseDist};max=${NYC_FALLBACK_MAX_HOUSE_DIST_CROSS_STREET})+tier+unit_count`;
  }

  return { row: best.row, fallbackType, scoreReason, sameStreetPool, houseDistance };
}

/**
 * Build LIKE patterns for street-level fallback when no exact full_address exists.
 * Deterministic; NYC grid + Central Park West.
 */
export function buildNycStreetFallbackLikePatterns(normalizedAddressLine: string): {
  patterns: string[];
  requestedHouseNumber: number | null;
} {
  const u = normalizedAddressLine.replace(/\s+/g, " ").trim().toUpperCase();
  if (!u) return { patterns: [], requestedHouseNumber: null };

  const requestedHouse = parseLeadingHouseNumberFromFullAddress(u);

  if (/\bCENTRAL\s+PARK\b/.test(u)) {
    // Detect whether the input specifies West (W/WEST) or East (E/EAST) side.
    const isCpw = /\bCENTRAL\s+PARK\s+(W|WEST)\b/.test(u);
    const isCpe = /\bCENTRAL\s+PARK\s+(E|EAST)\b/.test(u);

    const patterns: string[] = [];

    // Most specific first: anchored patterns with house number + correct side.
    if (requestedHouse != null) {
      if (isCpw) {
        // '234 CENTRAL PARK W%' matches "234 CENTRAL PARK WEST, ..." and "234 CENTRAL PARK W, ..."
        patterns.push(`${requestedHouse} CENTRAL PARK W%`);
        patterns.push(`${requestedHouse} CENTRAL PARK WEST%`);
      } else if (isCpe) {
        patterns.push(`${requestedHouse} CENTRAL PARK E%`);
        patterns.push(`${requestedHouse} CENTRAL PARK EAST%`);
      } else {
        // Side unknown — try both
        patterns.push(`${requestedHouse} CENTRAL PARK W%`);
        patterns.push(`${requestedHouse} CENTRAL PARK WEST%`);
        patterns.push(`${requestedHouse} CENTRAL PARK E%`);
        patterns.push(`${requestedHouse} CENTRAL PARK EAST%`);
      }
      // Broader wildcard anchor: any address containing this house number + CENTRAL
      patterns.push(`%${requestedHouse}%CENTRAL%`);
    }

    // Street-level patterns (no house anchor)
    if (isCpw) {
      patterns.push("%CENTRAL%PARK%WEST%");
      patterns.push("%CENTRAL%PARK%W%");
    } else if (isCpe) {
      patterns.push("%CENTRAL%PARK%EAST%");
      patterns.push("%CENTRAL%PARK%E%");
    } else {
      patterns.push("%CENTRAL%PARK%WEST%");
      patterns.push("%CENTRAL%PARK%W%");
      patterns.push("%CENTRAL%PARK%EAST%");
      patterns.push("%CENTRAL%PARK%E%");
    }

    // Broadest fallback — any Central Park address
    patterns.push("%CENTRAL%PARK%");

    return { patterns: [...new Set(patterns)], requestedHouseNumber: requestedHouse };
  }

  const grid = u.match(
    /^(\d+)\s+(W|E|N|S)\s+(\d{1,3})(?:(ST|ND|RD|TH))?(?:\s|$)/
  );
  if (grid) {
    const dir = grid[2]!;
    const sn = grid[3]!;
    const dirLong =
      dir === "W" ? "WEST" : dir === "E" ? "EAST" : dir === "N" ? "NORTH" : "SOUTH";
    const patterns = [
      `%${dir}%${sn}%`,
      `%${dirLong}%${sn}%`,
      `%${sn}%STREET%`,
      `%${sn}%ST%`,
      `%${sn}ST%`,
      `%${sn}ND%`,
      `%${sn}RD%`,
      `%${sn}TH%`,
    ];
    return { patterns: [...new Set(patterns)], requestedHouseNumber: requestedHouse };
  }

  const tok = u.split(/\s+/).filter(Boolean).slice(0, 6).join("%");
  if (tok.length >= 4) {
    return { patterns: [`%${tok}%`], requestedHouseNumber: requestedHouse };
  }
  return { patterns: [], requestedHouseNumber: requestedHouse };
}

/**
 * When exact equality on full_address fails: find rows whose full_address contains the street signature,
 * then rank by same street, house proximity, residential tier, unit_count.
 */
export async function queryPrecomputedNycStreetFallbackRow(
  client: ReturnType<typeof getUSBigQueryClient>,
  normalizedAddressLine: string
): Promise<{
  row: Record<string, unknown> | null;
  rowsReturned: number;
  patternsUsed: string[];
  fallbackType: "building_fallback" | "street_fallback" | null;
  scoreReason: string;
  sameStreetPool: boolean;
  houseDistance: number | null;
}> {
  const { patterns, requestedHouseNumber } = buildNycStreetFallbackLikePatterns(normalizedAddressLine);
  if (patterns.length === 0) {
    return {
      row: null,
      rowsReturned: 0,
      patternsUsed: [],
      fallbackType: null,
      scoreReason: "no_patterns",
      sameStreetPool: false,
      houseDistance: null,
    };
  }

  const ors = patterns.map((_, i) => `UPPER(c.full_address) LIKE @p${i}`).join("\n   OR ");
  const params: Record<string, string> = {};
  patterns.forEach((p, i) => {
    params[`p${i}`] = p;
  });

  const query = US_NYC_STREET_FALLBACK_JOIN_QUERY.replace("__STREET_FALLBACK_WHERE__", `(${ors})`);
  const [rows] = await client.query({
    query,
    params,
    location: NYC_PRECOMPUTED_LOCATION,
  });
  const list = (rows as Record<string, unknown>[] | null | undefined) ?? [];

  if (list.length === 0) {
    // Pool is empty: data coverage gap. Log for diagnosis.
    if (process.env.NODE_ENV !== "test") {
      try {
        console.log(
          "[NYC_FALLBACK_POOL_EMPTY]",
          JSON.stringify({
            normalized_address_line: normalizedAddressLine,
            patterns_tried: patterns,
            requested_house_number: requestedHouseNumber,
            note: "No rows in us_nyc_card_output_v5 match these LIKE patterns — data coverage gap",
          })
        );
      } catch {
        /* ignore logging failures */
      }
    }
    return {
      row: null,
      rowsReturned: 0,
      patternsUsed: patterns,
      fallbackType: null,
      scoreReason: "no_pool_rows",
      sameStreetPool: false,
      houseDistance: null,
    };
  }

  const rank = rankNycFallbackRows(list, normalizedAddressLine, requestedHouseNumber);
  return {
    row: rank.row,
    rowsReturned: list.length,
    patternsUsed: patterns,
    fallbackType: rank.fallbackType,
    scoreReason: rank.scoreReason,
    sameStreetPool: rank.sameStreetPool,
    houseDistance: rank.houseDistance,
  };
}
