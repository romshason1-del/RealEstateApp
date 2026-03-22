/**
 * France real estate data service using BigQuery as single source of truth.
 * Implements: 1) Estimated property value, 2) Last transaction, 3) Area average, 4) Livability.
 *
 * Schema: The france_transactions table uses original DVF column names with spaces/capitalization.
 * All column references use backticks and the exact BigQuery schema names.
 *
 * Monetary values: DVF "Valeur fonciere" is in euros. We treat all valeur_fonciere-derived
 * amounts as euros: no cent conversion. Price per m² = valeur_fonciere / surface_reelle_bati.
 * Estimated value = avg_price_m2 * surface_reelle_bati.
 */

import { getBigQueryClient, getBigQueryConfig, logAuthMetadata } from "./bigquery-client";

/** Actual BigQuery column names (DVF original schema with spaces). Use backticks in SQL. */
const COLS = {
  code_postal: "`Code postal`",
  commune: "`Commune`",
  no_voie: "`No voie`",
  type_de_voie: "`Type de voie`",
  voie: "`Voie`",
  lot_1er: "`1er lot`",
  valeur_fonciere: "`Valeur fonciere`",
  surface_reelle_bati: "`Surface reelle bati`",
  date_mutation: "`Date mutation`",
  nature_mutation: "`Nature mutation`",
  type_local: "`Type local`",
} as const;

export type MatchStage = 1 | 2 | 3 | 4 | 5;
export type ResultLevel = "exact_property" | "building" | "commune_fallback";

function logBigQueryError(label: string, err: unknown): void {
  const e = err as Error & {
    code?: number;
    errors?: unknown[];
    response?: { data?: unknown; body?: unknown; statusCode?: number };
    stack?: string;
  };
  console.error("[BigQuery ERROR]", label, {
    message: e?.message,
    code: e?.code,
    errors: e?.errors,
    responseData: e?.response?.data,
    responseBody: e?.response?.body,
    statusCode: e?.response?.statusCode,
    stack: e?.stack,
  });
}

export type FranceTransactionRow = {
  date_mutation: string | null;
  valeur_fonciere: string | null;
  no_voie: string | null;
  voie: string | null;
  code_postal: string | null;
  commune: string | null;
  type_local: string | null;
  surface_reelle_bati: string | null;
  lot_1er: string | null;
};

/** Facts-compatible row for France ladder integration (property_latest_facts schema). */
export type FranceRichSourceRow = {
  surface_m2: number | null;
  price_per_m2: number | null;
  last_sale_price: number | null;
  last_sale_date: string | null;
  unit_number: string | null;
  house_number: string | null;
  property_type: string | null;
  [key: string]: unknown;
};

export type FrancePropertyResult = {
  currentValue: number | null;
  lastTransaction: { date: string | null; value: number } | null;
  areaAverageValue: number | null;
  livabilityStandard: "Low" | "Medium" | "High" | "Premium";
  multipleUnits: boolean;
  unitCount?: number;
  averageBuildingValue?: number;
  buildingSales: Array<{ date: string | null; type: string; price: number; surface: number | null; lot_number: string | null }>;
  surfaceReelleBati: number | null;
  lotNumber: string | null;
  /** Normalized requested lot/apartment number (if provided). */
  requestedLot?: string | null;
  /** Number of DVF rows matched for the requested lot (0 when none). */
  exactLotRowCount?: number;
  matchStage?: MatchStage;
  resultLevel?: ResultLevel;
  rowsAtStage?: number;
  /** True when user provided apartment number but no matching lot found in DVF */
  apartmentNotMatched?: boolean;
  /** Available lot numbers in this building (for prompt) */
  availableLots?: string[];
  /** Step 2: best similar apartment (same building) when lot not matched */
  similarApartment?: { lotNumber: string | null; date: string | null; value: number; surface: number | null; typeLocal: string | null } | null;
  /** Step 4: nearby comparable when building fallback is weak/unavailable */
  nearbyComparable?: {
    address: string;
    lotNumber: string | null;
    date: string | null;
    value: number;
    surface: number | null;
    typeLocal: string | null;
    scope: "same_street" | "same_postcode_commune" | "same_commune";
    selectedNearbyStrategy: "street_postcode" | "postcode_commune" | "commune";
  } | null;
  debug?: {
    candidateCountSameBuilding?: number;
    candidateCountNearby?: number;
    reliableCandidateCountSameBuilding?: number;
    reliableCandidateCountNearby?: number;
    similarityScore?: number | null;
    nearbyStageCounts?: { same_street: number; same_postcode_commune: number; same_commune: number };
    selectedNearbyStrategy?: "street_postcode" | "postcode_commune" | "commune" | null;
    nearbyFilterStats?: { raw: number; missingSurface: number; missingDate: number; tooOld: number; trustworthy: number };
    validationNotes?: string[];
  };
};

const EXCLUDED_TYPES = ["dépendance", "local industriel", "parking"];
const PRIMARY_TYPES = ["appartement", "maison", ""];

const PRICE_PER_SQM_MIN = 1000;
const PRICE_PER_SQM_MAX = 20000;
const OUTLIER_MAX_DEVIATION_FROM_MEDIAN = 0.4;

function computePricePerSqm(value: number, surface: number | null): number | null {
  if (!Number.isFinite(value) || value <= 0) return null;
  if (surface == null || !Number.isFinite(surface) || surface <= 0) return null;
  return value / surface;
}

function median(values: number[]): number | null {
  const arr = values.filter((v) => Number.isFinite(v)).sort((a, b) => a - b);
  if (arr.length === 0) return null;
  const mid = Math.floor(arr.length / 2);
  if (arr.length % 2 === 1) return arr[mid] ?? null;
  const a = arr[mid - 1];
  const b = arr[mid];
  if (a == null || b == null) return null;
  return (a + b) / 2;
}

function filterCompsByPricePerSqmValidation<T extends { value: number; surface: number | null }>(items: T[]) {
  const withP = items
    .map((c) => ({ c, p: computePricePerSqm(c.value, c.surface) }))
    .filter((x) => x.p != null) as Array<{ c: T; p: number }>;

  const inRange = withP.filter((x) => x.p >= PRICE_PER_SQM_MIN && x.p <= PRICE_PER_SQM_MAX);
  const med = median(inRange.map((x) => x.p));
  if (med == null || med <= 0) {
    return { kept: [] as T[], medianPricePerSqm: null as number | null };
  }
  const kept = inRange
    .filter((x) => Math.abs(x.p - med) / med <= OUTLIER_MAX_DEVIATION_FROM_MEDIAN)
    .map((x) => x.c);
  return { kept, medianPricePerSqm: med };
}

function parseFrenchNumber(s: string | null | undefined): number {
  if (!s || !String(s).trim()) return NaN;
  const normalized = String(s).replace(/\s/g, "").replace(",", ".");
  return parseFloat(normalized) || NaN;
}

/**
 * DVF Valeur fonciere is in euros. No cent conversion.
 * If the BigQuery table stores values 100x (e.g. 82953000 for €829,530), convert to euros.
 */
function valueFonciereToEuros(raw: number): number {
  if (!Number.isFinite(raw)) return 0;
  if (raw >= 1e7) return raw / 100;
  return raw;
}

function valueFonciereToEurosSafe(raw: number, surface: number | null): number {
  if (!Number.isFinite(raw)) return 0;
  const base = valueFonciereToEuros(raw);
  if (surface != null && surface > 0) {
    const p = base / surface;
    if (p > 50000) {
      // Heuristic: some datasets store valeurs in cents even for <1e7 values.
      // If dividing by 100 yields a plausible €/m², prefer that to avoid multi-lot / scaling anomalies.
      const v2 = raw / 100;
      if (Number.isFinite(v2) && v2 > 0 && v2 / surface <= 50000) return v2;
    }
  }
  return base;
}

function parseFrenchDate(s: any): string | null {
  if (s == null) return null;
  if (s instanceof Date && !Number.isNaN(s.getTime())) return s.toISOString().slice(0, 10);
  // BigQuery can return DATE/TIMESTAMP-like objects (e.g. { value: '2023-05-18' }).
  if (typeof s === "object" && typeof (s as any).value === "string") {
    const v = String((s as any).value).trim();
    if (v) s = v;
  }
  if (!String(s).trim()) return null;
  const raw = String(s).trim();
  // Common DVF export format: DD/MM/YYYY
  const fr = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (fr) return `${fr[3]}-${fr[2]}-${fr[1]}`;
  // Sometimes already normalized: YYYY-MM-DD (or YYYY/MM/DD)
  const iso = raw.match(/^(\d{4})[-\/](\d{2})[-\/](\d{2})/);
  if (iso) return `${iso[1]}-${iso[2]}-${iso[3]}`;
  return null;
}

function isExcludedType(typeLocal: string | null | undefined): boolean {
  const t = (typeLocal ?? "").toLowerCase().trim();
  return EXCLUDED_TYPES.includes(t);
}

function isPrimaryUnit(typeLocal: string | null | undefined): boolean {
  const t = (typeLocal ?? "").toLowerCase().trim();
  return PRIMARY_TYPES.includes(t) || t === "appartement" || t === "maison";
}

/**
 * Normalize a user-provided lot/apartment value into a stable display string.
 * Keeps it human-readable; matching uses `canonicalizeLotCandidates`.
 */
export function normalizeLot(val: string | number | null | undefined): string {
  if (val === null || val === undefined) return "";
  const raw = String(val).trim();
  if (!raw) return "";
  // Normalize common prefixes / separators but keep original meaning.
  return raw.replace(/\s+/g, " ").trim();
}

function canonicalizeLotSegment(seg: string): string {
  const s = seg
    .toLowerCase()
    .trim()
    .replace(/^lot[\s.:#-]*/i, "")
    .replace(/[()]/g, "")
    .trim();
  if (!s) return "";
  // Keep alphanumerics only, collapse.
  const cleaned = s.replace(/[^0-9a-z]/gi, "");
  if (!cleaned) return "";
  // If purely numeric, strip leading zeros ("0009" -> "9").
  if (/^\d+$/.test(cleaned)) return cleaned.replace(/^0+/, "") || "0";
  // If mixed, still strip leading zeros from numeric prefix (e.g. "0009A" -> "9A").
  const m = cleaned.match(/^0+(\d.*)$/);
  return m ? m[1] : cleaned;
}

function canonicalizeLotCandidates(value: string): string[] {
  const raw = normalizeLot(value);
  if (!raw) return [];
  // Support "09/0009" or "09-0009" by splitting into segments.
  const parts = raw.split(/[\/\\-]/g).map((p) => canonicalizeLotSegment(p)).filter(Boolean);
  // Also add the fully-collapsed canonical form as a candidate.
  const collapsed = canonicalizeLotSegment(raw);
  const all = [...parts, ...(collapsed ? [collapsed] : [])];
  // Unique preserving order.
  return all.filter((v, i) => all.indexOf(v) === i);
}

/** Canonical form for lot comparison: "9" and "09" match. */
function lotMatches(lotInRow: string, requestedLot: string): boolean {
  if (!requestedLot) return false;
  const rowCandidates = canonicalizeLotCandidates(lotInRow);
  const reqCandidates = canonicalizeLotCandidates(requestedLot);
  if (rowCandidates.length === 0 || reqCandidates.length === 0) return false;
  for (const r of rowCandidates) {
    for (const q of reqCandidates) {
      if (r === q) return true;
    }
  }
  return false;
}

/** Normalize string for address matching: uppercase, trim, collapse spaces, basic accent fold. */
function normalizeForMatch(s: string | null | undefined): string {
  if (!s || typeof s !== "string") return "";
  const unified = s
    // normalize smart apostrophes to ASCII apostrophe
    .replace(/[\u2019\u2018\u02BC\u00B4\u0060]/g, "'")
    .replace(/[’‘]/g, "'")
    .replace(/\s+/g, " ")
    .trim();
  return unified
    .toUpperCase()
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .replace(/\s+/g, " ")
    .trim();
}

/** Loose street key for matching: remove punctuation/spaces after normalizeForMatch. */
function normalizeStreetLoose(s: string | null | undefined): string {
  const strict = normalizeForMatch(s);
  if (!strict) return "";
  return strict.replace(/[^A-Z0-9]+/g, "").trim();
}

/** Build full street from Type de voie + Voie for matching. */
function buildFullStreet(typeDeVoie: string | null, voie: string | null): string {
  const t = (typeDeVoie ?? "").trim();
  const v = (voie ?? "").trim();
  if (!t && !v) return "";
  if (!t) return v;
  if (!v) return t;
  return `${t} ${v}`;
}

function medianNumber(values: number[]): number | null {
  if (!values || values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid] ?? null;
  const a = sorted[mid - 1];
  const b = sorted[mid];
  if (typeof a !== "number" || typeof b !== "number") return null;
  return (a + b) / 2;
}

type NearbyScope = "same_street" | "same_postcode_commune" | "same_commune";
type NearbyStrategy = "street_postcode" | "postcode_commune" | "commune";

function makeNearbyAddress(r: FranceTransactionRow, codePostalFallback: string, communeFallback: string): string {
  const cp = normalizeCodePostal(String(r.code_postal ?? codePostalFallback ?? "").trim());
  const com = String(r.commune ?? communeFallback ?? "").trim();
  const street = buildFullStreet(String((r as any).type_de_voie ?? ""), String(r.voie ?? ""));
  const no = normalizeLot(r.no_voie);
  return `${`${no} ${street}`.trim()}, ${cp}, ${com}`.trim();
}

function rankNearbyCandidate(args: {
  candidateType: string | null;
  candidateSurface: number | null;
  candidateDate: string | null;
  scope: NearbyScope;
  preferredType: string | null;
  targetSurface: number | null;
}): number {
  // Higher is better.
  const { candidateType, candidateSurface, candidateDate, scope, preferredType, targetSurface } = args;
  let score = 0;

  // Scope preference (street > postcode+commune > commune).
  if (scope === "same_street") score += 1200;
  else if (scope === "same_postcode_commune") score += 800;
  else score += 400;

  // Property type match when we have a preference.
  if (preferredType && candidateType) {
    if (candidateType.toLowerCase() === preferredType.toLowerCase()) score += 600;
    else score -= 120;
  }

  // Surface proximity when we have a target.
  if (targetSurface && candidateSurface && targetSurface > 0 && candidateSurface > 0) {
    const diff = Math.abs(candidateSurface - targetSurface);
    // Keep bounded; closer surface gets higher score.
    score += Math.max(0, 450 - diff * 6);
  }

  // Recency: use lexicographic YYYY-MM-DD (higher is newer).
  // We don't convert to timestamps (cheaper / safer for partial data).
  if (candidateDate) score += 200;

  return score;
}

async function queryNearbyComparableOnStreet(
  codePostal: string,
  normalizedStreet: string,
  excludeNoVoie: string | null
): Promise<FranceTransactionRow[]> {
  if (!codePostal?.trim() || !normalizedStreet?.trim()) return [];
  const client = getBigQueryClient();
  const { projectId, dataset, table, location } = getBigQueryConfig();
  const fullTable = `\`${projectId}.${dataset}.${table}\``;
  const pc = normalizeCodePostal(codePostal);
  const pcAlt = pc.startsWith("0") ? pc.slice(1) : pc;
  const streetNormSql = sqlFullStreetNormalized();
  const where = `
    LOWER(TRIM(CAST(${COLS.nature_mutation} AS STRING))) = 'vente'
    AND (TRIM(CAST(${COLS.code_postal} AS STRING)) = @pc OR TRIM(CAST(${COLS.code_postal} AS STRING)) = @pcAlt)
    AND ${streetNormSql} = @streetNormValue
    AND LOWER(TRIM(CAST(${COLS.type_local} AS STRING))) IN ('appartement','maison')
    ${excludeNoVoie ? `AND TRIM(CAST(${COLS.no_voie} AS STRING)) != @excludeNoVoie` : ""}
  `;
  const query = `
    SELECT ${COLS.date_mutation} AS date_mutation, ${COLS.valeur_fonciere} AS valeur_fonciere, ${COLS.no_voie} AS no_voie,
           ${COLS.type_de_voie} AS type_de_voie, ${COLS.voie} AS voie, ${COLS.code_postal} AS code_postal, ${COLS.commune} AS commune,
           ${COLS.type_local} AS type_local, ${COLS.surface_reelle_bati} AS surface_reelle_bati, ${COLS.lot_1er} AS lot_1er
    FROM ${fullTable}
    WHERE ${where}
    ORDER BY SAFE.PARSE_DATE('%d/%m/%Y', CAST(${COLS.date_mutation} AS STRING)) DESC NULLS LAST
    LIMIT 50
  `;
  const params: Record<string, string> = {
    pc,
    pcAlt: pcAlt || "0",
    streetNormValue: normalizedStreet,
    ...(excludeNoVoie ? { excludeNoVoie: excludeNoVoie.trim() } : {}),
  };
  const [rows] = await client.query({ query, params, location });
  return (rows as FranceTransactionRow[]) || [];
}

async function queryNearbyComparableInPostcode(
  codePostal: string
): Promise<FranceTransactionRow[]> {
  if (!codePostal?.trim()) return [];
  const client = getBigQueryClient();
  const { projectId, dataset, table, location } = getBigQueryConfig();
  const fullTable = `\`${projectId}.${dataset}.${table}\``;
  const pc = normalizeCodePostal(codePostal);
  const pcAlt = pc.startsWith("0") ? pc.slice(1) : pc;
  const where = `
    LOWER(TRIM(CAST(${COLS.nature_mutation} AS STRING))) = 'vente'
    AND (TRIM(CAST(${COLS.code_postal} AS STRING)) = @pc OR TRIM(CAST(${COLS.code_postal} AS STRING)) = @pcAlt)
    AND LOWER(TRIM(CAST(${COLS.type_local} AS STRING))) IN ('appartement','maison')
  `;
  const query = `
    SELECT ${COLS.date_mutation} AS date_mutation, ${COLS.valeur_fonciere} AS valeur_fonciere, ${COLS.no_voie} AS no_voie,
           ${COLS.type_de_voie} AS type_de_voie, ${COLS.voie} AS voie, ${COLS.code_postal} AS code_postal, ${COLS.commune} AS commune,
           ${COLS.type_local} AS type_local, ${COLS.surface_reelle_bati} AS surface_reelle_bati, ${COLS.lot_1er} AS lot_1er
    FROM ${fullTable}
    WHERE ${where}
    ORDER BY SAFE.PARSE_DATE('%d/%m/%Y', CAST(${COLS.date_mutation} AS STRING)) DESC NULLS LAST
    LIMIT 80
  `;
  const params: Record<string, string> = { pc, pcAlt: pcAlt || "0" };
  const [rows] = await client.query({ query, params, location });
  return (rows as FranceTransactionRow[]) || [];
}

async function queryNearbyComparableInPostcodeAndCommune(
  codePostal: string,
  commune: string | null
): Promise<FranceTransactionRow[]> {
  if (!codePostal?.trim() || !commune?.trim()) return [];
  const client = getBigQueryClient();
  const { projectId, dataset, table, location } = getBigQueryConfig();
  const fullTable = `\`${projectId}.${dataset}.${table}\``;
  const pc = normalizeCodePostal(codePostal);
  const pcAlt = pc.startsWith("0") ? pc.slice(1) : pc;
  const where = `
    LOWER(TRIM(CAST(${COLS.nature_mutation} AS STRING))) = 'vente'
    AND (TRIM(CAST(${COLS.code_postal} AS STRING)) = @pc OR TRIM(CAST(${COLS.code_postal} AS STRING)) = @pcAlt)
    AND LOWER(TRIM(CAST(${COLS.commune} AS STRING))) = LOWER(TRIM(@commune))
    AND LOWER(TRIM(CAST(${COLS.type_local} AS STRING))) IN ('appartement','maison')
  `;
  const query = `
    SELECT ${COLS.date_mutation} AS date_mutation, ${COLS.valeur_fonciere} AS valeur_fonciere, ${COLS.no_voie} AS no_voie,
           ${COLS.type_de_voie} AS type_de_voie, ${COLS.voie} AS voie, ${COLS.code_postal} AS code_postal, ${COLS.commune} AS commune,
           ${COLS.type_local} AS type_local, ${COLS.surface_reelle_bati} AS surface_reelle_bati, ${COLS.lot_1er} AS lot_1er
    FROM ${fullTable}
    WHERE ${where}
    ORDER BY SAFE.PARSE_DATE('%d/%m/%Y', CAST(${COLS.date_mutation} AS STRING)) DESC NULLS LAST
    LIMIT 100
  `;
  const params: Record<string, string> = { pc, pcAlt: pcAlt || "0", commune: commune.trim() };
  const [rows] = await client.query({ query, params, location });
  return (rows as FranceTransactionRow[]) || [];
}

async function queryNearbyComparableInCommune(
  commune: string | null
): Promise<FranceTransactionRow[]> {
  if (!commune?.trim()) return [];
  const client = getBigQueryClient();
  const { projectId, dataset, table, location } = getBigQueryConfig();
  const fullTable = `\`${projectId}.${dataset}.${table}\``;
  const where = `
    LOWER(TRIM(CAST(${COLS.nature_mutation} AS STRING))) = 'vente'
    AND LOWER(TRIM(CAST(${COLS.commune} AS STRING))) = LOWER(TRIM(@commune))
    AND LOWER(TRIM(CAST(${COLS.type_local} AS STRING))) IN ('appartement','maison')
  `;
  const query = `
    SELECT ${COLS.date_mutation} AS date_mutation, ${COLS.valeur_fonciere} AS valeur_fonciere, ${COLS.no_voie} AS no_voie,
           ${COLS.type_de_voie} AS type_de_voie, ${COLS.voie} AS voie, ${COLS.code_postal} AS code_postal, ${COLS.commune} AS commune,
           ${COLS.type_local} AS type_local, ${COLS.surface_reelle_bati} AS surface_reelle_bati, ${COLS.lot_1er} AS lot_1er
    FROM ${fullTable}
    WHERE ${where}
    ORDER BY SAFE.PARSE_DATE('%d/%m/%Y', CAST(${COLS.date_mutation} AS STRING)) DESC NULLS LAST
    LIMIT 120
  `;
  const params: Record<string, string> = { commune: commune.trim() };
  const [rows] = await client.query({ query, params, location });
  return (rows as FranceTransactionRow[]) || [];
}

/** Log 10 sample rows for a commune to inspect address format. */
async function logSampleRows(commune: string): Promise<void> {
  try {
    const client = getBigQueryClient();
    const { projectId, dataset, table, location } = getBigQueryConfig();
    const fullTable = `\`${projectId}.${dataset}.${table}\``;
    const query = `
      SELECT ${COLS.code_postal}, ${COLS.commune}, ${COLS.no_voie}, ${COLS.type_de_voie}, ${COLS.voie}, ${COLS.lot_1er}, ${COLS.valeur_fonciere}, ${COLS.surface_reelle_bati}, ${COLS.date_mutation}
      FROM ${fullTable}
      WHERE LOWER(TRIM(CAST(${COLS.commune} AS STRING))) = LOWER(TRIM(@commune))
        AND LOWER(TRIM(CAST(${COLS.nature_mutation} AS STRING))) = 'vente'
      ORDER BY SAFE.PARSE_DATE('%d/%m/%Y', CAST(${COLS.date_mutation} AS STRING)) DESC NULLS LAST
      LIMIT 10
    `;
    const [rows] = await client.query({ query, params: { commune }, location });
    const arr = (rows as Record<string, unknown>[]) || [];
    console.log("[BigQuery] Sample rows for commune =", JSON.stringify(commune), "count =", arr.length);
    arr.forEach((r, i) => {
      const fullStreet = buildFullStreet(String(r["Type de voie"] ?? ""), String(r.Voie ?? ""));
      console.log(`[BigQuery] Sample ${i + 1}:`, {
        "Code postal": r["Code postal"],
        Commune: r.Commune,
        "No voie": r["No voie"],
        "Type de voie": r["Type de voie"],
        Voie: r.Voie,
        fullStreet,
        "1er lot": r["1er lot"],
        "Valeur fonciere": r["Valeur fonciere"],
        "Surface reelle bati": r["Surface reelle bati"],
        "Date mutation": r["Date mutation"],
      });
    });
  } catch (e) {
    console.log("[BigQuery] Could not fetch sample rows:", (e as Error)?.message);
  }
}

/** Log first 15 column names from the table schema (one-time per request). */
async function logSchemaColumns(): Promise<void> {
  try {
    const client = getBigQueryClient();
    const { projectId, dataset, table, location } = getBigQueryConfig();
    const fullTable = `\`${projectId}.${dataset}.${table}\``;
    const [rows] = await client.query({
      query: `SELECT column_name FROM \`${projectId}.${dataset}.INFORMATION_SCHEMA.COLUMNS\` WHERE table_name = '${table}' ORDER BY ordinal_position LIMIT 45`,
      location,
    });
    const names = (rows as { column_name: string }[]).map((r) => r.column_name);
    console.log("[BigQuery] Schema columns (first 15):", names.join(", "));
  } catch (e) {
    console.log("[BigQuery] Could not fetch schema:", (e as Error)?.message);
  }
}

function normalizeCodePostal(pc: string): string {
  const t = pc.trim();
  if (t.length === 4 && !t.startsWith("0")) return "0" + t;
  return t;
}

/** Paris: DVF stores commune as "PARIS 04", "PARIS 11", etc. Normalize "Paris" + postcode 75XXX to "PARIS XX". */
function normalizeParisCommune(codePostal: string, commune: string | null): string | null {
  const pc = codePostal.trim();
  const c = (commune ?? "").trim();
  if (!c) return null;
  if (!/^75\d{3}$/.test(pc)) return c;
  if (!/^paris$/i.test(c)) return c;
  const arr = pc.slice(-2);
  return `PARIS ${arr}`;
}

/**
 * Minimal test: SELECT 1 AS ok - verifies client/query path works.
 * Uses createQueryJob + getQueryResults to log raw request if query() fails.
 */
export async function runSelect1Test(): Promise<{ ok: boolean; error?: string }> {
  try {
    const client = getBigQueryClient();
    await logAuthMetadata(client);
    const { location } = getBigQueryConfig();
    const opts = { query: "SELECT 1 AS ok", location };
    console.log("[BigQuery] SELECT 1 - query options:", JSON.stringify(opts, null, 2));

    const [rows] = await client.query(opts);
    const row = (rows as { ok: number }[])?.[0];
    const ok = row?.ok === 1;
    console.log("[BigQuery] SELECT 1 result:", ok ? "OK" : "FAIL", rows);
    return { ok };
  } catch (err) {
    logBigQueryError("SELECT 1 test (query() failed)", err);
    console.log("[BigQuery] Retrying SELECT 1 via createQueryJob to log raw request...");
    try {
      const client = getBigQueryClient();
      const { location } = getBigQueryConfig();
      const reqOpts = { query: "SELECT 1 AS ok", location };
      console.log("[BigQuery] createQueryJob request opts:", JSON.stringify(reqOpts, null, 2));
      const [job] = await client.createQueryJob(reqOpts);
      const [rows] = await job.getQueryResults();
      const row = (rows as { ok: number }[])?.[0];
      const ok = row?.ok === 1;
      console.log("[BigQuery] SELECT 1 via createQueryJob:", ok ? "OK" : "FAIL");
      return { ok };
    } catch (err2) {
      logBigQueryError("SELECT 1 test (createQueryJob also failed)", err2);
      const msg = (err2 as Error)?.message ?? "";
      if (msg.includes("Not found: Project") || (err2 as { code?: number })?.code === 404) {
        console.error(
          "[BigQuery] Auth/project hint: Ensure (1) GOOGLE_APPLICATION_CREDENTIALS points to a service account JSON with access to the project, " +
            "(2) BigQuery API is enabled, (3) project ID matches the GCP Console (e.g. project-29fdf5d2-b1fb-4c43-b668)."
        );
      }
      return { ok: false, error: (err as Error)?.message };
    }
  }
}

/**
 * Minimal real query: COUNT(*) for Code postal 06000 - no other filters.
 */
export async function runCount06000Test(): Promise<{ n: number; error?: string }> {
  try {
    const client = getBigQueryClient();
    const { projectId, dataset, table, location } = getBigQueryConfig();
    const fullTable = `\`${projectId}.${dataset}.${table}\``;
    const query = `SELECT COUNT(*) AS n FROM ${fullTable} WHERE TRIM(CAST(${COLS.code_postal} AS STRING)) = '06000'`;
    const opts = { query, location };
    console.log("[BigQuery] COUNT 06000 - query options:", JSON.stringify(opts, null, 2));
    const [rows] = await client.query(opts);
    const row = (rows as { n: number }[])?.[0];
    const n = Number(row?.n ?? 0);
    console.log("[BigQuery] COUNT 06000 result: n =", n);
    return { n };
  } catch (err) {
    logBigQueryError("COUNT 06000 test", err);
    return { n: 0, error: (err as Error)?.message };
  }
}

/**
 * Full normalized street in SQL: CONCAT(Type de voie, ' ', Voie), uppercased and trimmed.
 * Avoid NORMALIZE/NFD due to BigQuery mode compatibility; use UPPER for matching.
 */
function sqlFullStreetNormalized(): string {
  const concat = `TRIM(CONCAT(COALESCE(TRIM(CAST(${COLS.type_de_voie} AS STRING)), ''), ' ', COALESCE(TRIM(CAST(${COLS.voie} AS STRING)), '')))`;
  const upper = `UPPER(${concat})`;
  // Unify apostrophe-like chars (Google input may use ’, dataset may use ')
  // NOTE: keep regex ASCII-only so TS parsing stays stable on Windows tooling.
  return `REGEXP_REPLACE(${upper}, r"[\\x60\\xB4\\x27]", "'")`;
}

/**
 * Staged address matching. Returns rows and the stage that matched.
 * Stage 1: postcode + no_voie + full street + lot (if provided)
 * Stage 2: postcode + no_voie + full street
 * Stage 3: commune + no_voie + full street
 * Stage 4: commune + full street
 * Stage 5: commune only
 */
async function queryLastTransactionByStage(
  stage: MatchStage,
  codePostal: string,
  commune: string | null,
  noVoie: string | null,
  voie: string | null,
  lotNumber: string | null,
  normalizedStreet: string
): Promise<FranceTransactionRow[]> {
  const client = getBigQueryClient();
  const { projectId, dataset, table, location } = getBigQueryConfig();
  const fullTable = `\`${projectId}.${dataset}.${table}\``;
  const pc = normalizeCodePostal(codePostal);
  const pcAlt = pc.startsWith("0") ? pc.slice(1) : pc;
  const streetNorm = sqlFullStreetNormalized();
  const streetNormLoose = `REGEXP_REPLACE(${streetNorm}, r"[^A-Z0-9]+", "")`;

  const baseConditions: string[] = [`LOWER(TRIM(CAST(${COLS.nature_mutation} AS STRING))) = 'vente'`];
  const params: Record<string, string> = {
    pc,
    pcAlt: pcAlt || "0",
    streetNormValue: normalizedStreet,
    streetNormLooseValue: normalizeStreetLoose(normalizedStreet) || normalizedStreet,
  };

  if (stage <= 4 && normalizedStreet) {
    baseConditions.push(`(${streetNorm} = @streetNormValue OR ${streetNormLoose} = @streetNormLooseValue)`);
  }
  if (stage <= 3 && noVoie && noVoie.trim()) {
    baseConditions.push(`(TRIM(CAST(${COLS.no_voie} AS STRING)) = @noVoie OR TRIM(CAST(${COLS.no_voie} AS STRING)) = CONCAT('0', @noVoie))`);
    params.noVoie = noVoie.trim();
  }
  if (stage === 1 && lotNumber && lotNumber.trim()) {
    const lotNorm = normalizeLot(lotNumber);
    const lotCanonical = lotNorm.replace(/^0+/, "") || lotNorm;
    const lotClean = canonicalizeLotSegment(lotNorm);
    const lotCleanCanonical = lotClean.replace(/^0+/, "") || lotClean;
    params.lotNumber = lotNorm;
    params.lotCanonical = lotCanonical;
    params.lotClean = lotClean;
    params.lotCleanCanonical = lotCleanCanonical;
    // Robust matching:
    // - Exact match (trimmed)
    // - Leading-zero variants (strip leading zeros on BOTH sides)
    // - "Lot 9" style prefixes and separators (cleaned)
    // - Case-insensitive (handles e.g. 9A)
    baseConditions.push(`(
      LOWER(TRIM(CAST(${COLS.lot_1er} AS STRING))) = LOWER(@lotNumber)
      OR LOWER(REGEXP_REPLACE(TRIM(CAST(${COLS.lot_1er} AS STRING)), r'^0+', '')) = LOWER(@lotCanonical)
      OR LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(CAST(${COLS.lot_1er} AS STRING)), r'(?i)^lot\\s*', ''), r'[^0-9a-z]+', '')) = LOWER(@lotClean)
      OR LOWER(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(CAST(${COLS.lot_1er} AS STRING)), r'(?i)^lot\\s*', ''), r'[^0-9a-z]+', ''), r'^0+', '')) = LOWER(@lotCleanCanonical)
    )`);
  }

  if (stage <= 2) {
    baseConditions.push(`(TRIM(CAST(${COLS.code_postal} AS STRING)) = @pc OR TRIM(CAST(${COLS.code_postal} AS STRING)) = @pcAlt)`);
  } else if (commune && commune.trim()) {
    baseConditions.push(`LOWER(TRIM(CAST(${COLS.commune} AS STRING))) = LOWER(TRIM(@commune))`);
    params.commune = commune.trim();
  }

  const whereClause = baseConditions.join(" AND ");
  const query = `
    SELECT ${COLS.date_mutation} AS date_mutation, ${COLS.valeur_fonciere} AS valeur_fonciere, ${COLS.no_voie} AS no_voie, ${COLS.voie} AS voie, ${COLS.code_postal} AS code_postal, ${COLS.commune} AS commune, ${COLS.type_local} AS type_local, ${COLS.surface_reelle_bati} AS surface_reelle_bati, ${COLS.lot_1er} AS lot_1er
    FROM ${fullTable}
    WHERE ${whereClause}
    ORDER BY SAFE.PARSE_DATE('%d/%m/%Y', CAST(${COLS.date_mutation} AS STRING)) DESC NULLS LAST
    LIMIT 50
  `;

  if (stage === 1 && params.lotNumber) {
    console.log("[BigQuery] Stage 1 lot query debug:", {
      lotInput: params.lotNumber,
      lotCanonical: params.lotCanonical,
      lotClean: (params as any).lotClean,
      lotCleanCanonical: (params as any).lotCleanCanonical,
      whereClause,
    });
  }

  const [rows] = await client.query({ query, params, location });
  if (stage === 1 && params.lotNumber) {
    const lots = (rows as FranceTransactionRow[]).map((r) => normalizeLot(r.lot_1er)).filter(Boolean);
    console.log("[BigQuery] Stage 1 lot query result:", { rows: (rows as FranceTransactionRow[]).length, lots: [...new Set(lots)].slice(0, 20) });
  }
  return (rows as FranceTransactionRow[]) || [];
}

/**
 * Query richer DVF-derived source for same-address rows (postcode + street + house_number).
 * Used by France ladder when property_latest_facts has no same-address rows (e.g. 10 Rue de Rivoli, 75004).
 * Returns rows in property_latest_facts-compatible shape (thousandths-of-euro convention).
 */
export async function queryFranceRichSourceSameAddress(params: {
  postcodeNorm: string;
  streetNorm: string;
  houseNumberNorm: string;
  cityNorm?: string;
  lotNorm?: string | null;
}): Promise<FranceRichSourceRow[]> {
  const { postcodeNorm, streetNorm, houseNumberNorm, cityNorm, lotNorm } = params;
  if (!postcodeNorm?.trim() || !streetNorm?.trim() || !houseNumberNorm?.trim()) {
    return [];
  }
  try {
    const rows = await queryLastTransactionByStage(
      2,
      postcodeNorm,
      cityNorm ?? null,
      houseNumberNorm,
      streetNorm,
      lotNorm ?? null,
      streetNorm
    );
    return rows.map((r): FranceRichSourceRow => {
      const valEur = valueFonciereToEuros(parseFrenchNumber(r.valeur_fonciere));
      const surf = parseFrenchNumber(r.surface_reelle_bati);
      const ppm = surf != null && surf > 0 && valEur > 0 ? valEur / surf : null;
      return {
        surface_m2: surf != null && Number.isFinite(surf) ? surf : null,
        price_per_m2: ppm != null && Number.isFinite(ppm) ? Math.round(ppm * 1000) : null,
        last_sale_price: valEur > 0 ? Math.round(valEur * 1000) : null,
        last_sale_date: parseFrenchDate(r.date_mutation),
        unit_number: normalizeLot(r.lot_1er) || null,
        house_number: r.no_voie ? String(r.no_voie).trim() : null,
        property_type: r.type_local ? String(r.type_local).trim() : null,
      };
    });
  } catch (err) {
    logBigQueryError("queryFranceRichSourceSameAddress", err);
    return [];
  }
}

/**
 * PROPERTY VALUE: Avg price per m² from comparables, then estimated_value = avg_price_m2 * surface.
 */
async function queryAvgPricePerM2(
  codePostal: string,
  commune: string | null,
  useCommuneFallback: boolean
): Promise<number | null> {
  const client = getBigQueryClient();
  const { projectId, dataset, table, location } = getBigQueryConfig();
  const fullTable = `\`${projectId}.${dataset}.${table}\``;
  const pc = normalizeCodePostal(codePostal);
  const pcAlt = pc.startsWith("0") ? pc.slice(1) : pc;

  let whereClause: string;
  const params: Record<string, string> = { pc, pcAlt: pcAlt || "0" };

  const vf = `CAST(${COLS.valeur_fonciere} AS STRING)`;
  const srb = `CAST(${COLS.surface_reelle_bati} AS STRING)`;
  if (useCommuneFallback && commune && commune.trim()) {
    whereClause = `
      LOWER(TRIM(CAST(${COLS.nature_mutation} AS STRING))) = 'vente'
      AND LOWER(TRIM(CAST(${COLS.commune} AS STRING))) = LOWER(TRIM(@commune))
      AND SAFE_CAST(REPLACE(REPLACE(TRIM(${srb}), ' ', ''), ',', '.') AS FLOAT64) > 0
      AND SAFE_CAST(REPLACE(REPLACE(TRIM(${vf}), ' ', ''), ',', '.') AS FLOAT64) > 0
    `;
    params.commune = commune.trim();
  } else {
    whereClause = `
      LOWER(TRIM(CAST(${COLS.nature_mutation} AS STRING))) = 'vente'
      AND (TRIM(CAST(${COLS.code_postal} AS STRING)) = @pc OR TRIM(CAST(${COLS.code_postal} AS STRING)) = @pcAlt)
      AND SAFE_CAST(REPLACE(REPLACE(TRIM(${srb}), ' ', ''), ',', '.') AS FLOAT64) > 0
      AND SAFE_CAST(REPLACE(REPLACE(TRIM(${vf}), ' ', ''), ',', '.') AS FLOAT64) > 0
    `;
  }

  const query = `
    SELECT AVG(SAFE_CAST(REPLACE(REPLACE(TRIM(${vf}), ' ', ''), ',', '.') AS FLOAT64) / SAFE_CAST(REPLACE(REPLACE(TRIM(${srb}), ' ', ''), ',', '.') AS FLOAT64)) AS avg_price_m2
    FROM ${fullTable}
    WHERE ${whereClause}
  `;

  const [rows] = await client.query({ query, params, location });
  const row = (rows as { avg_price_m2: number | null }[])?.[0];
  const avg = row?.avg_price_m2;
  const avgEuros = avg != null && Number.isFinite(avg) ? (avg >= 1e5 ? avg / 100 : avg) : null;
  console.log("[BigQuery] Avg price/m² query returned:", avgEuros != null ? avgEuros.toFixed(0) : "null", useCommuneFallback ? "(commune fallback)" : "");
  return avgEuros;
}

/**
 * AREA AVERAGE: Average property value in same postcode (or commune if fallback).
 */
async function queryAreaAverage(
  codePostal: string,
  commune: string | null,
  useCommuneFallback: boolean
): Promise<number | null> {
  const client = getBigQueryClient();
  const { projectId, dataset, table, location } = getBigQueryConfig();
  const fullTable = `\`${projectId}.${dataset}.${table}\``;
  const pc = normalizeCodePostal(codePostal);
  const pcAlt = pc.startsWith("0") ? pc.slice(1) : pc;

  let whereClause: string;
  const params: Record<string, string> = { pc, pcAlt: pcAlt || "0" };

  const vf = `CAST(${COLS.valeur_fonciere} AS STRING)`;
  if (useCommuneFallback && commune && commune.trim()) {
    whereClause = `
      LOWER(TRIM(CAST(${COLS.nature_mutation} AS STRING))) = 'vente'
      AND LOWER(TRIM(CAST(${COLS.commune} AS STRING))) = LOWER(TRIM(@commune))
      AND SAFE_CAST(REPLACE(REPLACE(TRIM(${vf}), ' ', ''), ',', '.') AS FLOAT64) > 0
    `;
    params.commune = commune.trim();
  } else {
    whereClause = `
      LOWER(TRIM(CAST(${COLS.nature_mutation} AS STRING))) = 'vente'
      AND (TRIM(CAST(${COLS.code_postal} AS STRING)) = @pc OR TRIM(CAST(${COLS.code_postal} AS STRING)) = @pcAlt)
      AND SAFE_CAST(REPLACE(REPLACE(TRIM(${vf}), ' ', ''), ',', '.') AS FLOAT64) > 0
    `;
  }

  const query = `
    SELECT AVG(SAFE_CAST(REPLACE(REPLACE(TRIM(${vf}), ' ', ''), ',', '.') AS FLOAT64)) AS avg_value
    FROM ${fullTable}
    WHERE ${whereClause}
  `;

  const [rows] = await client.query({ query, params, location });
  const row = (rows as { avg_value: number | null }[])?.[0];
  const avg = row?.avg_value;
  const avgEuros = avg != null && Number.isFinite(avg) ? (avg >= 1e7 ? avg / 100 : avg) : null;
  console.log("[BigQuery] Area average query returned:", avgEuros != null ? avgEuros.toFixed(0) : "null", useCommuneFallback ? "(commune fallback)" : "");
  return avgEuros;
}

/**
 * LIVABILITY: Uses avg price/m², transaction density, avg transaction value.
 */
async function queryLivabilityStats(
  codePostal: string,
  commune: string | null,
  useCommuneFallback: boolean
): Promise<{ avgPriceM2: number; avgValue: number; txCount: number }> {
  const client = getBigQueryClient();
  const { projectId, dataset, table, location } = getBigQueryConfig();
  const fullTable = `\`${projectId}.${dataset}.${table}\``;
  const pc = normalizeCodePostal(codePostal);
  const pcAlt = pc.startsWith("0") ? pc.slice(1) : pc;

  let whereClause: string;
  const params: Record<string, string> = { pc, pcAlt: pcAlt || "0" };

  const vf = `CAST(${COLS.valeur_fonciere} AS STRING)`;
  const srb = `CAST(${COLS.surface_reelle_bati} AS STRING)`;
  if (useCommuneFallback && commune && commune.trim()) {
    whereClause = `
      LOWER(TRIM(CAST(${COLS.nature_mutation} AS STRING))) = 'vente'
      AND LOWER(TRIM(CAST(${COLS.commune} AS STRING))) = LOWER(TRIM(@commune))
    `;
    params.commune = commune.trim();
  } else {
    whereClause = `
      LOWER(TRIM(CAST(${COLS.nature_mutation} AS STRING))) = 'vente'
      AND (TRIM(CAST(${COLS.code_postal} AS STRING)) = @pc OR TRIM(CAST(${COLS.code_postal} AS STRING)) = @pcAlt)
    `;
  }

  const query = `
    SELECT
      AVG(SAFE_CAST(REPLACE(REPLACE(TRIM(${vf}), ' ', ''), ',', '.') AS FLOAT64) / NULLIF(SAFE_CAST(REPLACE(REPLACE(TRIM(${srb}), ' ', ''), ',', '.') AS FLOAT64), 0)) AS avg_price_m2,
      AVG(SAFE_CAST(REPLACE(REPLACE(TRIM(${vf}), ' ', ''), ',', '.') AS FLOAT64)) AS avg_value,
      COUNT(*) AS tx_count
    FROM ${fullTable}
    WHERE ${whereClause}
      AND SAFE_CAST(REPLACE(REPLACE(TRIM(${vf}), ' ', ''), ',', '.') AS FLOAT64) > 0
      AND SAFE_CAST(REPLACE(REPLACE(TRIM(${srb}), ' ', ''), ',', '.') AS FLOAT64) > 0
  `;

  const [rows] = await client.query({ query, params, location });
  const row = (rows as { avg_price_m2: number; avg_value: number; tx_count: number }[])?.[0];
  const rawPriceM2 = row?.avg_price_m2 ?? 0;
  const rawAvgValue = row?.avg_value ?? 0;
  return {
    avgPriceM2: rawPriceM2 >= 1e5 ? rawPriceM2 / 100 : rawPriceM2,
    avgValue: rawAvgValue >= 1e7 ? rawAvgValue / 100 : rawAvgValue,
    txCount: Number(row?.tx_count ?? 0),
  };
}

function classifyLivability(
  avgPriceM2: number,
  avgValue: number,
  txCount: number
): "Low" | "Medium" | "High" | "Premium" {
  if (avgPriceM2 <= 0 && avgValue <= 0) return "Medium";
  const score = (avgPriceM2 / 1000) * 0.5 + (avgValue / 100000) * 0.3 + Math.min(txCount / 50, 1) * 20;
  if (score >= 80) return "Premium";
  if (score >= 50) return "High";
  if (score >= 25) return "Medium";
  return "Low";
}

function mapLivabilityToRating(liv: "Low" | "Medium" | "High" | "Premium"): "POOR" | "FAIR" | "GOOD" | "VERY GOOD" | "EXCELLENT" {
  switch (liv) {
    case "Premium": return "EXCELLENT";
    case "High": return "VERY GOOD";
    case "Medium": return "GOOD";
    case "Low": return "FAIR";
    default: return "FAIR";
  }
}

/**
 * Main entry: get full France property result.
 * 1. Estimated property value (avg_price_m2 * surface)
 * 2. Last transaction (date + price)
 * 3. Area average value
 * 4. Livability score
 */
export async function getFrancePropertyResult(
  codePostal: string,
  noVoie: string | null,
  voie: string | null,
  commune: string | null,
  lotNumber: string | null,
  surfaceM2: number | null
): Promise<FrancePropertyResult> {
  const config = getBigQueryConfig();
  const { projectId, dataset, table } = config;
  let effectiveCommune: string | null = normalizeParisCommune(codePostal, commune);
  if (effectiveCommune == null) effectiveCommune = commune?.trim() || null;
  const originalCommune = commune?.trim() || null;
  if (effectiveCommune !== originalCommune) {
    console.log("[BigQuery] Paris commune normalized:", { from: originalCommune, to: effectiveCommune });
  }
  const normalizedLot = normalizeLot(lotNumber);
  console.log("[BigQuery config]", { projectId, dataset, table });
  console.log("[BigQuery] Schema columns used:", Object.keys(COLS).slice(0, 5).map((k) => `${k} -> ${COLS[k as keyof typeof COLS]}`).join(", ") + ", ...");
  console.log("[BigQuery] getFrancePropertyResult:", { codePostal, noVoie, voie, commune: effectiveCommune, lotNumber: lotNumber ?? "(empty)", normalizedLot: normalizedLot || "(empty)" });

  const fastValidate = process.env.FR_FAST_VALIDATE === "1";
  if (!fastValidate) {
    const select1Result = await runSelect1Test();
    if (!select1Result.ok) {
      console.error("[BigQuery] SELECT 1 failed, aborting:", select1Result.error);
      throw new Error(`BigQuery SELECT 1 test failed: ${select1Result.error}`);
    }

    const countResult = await runCount06000Test();
    if (countResult.error) {
      console.error("[BigQuery] COUNT 06000 failed:", countResult.error);
      throw new Error(`BigQuery COUNT 06000 test failed: ${countResult.error}`);
    }
    console.log("[BigQuery] COUNT 06000 OK, n =", countResult.n);

    await logSchemaColumns();

    if (effectiveCommune) {
      await logSampleRows(effectiveCommune);
    }
  }

  const normalizedStreet = voie ? normalizeForMatch(voie) : "";
  const stages: MatchStage[] = [1, 2, 3, 4, 5];
  let lastTxRows: FranceTransactionRow[] = [];
  let matchedStage: MatchStage = 5;
  const usePostcodeForArea = !!codePostal?.trim();
  const useCommuneFallback = !usePostcodeForArea;

  const isParis = /^75\d{3}$/.test((codePostal || "").trim());
  if (isParis) {
    console.log("[BigQuery] Paris debug: postcode=", codePostal, "commune=", effectiveCommune, "noVoie=", noVoie, "normalizedStreet=", normalizedStreet);
  }

  for (const stage of stages) {
    if (stage >= 3 && !effectiveCommune) continue;
    if (stage <= 4 && !normalizedStreet) continue;
    const lotForQuery = normalizedLot || null;
    try {
      const rows = await queryLastTransactionByStage(stage, codePostal, effectiveCommune, noVoie, voie, lotForQuery, normalizedStreet);
      if (isParis) console.log(`[BigQuery] Paris stage ${stage} rows:`, rows.length);
      console.log(`[BigQuery] Stage ${stage} rows:`, rows.length);
      if (rows.length > 0) {
        lastTxRows = rows;
        matchedStage = stage;
        console.log(`[BigQuery] Matched at stage ${stage}, rows = ${rows.length}`);
        break;
      }
    } catch (err) {
      logBigQueryError(`queryLastTransactionByStage(${stage})`, err);
    }
  }

  const [avgPriceM2, areaAvg, livabilityStats] = await Promise.all([
    queryAvgPricePerM2(codePostal, effectiveCommune, useCommuneFallback),
    queryAreaAverage(codePostal, effectiveCommune, useCommuneFallback),
    queryLivabilityStats(codePostal, effectiveCommune, useCommuneFallback),
  ]);

  const livability = classifyLivability(
    livabilityStats.avgPriceM2,
    livabilityStats.avgValue,
    livabilityStats.txCount
  );

  const primaryRows = lastTxRows.filter((r) => isPrimaryUnit(r.type_local));
  const excludedRows = lastTxRows.filter((r) => isExcludedType(r.type_local));
  const displayRowsBase = primaryRows.length > 0 ? primaryRows : lastTxRows;

  const lotsWithValue = primaryRows.filter((r) => {
    const n = normalizeLot(r.lot_1er);
    return n !== "";
  });
  const distinctLots = new Set(lotsWithValue.map((r) => normalizeLot(r.lot_1er)));
  const unitCount = distinctLots.size > 0 ? distinctLots.size : primaryRows.length;
  const availableLots = Array.from(distinctLots).sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));

  const lotsBeforeExactLotFilter = [...new Set(primaryRows.map((r) => normalizeLot(r.lot_1er)).filter(Boolean))];
  const lotFilteredRows =
    normalizedLot && matchedStage >= 2
      ? primaryRows.filter((r) => lotMatches(normalizeLot(r.lot_1er), normalizedLot))
      : [];
  const lotsAfterExactLotFilter = lotFilteredRows.length > 0 ? [...new Set(lotFilteredRows.map((r) => normalizeLot(r.lot_1er)).filter(Boolean))] : [];

  const lotMatchedFromBuildingRows = lotFilteredRows.length > 0;
  const exactLotRows = normalizedLot
    ? (matchedStage === 1 ? lastTxRows : lotFilteredRows)
    : [];
  const exactLotRowCount = exactLotRows.length;
  const hasExactLotMatch = !!(normalizedLot && (matchedStage === 1 || lotMatchedFromBuildingRows));
  const isBuildingMatch = lastTxRows.length > 0 && matchedStage <= 4;
  const resultLevel: ResultLevel = hasExactLotMatch
    ? "exact_property"
    : isBuildingMatch
      ? "building"
      : "commune_fallback";

  const displayRows = lotMatchedFromBuildingRows ? lotFilteredRows : displayRowsBase;

  if (normalizedLot) {
    const valueLevel = hasExactLotMatch ? "exact_property" : lotMatchedFromBuildingRows ? "exact_property" : "building-level";
    console.log("[BigQuery] Lot match debug:", {
      inputLot: lotNumber ?? "(empty)",
      normalizedLot,
      lotsInRecentSales: availableLots,
      lotsBeforeExactLotFilter: lotsBeforeExactLotFilter,
      lotsAfterExactLotFilter: lotsAfterExactLotFilter,
      matchedStage,
      result_level: resultLevel,
      value_level: valueLevel,
      lotMatchedFromBuildingRows,
    });
    if (availableLots.some((l) => lotMatches(l, normalizedLot)) && !hasExactLotMatch) {
      console.error("[BigQuery] ASSERTION: requested lot is in Recent Sales but exact-lot path did not match. This should not happen.");
    }
  }

  console.log("[BigQuery] Match result:", { matchStage: matchedStage, rowsAtStage: lastTxRows.length, resultLevel, hasExactLotMatch });

  const apartmentNotMatched = !!(normalizedLot && matchedStage >= 2 && exactLotRowCount === 0);
  const multipleUnits = lastTxRows.length > 0 && matchedStage >= 2 && !normalizedLot;

  const twoYearsAgo = new Date();
  twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);
  const twoYearsAgoStr = twoYearsAgo.toISOString().slice(0, 10);

  const buildBuildingSales = (rows: FranceTransactionRow[], onlyPrimary = false, onlyLast2Y = false) => {
    let filtered = rows.filter((r) => {
      const surface = parseFrenchNumber(r.surface_reelle_bati) || null;
      return valueFonciereToEurosSafe(parseFrenchNumber(r.valeur_fonciere), surface) > 0;
    });
    if (onlyPrimary) filtered = filtered.filter((r) => isPrimaryUnit(r.type_local));
    if (onlyLast2Y) filtered = filtered.filter((r) => (parseFrenchDate(r.date_mutation) ?? "") >= twoYearsAgoStr);
    return filtered
      .sort((a, b) => (parseFrenchDate(b.date_mutation) ?? "").localeCompare(parseFrenchDate(a.date_mutation) ?? ""))
      .slice(0, 5)
      .map((r) => ({
        date: parseFrenchDate(r.date_mutation),
        type: String(r.type_local ?? "—").trim() || "—",
        price: Math.round(valueFonciereToEurosSafe(parseFrenchNumber(r.valeur_fonciere), parseFrenchNumber(r.surface_reelle_bati) || null)),
        surface: parseFrenchNumber(r.surface_reelle_bati) || null,
        lot_number: normalizeLot(r.lot_1er) || null,
      }));
  };

  const pickSimilarApartmentInBuilding = (
    rows: FranceTransactionRow[],
    requestedLotRaw: string,
    preferredType: "appartement" | "maison" | null
  ): { picked: FrancePropertyResult["similarApartment"]; candidateCount: number; similarityScore: number | null } => {
    const requested = normalizeLot(requestedLotRaw);
    const candidates = rows
      .filter((r) => isPrimaryUnit(r.type_local))
      .map((r) => {
        const surface = parseFrenchNumber(r.surface_reelle_bati) || null;
        const value = valueFonciereToEurosSafe(parseFrenchNumber(r.valeur_fonciere), surface);
        const date = parseFrenchDate(r.date_mutation);
        const lot = normalizeLot(r.lot_1er) || null;
        const typeLocal = (r.type_local ?? null) as string | null;
        return { value, surface, date, lot, typeLocal };
      })
      .filter((c) => c.value > 0)
      .filter((c) => !requested || !c.lot || !lotMatches(c.lot, requested));

    if (candidates.length === 0) return { picked: null, candidateCount: 0, similarityScore: null };

    const canonicalType = (t: string | null) => (t ?? "").trim().toLowerCase();
    const sameTypeCandidates =
      preferredType != null
        ? candidates.filter((c) => canonicalType(c.typeLocal) === preferredType)
        : candidates;

    if (preferredType && sameTypeCandidates.length === 0) {
      // Building context says appartement/maison, but we have no same-type candidates.
      // Be conservative: don't fabricate a cross-type 'similar apartment' result.
      return { picked: null, candidateCount: candidates.length, similarityScore: null };
    }

    const pool = sameTypeCandidates
      .filter((c) => c.surface != null && c.surface > 0)
      .filter((c) => !!c.date);

    // Strict validation: range gate + outlier rejection vs median, require min sample size.
    const validated = filterCompsByPricePerSqmValidation(pool as any);
    const validPool = validated.kept as typeof pool;
    if (validPool.length < 2) {
      return { picked: null, candidateCount: validPool.length, similarityScore: null };
    }

    // Prefer recency among the validated pool.
    const scored = validPool.map((c) => {
      const recency = c.date ? Date.parse(c.date) : 0;
      const score = Math.min(999999999, recency);
      return { c, score };
    });
    scored.sort((a, b) => b.score - a.score);
    const best = scored[0]!;

    return {
      picked: {
        lotNumber: best.c.lot,
        date: best.c.date,
        value: Math.round(best.c.value),
        surface: best.c.surface,
        typeLocal: best.c.typeLocal,
      },
      candidateCount: validPool.length,
      similarityScore: null,
    };
  };

  if (multipleUnits) {
    const avgRows = primaryRows.filter((r) => (parseFrenchDate(r.date_mutation) ?? "") >= twoYearsAgoStr);
    const avgValue = avgRows.length > 0
      ? avgRows.reduce((s, r) => s + valueFonciereToEurosSafe(parseFrenchNumber(r.valeur_fonciere), parseFrenchNumber(r.surface_reelle_bati) || null), 0) / avgRows.length
      : primaryRows.reduce((s, r) => s + valueFonciereToEurosSafe(parseFrenchNumber(r.valeur_fonciere), parseFrenchNumber(r.surface_reelle_bati) || null), 0) / Math.max(primaryRows.length, 1);
    return {
      currentValue: null,
      lastTransaction: null,
      areaAverageValue: areaAvg,
      livabilityStandard: livability,
      multipleUnits: true,
      unitCount,
      averageBuildingValue: Math.round(avgValue),
      buildingSales: buildBuildingSales(primaryRows, true, true),
      surfaceReelleBati: null,
      lotNumber: null,
      requestedLot: null,
      exactLotRowCount: 0,
      matchStage: matchedStage,
      resultLevel: "building",
      rowsAtStage: lastTxRows.length,
      availableLots,
    };
  }

  if (matchedStage === 5 && lastTxRows.length === 0) {
    // Fail-safe: even when we can't match the building, try street+postcode comparables if we have a street.
    let nearbyPicked: FrancePropertyResult["nearbyComparable"] = null;
    let candidateCountNearby = 0;
    const nearbyStageCounts = { same_street: 0, same_postcode_commune: 0, same_commune: 0 };
    let selectedNearbyStrategy: NearbyStrategy | null = null;
    let selectedScope: NearbyScope | null = null;
    if (normalizedStreet && codePostal?.trim()) {
      try {
        let nearbyRows: FranceTransactionRow[] = [];
        // Stage A
        nearbyRows = await queryNearbyComparableOnStreet(codePostal, normalizedStreet, noVoie);
        nearbyStageCounts.same_street = nearbyRows.length;
        if (nearbyRows.length > 0) {
          selectedNearbyStrategy = "street_postcode";
          selectedScope = "same_street";
        }
        // Stage B
        if (nearbyRows.length === 0 && effectiveCommune?.trim()) {
          nearbyRows = await queryNearbyComparableInPostcodeAndCommune(codePostal, String(effectiveCommune));
          nearbyStageCounts.same_postcode_commune = nearbyRows.length;
          if (nearbyRows.length > 0) {
            selectedNearbyStrategy = "postcode_commune";
            selectedScope = "same_postcode_commune";
          }
        }
        // Stage C
        if (nearbyRows.length === 0 && effectiveCommune?.trim()) {
          nearbyRows = await queryNearbyComparableInCommune(String(effectiveCommune));
          nearbyStageCounts.same_commune = nearbyRows.length;
          if (nearbyRows.length > 0) {
            selectedNearbyStrategy = "commune";
            selectedScope = "same_commune";
          }
        }
        const candidates = nearbyRows
          .map((r) => {
            const surface = parseFrenchNumber(r.surface_reelle_bati) || null;
            const value = valueFonciereToEurosSafe(parseFrenchNumber(r.valeur_fonciere), surface);
            const date = parseFrenchDate(r.date_mutation);
            const typeLocal = (r.type_local ?? null) as string | null;
            return { value, surface, date, typeLocal, row: r };
          })
          .filter((c) => c.value > 0)
          .filter((c) => c.surface != null && c.surface > 0)
          .filter((c) => !!c.date);
        candidateCountNearby = candidates.length;
        const validated = filterCompsByPricePerSqmValidation(candidates);
        const validCandidates = validated.kept;
        if (validCandidates.length >= 2) {
          const scopeForRanking: NearbyScope = selectedScope ?? "same_commune";
          validCandidates.sort((a, b) => {
            const as = rankNearbyCandidate({
              candidateType: a.typeLocal,
              candidateSurface: a.surface,
              candidateDate: a.date,
              scope: scopeForRanking,
              preferredType: null,
              targetSurface: null,
            });
            const bs = rankNearbyCandidate({
              candidateType: b.typeLocal,
              candidateSurface: b.surface,
              candidateDate: b.date,
              scope: scopeForRanking,
              preferredType: null,
              targetSurface: null,
            });
            if (bs !== as) return bs - as;
            return (b.date ?? "").localeCompare(a.date ?? "");
          });
          const best = validCandidates[0];
          if (best) {
            nearbyPicked = {
              address: makeNearbyAddress(best.row, String(codePostal ?? ""), String(effectiveCommune ?? "")),
              lotNumber: null,
              date: best.date ?? null,
              value: Math.round(best.value),
              surface: best.surface,
              typeLocal: best.typeLocal,
              scope: scopeForRanking,
              selectedNearbyStrategy: selectedNearbyStrategy ?? "commune",
            };
          }
        }
      } catch (e) {
        console.log("[BigQuery] Nearby comparable fallback failed:", (e as Error)?.message);
      }
    }
    return {
      currentValue: null,
      lastTransaction: null,
      areaAverageValue: areaAvg,
      livabilityStandard: livability,
      multipleUnits: false,
      buildingSales: [],
      surfaceReelleBati: null,
      lotNumber: null,
      requestedLot: normalizedLot || null,
      exactLotRowCount: 0,
      matchStage: 5,
      resultLevel: "commune_fallback",
      rowsAtStage: 0,
      nearbyComparable: nearbyPicked,
      debug: {
        candidateCountNearby,
        reliableCandidateCountNearby: nearbyPicked ? 2 : 0,
        nearbyStageCounts,
        selectedNearbyStrategy,
      },
    };
  }

  if (apartmentNotMatched) {
    const avgRows = primaryRows.filter((r) => (parseFrenchDate(r.date_mutation) ?? "") >= twoYearsAgoStr);
    const avgValue = avgRows.length > 0
      ? avgRows.reduce((s, r) => s + valueFonciereToEurosSafe(parseFrenchNumber(r.valeur_fonciere), parseFrenchNumber(r.surface_reelle_bati) || null), 0) / avgRows.length
      : primaryRows.reduce((s, r) => s + valueFonciereToEurosSafe(parseFrenchNumber(r.valeur_fonciere), parseFrenchNumber(r.surface_reelle_bati) || null), 0) / Math.max(primaryRows.length, 1);
    const preferredTypeForBuilding = (() => {
      const types = primaryRows
        .map((r) => String(r.type_local ?? "").trim().toLowerCase())
        .filter((t) => t === "appartement" || t === "maison");
      if (types.length === 0) return null;
      const unique = Array.from(new Set(types));
      if (unique.length === 1) return unique[0] as "appartement" | "maison";
      // Mixed building: type context is ambiguous, do not assume.
      return null;
    })();
    const sim = pickSimilarApartmentInBuilding(primaryRows, normalizedLot, preferredTypeForBuilding);

    // Nearby staged search (only when similar-in-building is not trustworthy).
    const shouldTryNearby = sim.picked == null;
    const nearbyStageCounts = { same_street: 0, same_postcode_commune: 0, same_commune: 0 };
    let selectedNearbyStrategy: NearbyStrategy | null = null;
    let selectedScope: NearbyScope | null = null;
    let nearbyRows: FranceTransactionRow[] = [];
    if (shouldTryNearby) {
      // Stage A: same street + same postcode
      if (normalizedStreet?.trim() && codePostal?.trim()) {
        nearbyRows = await queryNearbyComparableOnStreet(codePostal, normalizedStreet, noVoie);
        nearbyStageCounts.same_street = nearbyRows.length;
        if (nearbyRows.length > 0) {
          selectedNearbyStrategy = "street_postcode";
          selectedScope = "same_street";
        }
      }
      // Stage B: same postcode + same commune
      if (nearbyRows.length === 0 && codePostal?.trim() && effectiveCommune?.trim()) {
        nearbyRows = await queryNearbyComparableInPostcodeAndCommune(codePostal, String(effectiveCommune));
        nearbyStageCounts.same_postcode_commune = nearbyRows.length;
        if (nearbyRows.length > 0) {
          selectedNearbyStrategy = "postcode_commune";
          selectedScope = "same_postcode_commune";
        }
      }
      // Stage C: same commune (broadest)
      if (nearbyRows.length === 0 && effectiveCommune?.trim()) {
        nearbyRows = await queryNearbyComparableInCommune(String(effectiveCommune));
        nearbyStageCounts.same_commune = nearbyRows.length;
        if (nearbyRows.length > 0) {
          selectedNearbyStrategy = "commune";
          selectedScope = "same_commune";
        }
      }
    }
    const tenYearsAgo = new Date();
    tenYearsAgo.setFullYear(tenYearsAgo.getFullYear() - 10);
    const tenYearsAgoStr = tenYearsAgo.toISOString().slice(0, 10);
    const preferredType = (() => {
      const types = primaryRows
        .map((r) => String(r.type_local ?? "").trim().toLowerCase())
        .filter((t) => t === "appartement" || t === "maison");
      if (types.length === 0) return null;
      const apt = types.filter((t) => t === "appartement").length;
      const house = types.filter((t) => t === "maison").length;
      return apt >= house ? "appartement" : "maison";
    })();
    const targetSurface = medianNumber(
      primaryRows
        .map((r) => parseFrenchNumber(r.surface_reelle_bati))
        .filter((n): n is number => typeof n === "number" && n > 0)
    );

    const nearbyCandidates = nearbyRows
      .map((r) => {
        const surface = parseFrenchNumber(r.surface_reelle_bati) || null;
        const value = valueFonciereToEurosSafe(parseFrenchNumber(r.valeur_fonciere), surface);
        const date = parseFrenchDate(r.date_mutation);
        const lot = normalizeLot(r.lot_1er) || null;
        const typeLocal = (r.type_local ?? null) as string | null;
        return { value, surface, date, lot, typeLocal, row: r };
      })
      .filter((c) => c.value > 0);
    // Guardrails: require surface + a parsable date, and avoid stale comps.
    const nearbyFilterStats = {
      raw: nearbyCandidates.length,
      missingSurface: 0,
      missingDate: 0,
      tooOld: 0,
      trustworthy: 0,
    };
    for (const c of nearbyCandidates) {
      if (!(c.surface != null && c.surface > 0)) nearbyFilterStats.missingSurface += 1;
      if (!c.date) nearbyFilterStats.missingDate += 1;
      if (c.date && c.date < tenYearsAgoStr) nearbyFilterStats.tooOld += 1;
    }
    let nearbyFiltered = nearbyCandidates
      .filter((c) => c.surface != null && c.surface > 0)
      .filter((c) => !!c.date && (c.date ?? "") >= tenYearsAgoStr);
    nearbyFilterStats.trustworthy = nearbyFiltered.length;
    const scopeForRanking: NearbyScope = selectedScope ?? "same_commune";
    // For nearby: if we have a clear preferredType, require same-type comps when they exist.
    if (preferredType && nearbyFiltered.length > 0) {
      const sameType = nearbyFiltered.filter(
        (c) => String(c.typeLocal ?? "").trim().toLowerCase() === preferredType
      );
      if (sameType.length > 0) {
        nearbyFiltered = sameType;
      } else {
        // No trustworthy same-type nearby comps: be conservative and skip nearby comparable.
        nearbyFiltered = [];
      }
    }
    nearbyFiltered.sort((a, b) => {
      const as = rankNearbyCandidate({
        candidateType: a.typeLocal,
        candidateSurface: a.surface,
        candidateDate: a.date,
        scope: scopeForRanking,
        preferredType,
        targetSurface,
      });
      const bs = rankNearbyCandidate({
        candidateType: b.typeLocal,
        candidateSurface: b.surface,
        candidateDate: b.date,
        scope: scopeForRanking,
        preferredType,
        targetSurface,
      });
      if (bs !== as) return bs - as;
      return (b.date ?? "").localeCompare(a.date ?? "");
    });
    const validatedNearby = filterCompsByPricePerSqmValidation(nearbyFiltered as any);
    nearbyFiltered = (validatedNearby.kept as any) ?? [];
    const nearbyPicked = nearbyFiltered.length >= 2
      ? {
          address: makeNearbyAddress(nearbyFiltered[0].row, String(codePostal ?? ""), String(effectiveCommune ?? "")),
          lotNumber: nearbyFiltered[0].lot,
          date: nearbyFiltered[0].date,
          value: Math.round(nearbyFiltered[0].value),
          surface: nearbyFiltered[0].surface,
          typeLocal: nearbyFiltered[0].typeLocal,
          scope: scopeForRanking,
          selectedNearbyStrategy: selectedNearbyStrategy ?? "commune",
        }
      : null;
    return {
      currentValue: Math.round(avgValue),
      lastTransaction: null,
      areaAverageValue: areaAvg,
      livabilityStandard: livability,
      multipleUnits: false,
      averageBuildingValue: Math.round(avgValue),
      buildingSales: buildBuildingSales(primaryRows, true, false),
      surfaceReelleBati: null,
      lotNumber: null,
      requestedLot: normalizedLot || null,
      exactLotRowCount: 0,
      matchStage: matchedStage,
      resultLevel: "building",
      rowsAtStage: lastTxRows.length,
      apartmentNotMatched: true,
      availableLots,
      similarApartment: sim.picked,
      nearbyComparable: nearbyPicked,
      debug: {
        candidateCountSameBuilding: sim.candidateCount,
        candidateCountNearby: nearbyCandidates.length,
        reliableCandidateCountSameBuilding: sim.candidateCount,
        reliableCandidateCountNearby: nearbyFiltered.length,
        similarityScore: sim.similarityScore,
        nearbyStageCounts,
        selectedNearbyStrategy,
        nearbyFilterStats,
      },
    };
  }

  const match = displayRows[0] ?? lastTxRows[0];
  const lastTxValue = match ? valueFonciereToEurosSafe(parseFrenchNumber(match.valeur_fonciere), parseFrenchNumber(match.surface_reelle_bati) || null) : 0;
  const lastTxDate = match ? parseFrenchDate(match.date_mutation) : null;
  const surface = surfaceM2 ?? (match ? parseFrenchNumber(match.surface_reelle_bati) || null : null);

  const estimatedValue =
    avgPriceM2 != null && surface != null && surface > 0
      ? Math.round(avgPriceM2 * surface)
      : lastTxValue > 0
        ? Math.round(lastTxValue)
        : null;

  return {
    currentValue: estimatedValue,
    lastTransaction: lastTxValue > 0 ? { date: lastTxDate, value: lastTxValue } : null,
    areaAverageValue: areaAvg,
    livabilityStandard: livability,
    multipleUnits: false,
    buildingSales: buildBuildingSales(primaryRows.length > 0 ? primaryRows : displayRows),
    surfaceReelleBati: surface,
    lotNumber: match ? (normalizeLot(match.lot_1er) || null) : null,
    requestedLot: normalizedLot || null,
    exactLotRowCount: normalizedLot ? exactLotRowCount : 0,
    matchStage: matchedStage,
    resultLevel,
    rowsAtStage: lastTxRows.length,
  };
}

export { mapLivabilityToRating };
