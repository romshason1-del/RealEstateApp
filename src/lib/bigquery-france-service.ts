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
  matchStage?: MatchStage;
  resultLevel?: ResultLevel;
  rowsAtStage?: number;
  /** True when user provided apartment number but no matching lot found in DVF */
  apartmentNotMatched?: boolean;
  /** Available lot numbers in this building (for prompt) */
  availableLots?: string[];
};

const EXCLUDED_TYPES = ["dépendance", "local industriel", "parking"];
const PRIMARY_TYPES = ["appartement", "maison", ""];

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

function parseFrenchDate(s: string | null | undefined): string | null {
  if (!s || !String(s).trim()) return null;
  const m = String(s).trim().match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!m) return null;
  return `${m[3]}-${m[2]}-${m[1]}`;
}

function isExcludedType(typeLocal: string | null | undefined): boolean {
  const t = (typeLocal ?? "").toLowerCase().trim();
  return EXCLUDED_TYPES.includes(t);
}

function isPrimaryUnit(typeLocal: string | null | undefined): boolean {
  const t = (typeLocal ?? "").toLowerCase().trim();
  return PRIMARY_TYPES.includes(t) || t === "appartement" || t === "maison";
}

/** Normalize lot/apartment number for matching and display: string, trimmed, comparable ("9" vs 9). */
export function normalizeLot(val: string | number | null | undefined): string {
  if (val === null || val === undefined) return "";
  return String(val).trim();
}

/** Canonical form for lot comparison: "9" and "09" match. */
function lotMatches(lotInRow: string, requestedLot: string): boolean {
  if (!requestedLot) return false;
  const a = normalizeLot(lotInRow);
  const b = normalizeLot(requestedLot);
  if (a === b) return true;
  const canonical = (s: string) => (s.replace(/^0+/, "") || s);
  return canonical(a) === canonical(b);
}

/** Normalize string for address matching: uppercase, trim, collapse spaces, basic accent fold. */
function normalizeForMatch(s: string | null | undefined): string {
  if (!s || typeof s !== "string") return "";
  return s
    .trim()
    .toUpperCase()
    .replace(/\s+/g, " ")
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .trim();
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
  return `UPPER(${concat})`;
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

  const baseConditions: string[] = [`LOWER(TRIM(CAST(${COLS.nature_mutation} AS STRING))) = 'vente'`];
  const params: Record<string, string> = { pc, pcAlt: pcAlt || "0", streetNormValue: normalizedStreet };

  if (stage <= 4 && normalizedStreet) {
    baseConditions.push(`${streetNorm} = @streetNormValue`);
  }
  if (stage <= 3 && noVoie && noVoie.trim()) {
    baseConditions.push(`(TRIM(CAST(${COLS.no_voie} AS STRING)) = @noVoie OR TRIM(CAST(${COLS.no_voie} AS STRING)) = CONCAT('0', @noVoie))`);
    params.noVoie = noVoie.trim();
  }
  if (stage === 1 && lotNumber && lotNumber.trim()) {
    const lotNorm = String(lotNumber).trim();
    baseConditions.push(`(TRIM(CAST(${COLS.lot_1er} AS STRING)) = @lotNumber OR TRIM(CAST(${COLS.lot_1er} AS STRING)) = CONCAT('0', @lotNumber))`);
    params.lotNumber = lotNorm;
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

  const [rows] = await client.query({ query, params, location });
  return (rows as FranceTransactionRow[]) || [];
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

  const apartmentNotMatched = !!(normalizedLot && matchedStage >= 2 && !lotMatchedFromBuildingRows);
  const multipleUnits = lastTxRows.length > 0 && matchedStage >= 2 && !normalizedLot;

  const twoYearsAgo = new Date();
  twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);
  const twoYearsAgoStr = twoYearsAgo.toISOString().slice(0, 10);

  const buildBuildingSales = (rows: FranceTransactionRow[], onlyPrimary = false, onlyLast2Y = false) => {
    let filtered = rows.filter((r) => valueFonciereToEuros(parseFrenchNumber(r.valeur_fonciere)) > 0);
    if (onlyPrimary) filtered = filtered.filter((r) => isPrimaryUnit(r.type_local));
    if (onlyLast2Y) filtered = filtered.filter((r) => (parseFrenchDate(r.date_mutation) ?? "") >= twoYearsAgoStr);
    return filtered
      .sort((a, b) => (parseFrenchDate(b.date_mutation) ?? "").localeCompare(parseFrenchDate(a.date_mutation) ?? ""))
      .slice(0, 5)
      .map((r) => ({
        date: parseFrenchDate(r.date_mutation),
        type: String(r.type_local ?? "—").trim() || "—",
        price: Math.round(valueFonciereToEuros(parseFrenchNumber(r.valeur_fonciere))),
        surface: parseFrenchNumber(r.surface_reelle_bati) || null,
        lot_number: normalizeLot(r.lot_1er) || null,
      }));
  };

  if (multipleUnits) {
    const avgRows = primaryRows.filter((r) => (parseFrenchDate(r.date_mutation) ?? "") >= twoYearsAgoStr);
    const avgValue = avgRows.length > 0
      ? avgRows.reduce((s, r) => s + valueFonciereToEuros(parseFrenchNumber(r.valeur_fonciere)), 0) / avgRows.length
      : primaryRows.reduce((s, r) => s + valueFonciereToEuros(parseFrenchNumber(r.valeur_fonciere)), 0) / Math.max(primaryRows.length, 1);
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
      matchStage: matchedStage,
      resultLevel: "building",
      rowsAtStage: lastTxRows.length,
      availableLots,
    };
  }

  if (matchedStage === 5 && lastTxRows.length === 0) {
    return {
      currentValue: null,
      lastTransaction: null,
      areaAverageValue: areaAvg,
      livabilityStandard: livability,
      multipleUnits: false,
      buildingSales: [],
      surfaceReelleBati: null,
      lotNumber: null,
      matchStage: 5,
      resultLevel: "commune_fallback",
      rowsAtStage: 0,
    };
  }

  if (apartmentNotMatched) {
    const avgRows = primaryRows.filter((r) => (parseFrenchDate(r.date_mutation) ?? "") >= twoYearsAgoStr);
    const avgValue = avgRows.length > 0
      ? avgRows.reduce((s, r) => s + valueFonciereToEuros(parseFrenchNumber(r.valeur_fonciere)), 0) / avgRows.length
      : primaryRows.reduce((s, r) => s + valueFonciereToEuros(parseFrenchNumber(r.valeur_fonciere)), 0) / Math.max(primaryRows.length, 1);
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
      matchStage: matchedStage,
      resultLevel: "building",
      rowsAtStage: lastTxRows.length,
      apartmentNotMatched: true,
      availableLots,
    };
  }

  const match = displayRows[0] ?? lastTxRows[0];
  const lastTxValue = match ? valueFonciereToEuros(parseFrenchNumber(match.valeur_fonciere)) : 0;
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
    matchStage: matchedStage,
    resultLevel,
    rowsAtStage: lastTxRows.length,
  };
}

export { mapLivabilityToRating };
