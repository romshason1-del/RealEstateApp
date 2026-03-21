/**
 * Build France property_latest_facts from DVF source.
 *
 * Expands coverage for exact_house, exact_address, and building_similar_unit
 * by transforming DVF data into the property_latest_facts schema.
 *
 * Usage:
 *   npx tsx scripts/build-france-property-latest-facts.ts [dvf-file-or-dir]
 *   npx tsx scripts/build-france-property-latest-facts.ts ValeursFoncieres-2024.txt
 *   npx tsx scripts/build-france-property-latest-facts.ts .  # all ValeursFoncieres-*.txt in cwd
 *
 * Output:
 *   - output/property_latest_facts_france.ndjson (default)
 *   - Summary logs: total rows before/after, distinct city, street+house, address candidate counts
 *
 * Schema (property_latest_facts):
 *   country, city, postcode, street, house_number, unit_number (nullable),
 *   property_type, surface_m2, last_sale_date, last_sale_price, price_per_m2, data_source
 *
 * Money convention: last_sale_price and price_per_m2 stored in thousandths of euro (N ⇒ N/1000 €).
 */

import * as fs from "fs";
import * as path from "path";
import * as readline from "readline";

const PREFIXES = ["RUE", "AVENUE", "AV", "BD", "BOULEVARD", "CHEMIN", "CHE", "ROUTE", "IMPASSE", "IMP", "ALLEE", "ALL"];
const PREFIX_REGEX = new RegExp(`^(?:${PREFIXES.join("|")})\\.?\\s+`, "i");

function parsePipeLine(line: string): string[] {
  return line.split("|").map((s) => s.trim());
}

function parseFrenchNumber(s: string): number {
  if (!s || !s.trim()) return NaN;
  const normalized = s.replace(/\s/g, "").replace(",", ".");
  return parseFloat(normalized);
}

function parseFrenchDate(s: string): string | null {
  if (!s || !s.trim()) return null;
  const m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  return m ? `${m[3]}-${m[2]}-${m[1]}` : null;
}

function normalizeStreet(s: string): string {
  const unified = s
    .replace(/[\u2019\u2018\u02BC\u00B4\u0060]/g, "'")
    .replace(/[''']/g, "'")
    .replace(/\s+/g, " ")
    .trim()
    .toUpperCase()
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .replace(/[^A-Z0-9 ]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return unified.replace(PREFIX_REGEX, "").replace(/\s+/g, " ").trim();
}

function normalizeHouseNumber(s: string): string {
  return (s ?? "")
    .toString()
    .trim()
    .replace(/\s+/g, " ")
    .toUpperCase();
}

function mapTypeLocal(t: string | null): string {
  if (!t || !t.trim()) return "Local";
  const u = t.trim();
  const lower = u.toLowerCase();
  if (lower.includes("appart") || lower === "appartement") return "Appartement";
  if (lower.includes("maison") || lower.includes("villa") || lower.includes("pavillon")) return "Maison";
  if (lower.includes("dependance")) return "Dépendance";
  if (lower.includes("parking")) return "Parking";
  return u;
}

type GoldRow = {
  country: string;
  city: string;
  postcode: string;
  street: string;
  house_number: string;
  unit_number: string | null;
  property_type: string;
  surface_m2: number | null;
  last_sale_date: string | null;
  last_sale_price: number;
  price_per_m2: number | null;
  data_source: string;
};

/**
 * DVF lot columns (0-indexed): 1er lot, 2ème lot, 3ème lot, 4ème lot, 5ème lot
 */
const DVF_LOT_COLS = [24, 26, 28, 30, 32];

/**
 * Collect all non-empty lot values from a DVF row.
 * Uses 1er lot (col 24) through 5ème lot (col 32).
 */
function extractLotValues(cols: string[]): string[] {
  const lots: string[] = [];
  for (const idx of DVF_LOT_COLS) {
    const v = (cols[idx] ?? "").trim();
    if (v && !lots.includes(v)) lots.push(v);
  }
  return lots;
}

/**
 * Use Surface Carrez (cols 25,27,29,31,33) when surface_reelle_bati is null.
 * Carrez is typical for co-ownership apartments.
 */
function extractSurfaceM2(cols: string[]): number | null {
  const surfBati = parseFrenchNumber(cols[38] ?? "");
  if (Number.isFinite(surfBati) && surfBati > 0) return surfBati;
  for (const idx of [25, 27, 29, 31, 33]) {
    const carrez = parseFrenchNumber(cols[idx] ?? "");
    if (Number.isFinite(carrez) && carrez > 0) return carrez;
  }
  return null;
}

/**
 * Transform one DVF row into one or more GoldRows.
 * Emits one row per non-empty lot to preserve apartment/building coverage.
 * When all lots are empty (e.g. house), emits one row with unit_number=null.
 */
function dvfToGoldRows(cols: string[]): GoldRow[] {
  if (cols.length < 39) return [];

  const nature = (cols[9] ?? "").trim();
  if (nature !== "Vente") return [];

  const price = parseFrenchNumber(cols[10] ?? "");
  if (!Number.isFinite(price) || price <= 0) return [];

  const commune = (cols[17] ?? "").trim();
  const voie = (cols[15] ?? "").trim();
  const noVoie = (cols[11] ?? "").trim();
  const codePostal = (cols[16] ?? "").trim();

  if (!commune && !voie) return [];
  const streetRaw = voie || commune;
  const houseNumber = noVoie || "0";
  if (!streetRaw.trim()) return [];

  const dateStr = parseFrenchDate(cols[8] ?? "");
  const surfaceM2 = extractSurfaceM2(cols);
  const typeLocalRaw = (cols[36] ?? "").trim() || "Local";
  const propertyType = mapTypeLocal(typeLocalRaw);

  const lastSalePriceThousandths = Math.round(price * 1000);
  const pricePerM2 =
    surfaceM2 != null && surfaceM2 > 0 ? Math.round((price / surfaceM2) * 1000) : null;

  const lots = extractLotValues(cols);
  const base: Omit<GoldRow, "unit_number"> = {
    country: "FR",
    city: commune,
    postcode: codePostal,
    street: voie || commune,
    house_number: normalizeHouseNumber(houseNumber),
    property_type: propertyType,
    surface_m2: surfaceM2,
    last_sale_date: dateStr,
    last_sale_price: lastSalePriceThousandths,
    price_per_m2: pricePerM2,
    data_source: "dvf",
  };

  if (lots.length === 0) {
    return [{ ...base, unit_number: null }];
  }

  return lots.map((unitNumber) => ({ ...base, unit_number: unitNumber }));
}

function buildDedupKey(r: GoldRow): string {
  const streetNorm = normalizeStreet(r.street);
  const unitPart = r.unit_number ?? "";
  return `${r.country}\0${r.city}\0${r.postcode}\0${streetNorm}\0${r.house_number}\0${unitPart}`;
}

/**
 * Dedup: keep latest per (country, city, postcode, street_norm, house_number, unit_number).
 * Same key = same address+unit candidate. Most recent sale wins.
 * We do NOT collapse different units or different addresses.
 */
async function processFile(filePath: string): Promise<{ rawCount: number; goldRowsGenerated: number; dedupedRows: GoldRow[] }> {
  const keyToBest = new Map<string, GoldRow>();

  const stream = fs.createReadStream(filePath, { encoding: "utf-8" });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let lineNum = 0;
  let rawCount = 0;
  let goldRowsGenerated = 0;

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    lineNum++;
    if (lineNum === 1) continue;

    const cols = parsePipeLine(line);
    const goldRows = dvfToGoldRows(cols);
    if (goldRows.length === 0) continue;

    rawCount++;
    goldRowsGenerated += goldRows.length;

    for (const gold of goldRows) {
      const key = buildDedupKey(gold);
      const existing = keyToBest.get(key);
      const goldDate = gold.last_sale_date ? new Date(gold.last_sale_date).getTime() : 0;
      const existingDate = existing?.last_sale_date ? new Date(existing.last_sale_date).getTime() : 0;

      if (!existing || goldDate >= existingDate) {
        keyToBest.set(key, gold);
      }
    }
  }

  return { rawCount, goldRowsGenerated, dedupedRows: Array.from(keyToBest.values()) };
}

async function main() {
  try {
    const dotenv = await import("dotenv");
    dotenv.config({ path: path.join(process.cwd(), ".env.local") });
  } catch {
    /* optional */
  }

  const root = process.cwd();
  const input = process.argv[2];
  let files: string[] = [];

  if (!input || input === ".") {
    const dir = root;
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    for (const e of entries) {
      if (e.isFile() && e.name.match(/^ValeursFoncieres-\d{4}\.txt$/)) {
        files.push(path.join(dir, e.name));
      }
    }
    files.sort();
  } else if (fs.existsSync(input)) {
    const stat = fs.statSync(input);
    if (stat.isDirectory()) {
      const entries = fs.readdirSync(input, { withFileTypes: true });
      for (const e of entries) {
        if (e.isFile() && e.name.match(/^ValeursFoncieres-\d{4}\.txt$/)) {
          files.push(path.join(input, e.name));
        }
      }
      files.sort();
    } else {
      files = [path.resolve(input)];
    }
  } else {
    console.error("Usage: npx tsx scripts/build-france-property-latest-facts.ts [file-or-dir]");
    console.error("  Default: all ValeursFoncieres-*.txt in project root");
    process.exit(1);
  }

  if (files.length === 0) {
    console.error("No ValeursFoncieres-*.txt files found. Run: npm run download:dvf");
    process.exit(1);
  }

  console.log("[FR_FACTS] Input files:", files.length);
  for (const f of files) console.log("  -", path.basename(f));

  let totalBefore = 0;
  let totalGoldGenerated = 0;
  const keyToBest = new Map<string, GoldRow>();

  for (const f of files) {
    console.log("[FR_FACTS] Processing", path.basename(f), "...");
    const { rawCount, goldRowsGenerated, dedupedRows } = await processFile(f);
    totalBefore += rawCount;
    totalGoldGenerated += goldRowsGenerated;
    for (const r of dedupedRows) {
      const key = buildDedupKey(r);
      const existing = keyToBest.get(key);
      const rDate = r.last_sale_date ? new Date(r.last_sale_date).getTime() : 0;
      const exDate = existing?.last_sale_date ? new Date(existing.last_sale_date).getTime() : 0;
      if (!existing || rDate >= exDate) {
        keyToBest.set(key, r);
      }
    }
  }

  const goldRows = Array.from(keyToBest.values());
  const totalAfter = goldRows.length;

  const cities = new Set(goldRows.map((r) => r.city.trim().toLowerCase()));
  const streetHouse = new Set(
    goldRows.map((r) => `${r.city}|${r.postcode}|${normalizeStreet(r.street)}|${r.house_number}`.toLowerCase())
  );
  const addressCandidates = new Set(
    goldRows.map((r) => {
      const unitPart = r.unit_number ?? "";
      return `${r.city}|${r.postcode}|${normalizeStreet(r.street)}|${r.house_number}|${unitPart}`.toLowerCase();
    })
  );

  console.log("");
  console.log("[FR_FACTS] ========== SUMMARY ==========");
  console.log("[FR_FACTS] total_france_fact_rows_before=" + String(totalBefore));
  console.log("[FR_FACTS] total_gold_rows_generated=" + String(totalGoldGenerated));
  console.log("[FR_FACTS] total_france_fact_rows_after=" + String(totalAfter));
  console.log("[FR_FACTS] distinct_city_count=" + String(cities.size));
  console.log("[FR_FACTS] distinct_street_house_number_count=" + String(streetHouse.size));
  console.log("[FR_FACTS] distinct_apartment_address_candidate_count=" + String(addressCandidates.size));
  console.log("[FR_FACTS] ================================");
  console.log("");
  console.log("[FR_FACTS] DEDUP_KEY_BEFORE=country|city|postcode|street_norm|house_number|unit_number (unit from col 24 only)");
  console.log("[FR_FACTS] DEDUP_KEY_AFTER=country|city|postcode|street_norm|house_number|unit_number (unit from cols 24,26,28,30,32)");
  console.log("[FR_FACTS] LOT_COLUMNS_NOW_USED=1er lot (24), 2eme lot (26), 3eme lot (28), 4eme lot (30), 5eme lot (32)");
  console.log("[FR_FACTS] EMIT_STRATEGY=one row per non-empty lot (multi-lot DVF rows expand to multiple gold rows)");
  console.log("[FR_FACTS] SURFACE_FALLBACK=Surface Carrez (cols 25,27,29,31,33) when surface_reelle_bati (38) null");

  const outDir = path.join(root, "output");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, "property_latest_facts_france.ndjson");

  const out = fs.createWriteStream(outPath, { encoding: "utf-8" });
  for (let i = 0; i < goldRows.length; i++) {
    out.write(JSON.stringify(goldRows[i]) + "\n");
  }
  out.end();
  await new Promise<void>((res, rej) => {
    out.on("finish", res);
    out.on("error", rej);
  });

  console.log("[FR_FACTS] Wrote", outPath, "(", goldRows.length, "rows )");

  const houseCount = goldRows.filter((r) =>
    /maison|villa|pavillon|house/i.test(r.property_type)
  ).length;
  const aptCount = goldRows.filter((r) =>
    /appartement|appart/i.test(r.property_type)
  ).length;
  console.log("[FR_FACTS] Houses:", houseCount, "| Apartments:", aptCount);
  console.log("");
  console.log("[FR_FACTS] EXPECTED_IMPACT:");
  console.log("[FR_FACTS]   exact_house: more rows (unchanged key; more address coverage from multi-lot expansion)");
  console.log("[FR_FACTS]   exact_address: more rows (address+unit candidates preserved via 5 lot columns + multi-row emit)");
  console.log("[FR_FACTS]   building_similar_unit: more candidates (same-building apartments from lot2..5, Surface Carrez fallback)");
  console.log("");
  console.log("[FR_FACTS] To load into BigQuery property_latest_facts:");
  console.log("  bq load --source_format=NEWLINE_DELIMITED_JSON \\");
  console.log("    streetiq-bigquery:streetiq_gold.property_latest_facts \\");
  console.log("    " + outPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
