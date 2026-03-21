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

function dvfToGoldRow(cols: string[]): GoldRow | null {
  if (cols.length < 39) return null;

  const nature = (cols[9] ?? "").trim();
  if (nature !== "Vente") return null;

  const price = parseFrenchNumber(cols[10] ?? "");
  if (!Number.isFinite(price) || price <= 0) return null;

  const commune = (cols[17] ?? "").trim();
  const voie = (cols[15] ?? "").trim();
  const noVoie = (cols[11] ?? "").trim();
  const codePostal = (cols[16] ?? "").trim();
  const lotNumberRaw = (cols[24] ?? "").trim();
  const unitNumber = lotNumberRaw || null;

  if (!commune && !voie) return null;
  const streetRaw = voie || commune;
  const houseNumber = noVoie || "0";
  if (!streetRaw.trim()) return null;

  const dateStr = parseFrenchDate(cols[8] ?? "");
  const surfaceStr = (cols[38] ?? "").trim();
  const surface = surfaceStr ? parseFrenchNumber(surfaceStr) : NaN;
  const surfaceM2 = Number.isFinite(surface) && surface > 0 ? surface : null;
  const typeLocalRaw = (cols[36] ?? "").trim() || "Local";
  const propertyType = mapTypeLocal(typeLocalRaw);

  const lastSalePriceThousandths = Math.round(price * 1000);
  const pricePerM2 =
    surfaceM2 != null && surfaceM2 > 0 ? Math.round((price / surfaceM2) * 1000) : null;

  return {
    country: "FR",
    city: commune,
    postcode: codePostal,
    street: voie || commune,
    house_number: normalizeHouseNumber(houseNumber),
    unit_number: unitNumber,
    property_type: propertyType,
    surface_m2: surfaceM2,
    last_sale_date: dateStr,
    last_sale_price: lastSalePriceThousandths,
    price_per_m2: pricePerM2,
    data_source: "dvf",
  };
}

function buildDedupKey(r: GoldRow): string {
  const streetNorm = normalizeStreet(r.street);
  const unitPart = r.unit_number ?? "";
  return `${r.country}\0${r.city}\0${r.postcode}\0${streetNorm}\0${r.house_number}\0${unitPart}`;
}

async function processFile(filePath: string): Promise<{ rawCount: number; dedupedRows: GoldRow[] }> {
  const keyToBest = new Map<string, GoldRow>();

  const stream = fs.createReadStream(filePath, { encoding: "utf-8" });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let lineNum = 0;
  let rawCount = 0;

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    lineNum++;
    if (lineNum === 1) continue;

    const cols = parsePipeLine(line);
    const gold = dvfToGoldRow(cols);
    if (!gold) continue;

    rawCount++;
    const key = buildDedupKey(gold);
    const existing = keyToBest.get(key);
    const goldDate = gold.last_sale_date ? new Date(gold.last_sale_date).getTime() : 0;
    const existingDate = existing?.last_sale_date ? new Date(existing.last_sale_date).getTime() : 0;

    if (!existing || goldDate >= existingDate) {
      keyToBest.set(key, gold);
    }
  }

  return { rawCount, dedupedRows: Array.from(keyToBest.values()) };
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
  const keyToBest = new Map<string, GoldRow>();

  for (const f of files) {
    console.log("[FR_FACTS] Processing", path.basename(f), "...");
    const { rawCount, dedupedRows } = await processFile(f);
    totalBefore += rawCount;
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
  console.log("[FR_FACTS] total_france_fact_rows_after=" + String(totalAfter));
  console.log("[FR_FACTS] distinct_city_count=" + String(cities.size));
  console.log("[FR_FACTS] distinct_street_house_number_count=" + String(streetHouse.size));
  console.log("[FR_FACTS] distinct_apartment_address_candidate_count=" + String(addressCandidates.size));
  console.log("[FR_FACTS] ================================");

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
  console.log("[FR_FACTS] To load into BigQuery property_latest_facts:");
  console.log("  bq load --source_format=NEWLINE_DELIMITED_JSON \\");
  console.log("    streetiq-bigquery:streetiq_gold.property_latest_facts \\");
  console.log("    " + outPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
