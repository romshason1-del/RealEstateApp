/**
 * Build France rich source from DVF (same source as property_latest_facts).
 * Outputs multiple rows per address WITHOUT aggressive deduplication.
 *
 * Usage:
 *   npx tsx scripts/build-france-rich-source.ts [dvf-file-or-dir]
 *   npx tsx scripts/build-france-rich-source.ts ValeursFoncieres-2024.txt
 *   npx tsx scripts/build-france-rich-source.ts .  # all ValeursFoncieres-*.txt in cwd
 *
 * Output:
 *   output/france_dvf_rich_source.ndjson
 *
 * Schema:
 *   country, city, postcode, street, house_number, unit_number,
 *   property_type, surface_m2, last_sale_price, last_sale_date, price_per_m2
 *
 * Money convention: last_sale_price and price_per_m2 in thousandths of euro (N ⇒ N/1000 €).
 * Preserves all rows; does not remove rows with missing surface_m2; does not deduplicate.
 */

import * as fs from "fs";
import * as path from "path";
import * as readline from "readline";

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

type RichSourceRow = {
  country: string;
  city: string;
  postcode: string;
  street: string;
  house_number: string;
  unit_number: string | null;
  property_type: string;
  surface_m2: number | null;
  last_sale_price: number;
  last_sale_date: string | null;
  price_per_m2: number | null;
};

/**
 * DVF lot columns (0-indexed): 1er lot, 2ème lot, 3ème lot, 4ème lot, 5ème lot
 */
const DVF_LOT_COLS = [24, 26, 28, 30, 32];

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
 * Transform one DVF row into one or more RichSourceRows.
 * One row per non-empty lot; one row with unit_number=null when no lots.
 * Does NOT filter by surface_m2 - keeps rows with null surface.
 */
function dvfToRichRows(cols: string[]): RichSourceRow[] {
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
  const base: Omit<RichSourceRow, "unit_number"> = {
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
  };

  if (lots.length === 0) {
    return [{ ...base, unit_number: null }];
  }

  return lots.map((unitNumber) => ({ ...base, unit_number: unitNumber }));
}

type ProcessStats = {
  totalRows: number;
  withSurface: number;
  addressKeys: Set<string>;
};

async function processFile(
  filePath: string,
  outStream: fs.WriteStream,
  stats: ProcessStats
): Promise<void> {
  const stream = fs.createReadStream(filePath, { encoding: "utf-8" });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let lineNum = 0;

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    lineNum++;
    if (lineNum === 1) continue;

    const cols = parsePipeLine(line);
    const richRows = dvfToRichRows(cols);
    for (const r of richRows) {
      outStream.write(JSON.stringify(r) + "\n");
      stats.totalRows++;
      if (r.surface_m2 != null && r.surface_m2 > 0) stats.withSurface++;
      stats.addressKeys.add(`${r.city}|${r.postcode}|${r.street}|${r.house_number}|${r.unit_number ?? ""}`);
    }
  }
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
    console.error("Usage: npx tsx scripts/build-france-rich-source.ts [file-or-dir]");
    console.error("  Default: all ValeursFoncieres-*.txt in project root");
    process.exit(1);
  }

  if (files.length === 0) {
    console.error("No ValeursFoncieres-*.txt files found. Run: npm run download:dvf");
    process.exit(1);
  }

  console.log("[FR_RICH] Input files:", files.length);
  for (const f of files) console.log("  -", path.basename(f));

  const outDir = path.join(root, "output");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, "france_dvf_rich_source.ndjson");
  const out = fs.createWriteStream(outPath, { encoding: "utf-8" });

  const stats: ProcessStats = { totalRows: 0, withSurface: 0, addressKeys: new Set() };

  for (const f of files) {
    console.log("[FR_RICH] Processing", path.basename(f), "...");
    await processFile(f, out, stats);
  }

  out.end();
  await new Promise<void>((res, rej) => {
    out.on("finish", res);
    out.on("error", rej);
  });

  const withoutSurface = stats.totalRows - stats.withSurface;

  console.log("");
  console.log("[FR_RICH] ========== SUMMARY ==========");
  console.log("[FR_RICH] total_rows=" + String(stats.totalRows));
  console.log("[FR_RICH] rows_with_surface_m2=" + String(stats.withSurface));
  console.log("[FR_RICH] rows_without_surface_m2=" + String(withoutSurface));
  console.log("[FR_RICH] distinct_address_unit_count=" + String(stats.addressKeys.size));
  console.log("[FR_RICH] ================================");
  console.log("[FR_RICH] Wrote", outPath);
  console.log("");
  console.log("[FR_RICH] To load into BigQuery (e.g. france_dvf_rich_source in streetiq_gold):");
  console.log("  bq load --source_format=NEWLINE_DELIMITED_JSON \\");
  console.log("    --replace \\");
  console.log("    streetiq-bigquery:streetiq_gold.france_dvf_rich_source \\");
  console.log("    " + outPath);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
