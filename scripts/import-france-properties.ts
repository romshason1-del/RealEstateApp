/**
 * Import France DVF (Demandes de Valeurs Foncières) data into Supabase properties_france.
 *
 * DATA SOURCE: ValeursFoncieres-2024.txt (pipe-delimited DGFiP DVF format)
 * Place the file in the project root.
 *
 * Run: npx ts-node scripts/import-france-properties.ts [path-to-file]
 * Default path: ValeursFoncieres-2024.txt in project root
 *
 * DVF columns (pipe-delimited):
 * - 8: Date mutation (DD/MM/YYYY)
 * - 9: Nature mutation (Vente, etc.)
 * - 10: Valeur fonciere (price, comma decimal)
 * - 11: No voie (house number)
 * - 15: Voie (street name)
 * - 16: Code postal
 * - 17: Commune
 * - 24: 1er lot (apartment/lot number) -> maps to lot_number
 *
 * ROW COUNT: DVF has ~3.5M rows total. We keep only Vente (skips Echange, etc.).
 * We dedupe by (address, lot_number) keeping the most recent transaction per unit.
 * Result: ~1.2M unique addresses when merging by address only; with lot_number
 * we get more rows (one per apartment in multi-unit buildings).
 */

import { createClient } from "@supabase/supabase-js";
import * as fs from "fs";
import * as path from "path";
import * as readline from "readline";

let supabase: ReturnType<typeof createClient>;

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
  if (!m) return null;
  return `${m[3]}-${m[2]}-${m[1]}`;
}

function buildAddress(commune: string, voie: string, noVoie: string, codePostal: string): string {
  const parts: string[] = [];
  if (noVoie?.trim()) parts.push(noVoie.trim());
  if (voie?.trim()) parts.push(voie.trim());
  if (codePostal?.trim() && commune?.trim()) {
    parts.push(`${codePostal.trim()} ${commune.trim()}`);
  } else if (commune?.trim()) {
    parts.push(commune.trim());
  }
  return parts.filter(Boolean).join(", ").trim() || "unknown";
}

async function main() {
  // Load .env.local so env vars are available when run via npx tsx
  try {
    const dotenv = await import("dotenv");
    dotenv.config({ path: path.join(process.cwd(), ".env.local") });
  } catch {
    /* dotenv optional */
  }

  const url = process.env.NEXT_PUBLIC_SUPABASE_URL ?? "";
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY ?? "";
  if (!url || !key) {
    console.error("Set NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env.local");
    process.exit(1);
  }
  supabase = createClient(url, key);

  const defaultPath = path.join(process.cwd(), "ValeursFoncieres-2024.txt");
  const filePath = process.argv[2] ?? defaultPath;

  if (!fs.existsSync(filePath)) {
    console.error(`
Usage: npx ts-node scripts/import-france-properties.ts [path-to-file]

Default: ValeursFoncieres-2024.txt in project root

Place the DVF file in the project root and run:
  npx ts-node scripts/import-france-properties.ts
`);
    process.exit(1);
  }

  console.log("Reading", filePath, "(streaming)...");
  const fileStream = fs.createReadStream(filePath, { encoding: "utf-8" });
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  type RowData = { current_value: number; last_sale_info: string; prices: number[]; commune: string; voie: string; code_postal: string; date_mutation: string | null; surface_reelle_bati: number | null; type_local: string | null; nombre_pieces_principales: number | null };
  const keyToData = new Map<string, RowData>();

  let processed = 0;
  let skipped = 0;
  let lineNum = 0;
  let header: string[] = [];

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    lineNum++;
    if (lineNum === 1) {
      header = parsePipeLine(trimmed);
      console.log("Columns:", header.length, "| Sample:", header.slice(0, 12).join(" | "));
      continue;
    }

    const cols = parsePipeLine(trimmed);
    if (cols.length < 25) continue;

    const natureMutation = cols[9] ?? "";
    if (natureMutation !== "Vente") {
      skipped++;
      continue;
    }

    const valeurStr = cols[10] ?? "";
    const price = parseFrenchNumber(valeurStr);
    if (!Number.isFinite(price) || price <= 0) {
      skipped++;
      continue;
    }

    const commune = (cols[17] ?? "").trim();
    const voie = (cols[15] ?? "").trim();
    const noVoie = (cols[11] ?? "").trim();
    const codePostal = (cols[16] ?? "").trim();
    const lotNumber = (cols[24] ?? "").trim() || "";

    if (!commune && !voie) {
      skipped++;
      continue;
    }

    const addr = buildAddress(commune, voie, noVoie, codePostal);
    if (!addr || addr === "unknown") {
      skipped++;
      continue;
    }

    const compositeKey = `${addr}\0${lotNumber}`;

    const dateStr = parseFrenchDate(cols[8] ?? "");
    const saleInfo = dateStr
      ? `${Math.round(price).toLocaleString("fr-FR")} € · ${dateStr}`
      : `${Math.round(price).toLocaleString("fr-FR")} €`;
    const surfaceStr = (cols[38] ?? "").trim();
    const surface = surfaceStr ? parseFrenchNumber(surfaceStr) : NaN;
    const surface_reelle_bati = Number.isFinite(surface) && surface > 0 ? surface : null;
    const type_local = (cols[36] ?? "").trim() || null;
    const nbPiecesStr = (cols[39] ?? "").trim();
    const nbPieces = nbPiecesStr ? parseInt(nbPiecesStr, 10) : NaN;
    const nombre_pieces_principales = Number.isFinite(nbPieces) && nbPieces >= 0 ? nbPieces : null;

    const existing = keyToData.get(compositeKey);
    if (!existing) {
      keyToData.set(compositeKey, {
        current_value: price,
        last_sale_info: saleInfo,
        prices: [price],
        commune,
        voie,
        code_postal: codePostal,
        date_mutation: dateStr,
        surface_reelle_bati,
        type_local,
        nombre_pieces_principales,
      });
    } else {
      existing.prices.push(price);
      const existingDate = (existing.last_sale_info.split(" · ")[1] ?? "").trim();
      if (dateStr && (!existingDate || new Date(dateStr) > new Date(existingDate))) {
        existing.current_value = price;
        existing.last_sale_info = saleInfo;
        existing.date_mutation = dateStr;
        existing.surface_reelle_bati = surface_reelle_bati;
        existing.type_local = type_local;
        existing.nombre_pieces_principales = nombre_pieces_principales;
      }
    }
    processed++;
    if (lineNum % 500000 === 0) console.log(`  Processed ${lineNum} lines...`);
  }

  console.log(`Parsed ${processed} Vente transactions, skipped ${skipped}`);

  const streetAverages = new Map<string, number[]>();
  for (const [compositeKey, data] of keyToData) {
    const addr = compositeKey.split("\0")[0] ?? "";
    const voiePart = data.voie || (addr.split(",")[0]?.trim() ?? "");
    if (voiePart) {
      const list = streetAverages.get(voiePart) ?? [];
      list.push(...data.prices);
      streetAverages.set(voiePart, list);
    }
  }

  const rows: Array<{
    address: string;
    lot_number: string;
    current_value: number;
    last_sale_info: string;
    street_avg_price: number | null;
    neighborhood_quality: string | null;
    code_postal: string | null;
    commune: string | null;
    voie: string | null;
    type_local: string | null;
    surface_reelle_bati: number | null;
    date_mutation: string | null;
  }> = [];

  for (const [compositeKey, data] of keyToData) {
    const [addr, lotNumber] = compositeKey.split("\0");
    const voiePart = data.voie || (addr.split(",")[0]?.trim() ?? "");
    const streetPrices = streetAverages.get(voiePart) ?? data.prices;
    const streetAvg =
      streetPrices.length > 0
        ? streetPrices.reduce((a, b) => a + b, 0) / streetPrices.length
        : null;

    rows.push({
      address: addr,
      lot_number: lotNumber ?? "",
      current_value: data.current_value,
      last_sale_info: data.last_sale_info,
      street_avg_price: streetAvg,
      neighborhood_quality: null,
      code_postal: data.code_postal || null,
      commune: data.commune || null,
      voie: data.voie || null,
      type_local: data.type_local ?? null,
      surface_reelle_bati: data.surface_reelle_bati ?? null,
      date_mutation: data.date_mutation ?? null,
    });
  }

  console.log(`Upserting ${rows.length} unique (address, lot_number) rows...`);

  const BATCH = 500;
  let inserted = 0;
  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const { error } = await supabase.from("properties_france").upsert(batch, {
      onConflict: "address,lot_number",
    });
    if (error) {
      console.error("Batch error:", error);
    } else {
      inserted += batch.length;
      if (inserted % 10000 === 0 || inserted === rows.length) {
        console.log(`Upserted ${inserted}/${rows.length}`);
      }
    }
  }

  console.log(`Done. Imported ${inserted} French properties.`);
}

main().catch(console.error);
