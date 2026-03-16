/**
 * Stream DVF (Demandes de Valeurs Foncières) CSV into Supabase properties_france.
 * Handles large files without loading everything into memory.
 *
 * DATA SOURCE: ValeursFoncieres-*.txt (pipe-delimited, DGFiP format)
 * Download from: https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/
 *
 * Run: npx tsx scripts/import-france-dvf-stream.ts [path-to-file]
 * Example: npx tsx scripts/import-france-dvf-stream.ts ValeursFoncieres-2024.txt
 *
 * DVF columns (0-indexed, pipe-delimited):
 * - 8: Date mutation (DD/MM/YYYY)
 * - 9: Nature mutation (Vente only)
 * - 10: Valeur fonciere (price)
 * - 11: No voie (house number)
 * - 15: Voie (street name)
 * - 16: Code postal
 * - 17: Commune
 * - 24: 1er lot (apartment/lot number)
 * - 36: Type local
 * - 38: Surface reelle bati
 * - 39: Nombre pieces principales
 */

import { createClient } from "@supabase/supabase-js";
import * as fs from "fs";
import * as path from "path";
import * as readline from "readline";

const BATCH_LINES = 80_000; // Process this many lines before flushing batch (memory bound)
const BATCH_SIZE = 100; // Rows per Supabase upsert call (smaller = fewer timeouts)
const RETRY_DELAY_MS = 3000;
const HIGH_WATER_MARK = 128 * 1024; // 128KB read buffer for streaming

function isRetryableError(err: unknown): boolean {
  const msg = String(err instanceof Error ? err.message : err);
  const code = err && typeof err === "object" && "code" in err ? String((err as { code?: string }).code) : "";
  return (
    code === "57014" ||
    /query_canceled|timeout|ETIMEDOUT|ECONNRESET|connection|57014/i.test(msg)
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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

type RowData = {
  current_value: number;
  last_sale_info: string;
  commune: string;
  voie: string;
  code_postal: string;
  date_mutation: string | null;
  surface_reelle_bati: number | null;
  type_local: string | null;
  nombre_pieces_principales: number | null;
  numero_voie: string;
};

type DbRow = {
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
  numero_voie: string | null;
};

function processLine(cols: string[]): { compositeKey: string; data: RowData } | null {
  if (cols.length < 25) return null;

  const natureMutation = cols[9] ?? "";
  if (natureMutation !== "Vente") return null;

  const valeurStr = cols[10] ?? "";
  const price = parseFrenchNumber(valeurStr);
  if (!Number.isFinite(price) || price <= 0) return null;

  const commune = (cols[17] ?? "").trim();
  const voie = (cols[15] ?? "").trim();
  const noVoie = (cols[11] ?? "").trim();
  const codePostal = (cols[16] ?? "").trim();
  const lotNumber = (cols[24] ?? "").trim() || "";

  if (!commune && !voie) return null;

  const addr = buildAddress(commune, voie, noVoie, codePostal);
  if (!addr || addr === "unknown") return null;

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

  const data: RowData = {
    current_value: price,
    last_sale_info: saleInfo,
    commune,
    voie,
    code_postal: codePostal,
    date_mutation: dateStr,
    surface_reelle_bati,
    type_local,
    nombre_pieces_principales,
    numero_voie: noVoie || "",
  };

  return { compositeKey, data };
}

function dedupeBatch(
  batch: Map<string, RowData>
): DbRow[] {
  const rows: DbRow[] = [];
  for (const [compositeKey, data] of batch) {
    const [addr, lotNumber] = compositeKey.split("\0");
    rows.push({
      address: addr,
      lot_number: lotNumber ?? "",
      current_value: data.current_value,
      last_sale_info: data.last_sale_info,
      street_avg_price: null, // Backfill separately if needed
      neighborhood_quality: null,
      code_postal: data.code_postal || null,
      commune: data.commune || null,
      voie: data.voie || null,
      type_local: data.type_local ?? null,
      surface_reelle_bati: data.surface_reelle_bati ?? null,
      date_mutation: data.date_mutation ?? null,
      numero_voie: data.numero_voie || null,
    });
  }
  return rows;
}

async function upsertChunkWithRetry(
  supabase: ReturnType<typeof createClient>,
  chunk: DbRow[]
): Promise<void> {
  while (true) {
    try {
      const { error } = await supabase.from("properties_france").upsert(chunk, {
        onConflict: "address,lot_number",
      });
      if (error) {
        if (isRetryableError(error)) {
          console.log("Retrying...");
          await sleep(RETRY_DELAY_MS);
          continue;
        }
        throw error;
      }
      return;
    } catch (err) {
      if (isRetryableError(err)) {
        console.log("Retrying...");
        await sleep(RETRY_DELAY_MS);
        continue;
      }
      throw err;
    }
  }
}

async function upsertBatch(
  supabase: ReturnType<typeof createClient>,
  rows: DbRow[]
): Promise<number> {
  let inserted = 0;
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const chunk = rows.slice(i, i + BATCH_SIZE);
    await upsertChunkWithRetry(supabase, chunk);
    inserted += chunk.length;
  }
  return inserted;
}

async function processFile(
  supabase: ReturnType<typeof createClient>,
  filePath: string
): Promise<{ processed: number; skipped: number; upserted: number }> {
  const stats = fs.statSync(filePath);
  console.log(`\n--- ${path.basename(filePath)} (${(stats.size / 1024 / 1024).toFixed(1)} MB) ---`);

  const fileStream = fs.createReadStream(filePath, {
    encoding: "utf-8",
    highWaterMark: HIGH_WATER_MARK,
  });

  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  let lineNum = 0;
  let processed = 0;
  let skipped = 0;
  let totalUpserted = 0;
  let linesSinceFlush = 0;
  const batch = new Map<string, RowData>();

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    lineNum++;

    if (lineNum === 1) {
      const header = parsePipeLine(trimmed);
      console.log(`Columns: ${header.length} | Header: ${header.slice(0, 8).join(" | ")}...`);
      continue;
    }

    const cols = parsePipeLine(trimmed);
    const result = processLine(cols);
    if (!result) {
      skipped++;
      continue;
    }

    const { compositeKey, data } = result;
    const existing = batch.get(compositeKey);
    if (!existing) {
      batch.set(compositeKey, { ...data });
    } else {
      const existingDate = existing.date_mutation ?? "";
      if (data.date_mutation && (!existingDate || data.date_mutation > existingDate)) {
        batch.set(compositeKey, { ...data });
      }
    }
    processed++;
    linesSinceFlush++;

    if (linesSinceFlush >= BATCH_LINES) {
      const rows = dedupeBatch(batch);
      const upserted = await upsertBatch(supabase, rows);
      totalUpserted += upserted;
      console.log(`  Line ${lineNum} | Flushed ${batch.size} unique | Total: ${totalUpserted}`);
      batch.clear();
      linesSinceFlush = 0;
    }
  }

  if (batch.size > 0) {
    const rows = dedupeBatch(batch);
    const upserted = await upsertBatch(supabase, rows);
    totalUpserted += upserted;
    console.log(`  Final flush: ${batch.size} unique rows`);
  }

  return { processed, skipped, upserted: totalUpserted };
}


async function main() {
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

  const supabase = createClient(url, key);

  const args = process.argv.slice(2);
  const defaultPath = path.join(process.cwd(), "ValeursFoncieres-2024.txt");
  const filePaths = args.length > 0 ? args : [defaultPath];

  const missing = filePaths.filter((p) => !fs.existsSync(p));
  if (missing.length > 0) {
    console.error(`
Usage: npx tsx scripts/import-france-dvf-stream.ts [file1.txt] [file2.txt] ...

Default: ValeursFoncieres-2024.txt in project root

Download DVF files from:
  https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/

Examples:
  npx tsx scripts/import-france-dvf-stream.ts
  npx tsx scripts/import-france-dvf-stream.ts ValeursFoncieres-2024.txt
  npx tsx scripts/import-france-dvf-stream.ts dvf-06.txt dvf-13.txt dvf-83.txt

Missing files: ${missing.join(", ")}
`);
    process.exit(1);
  }

  console.log("Streaming DVF import (low memory, handles large files)\n");

  let totalProcessed = 0;
  let totalSkipped = 0;
  let totalUpserted = 0;

  for (const filePath of filePaths) {
    const result = await processFile(supabase, filePath);
    totalProcessed += result.processed;
    totalSkipped += result.skipped;
    totalUpserted += result.upserted;
  }

  console.log(`\n=== Done ===`);
  console.log(`Processed: ${totalProcessed} Vente rows | Skipped: ${totalSkipped} | Upserted: ${totalUpserted} unique (address, lot_number)`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
