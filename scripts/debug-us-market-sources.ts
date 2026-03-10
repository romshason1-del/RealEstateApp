#!/usr/bin/env tsx
/**
 * Debug US market data sources: verify URLs, headers, formats, sample rows.
 * Run: npx tsx scripts/debug-us-market-sources.ts
 */
import * as zlib from "zlib";
import { promisify } from "util";
const gunzipAsync = promisify(zlib.gunzip);

const SOURCES = [
  {
    name: "Zillow ZIP",
    url: "https://files.zillowstatic.com/research/public_csvs/zhvi/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
    type: "csv" as const,
  },
  {
    name: "Zillow City",
    url: "https://files.zillowstatic.com/research/public_csvs/zhvi/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv",
    type: "csv" as const,
  },
  {
    name: "Redfin ZIP",
    url: "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/zip_code_market_tracker.tsv000.gz",
    type: "gz" as const,
  },
  {
    name: "Redfin City",
    url: "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz",
    type: "gz" as const,
  },
];

function parseCSVLine(line: string): string[] {
  const result: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') inQuotes = !inQuotes;
    else if (inQuotes) current += c;
    else if (c === ",") {
      result.push(current.trim());
      current = "";
    } else current += c;
  }
  result.push(current.trim());
  return result;
}

async function main() {
  console.log("=== US Market Data Source Debug ===\n");

  for (const src of SOURCES) {
    console.log(`\n--- ${src.name} ---`);
    try {
      const res = await fetch(src.url, { signal: AbortSignal.timeout(90000) });
      console.log(`  HTTP Status: ${res.status} ${res.statusText}`);
      const ct = res.headers.get("content-type") ?? "(none)";
      console.log(`  Content-Type: ${ct}`);
      const first3 = Array.from(res.headers.entries()).slice(0, 3);
      console.log(`  First 3 headers: ${JSON.stringify(first3)}`);

      let text: string;
      if (src.type === "gz") {
        const buf = await res.arrayBuffer();
        const decompressed = (await gunzipAsync(Buffer.from(buf))) as Buffer;
        text = decompressed.toString("utf-8");
        console.log(`  Decompressed size: ${text.length} chars`);
      } else {
        text = await res.text();
        console.log(`  Content size: ${text.length} chars`);
      }

      const lines = text.split(/\r?\n/).filter((l) => l.trim());
      console.log(`  Total lines: ${lines.length}`);

      if (lines.length >= 1) {
        const headerLine = lines[0]!;
        const isTSV = headerLine.includes("\t");
        const headers = isTSV ? headerLine.split("\t").map((h) => h.trim()) : parseCSVLine(headerLine);
        console.log(`  Format: ${isTSV ? "TSV" : "CSV"}`);
        console.log(`  First 5 columns: ${headers.slice(0, 5).join(" | ")}`);
        console.log(`  All column names: ${headers.join(", ")}`);

        if (lines.length >= 2) {
          const row1 = isTSV ? lines[1]!.split("\t").map((c) => c.trim()) : parseCSVLine(lines[1]!);
          console.log(`  Row 1 first 5 values: ${row1.slice(0, 5).join(" | ")}`);
        }
      }
    } catch (err) {
      console.error(`  ERROR: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  console.log("\n=== Done ===\n");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
