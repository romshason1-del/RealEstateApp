#!/usr/bin/env tsx
/**
 * Sync US market data from Zillow Research and Redfin Data Center into cache.
 * Run: npx tsx scripts/sync-us-market-data.ts
 * Cache is saved to .us-market-cache.json (add to .gitignore).
 * For URL verification (HTTP status, content-type, headers): npx tsx scripts/debug-us-market-sources.ts
 */
import * as fs from "fs/promises";
import * as path from "path";
import { syncZillowZipData, syncZillowCityData } from "../src/lib/property-value-providers/us-zillow-research-provider";
import { syncRedfinZipData, syncRedfinCityData } from "../src/lib/property-value-providers/us-redfin-provider";
import { saveToFile, getSampleCacheKeys, getCacheSize } from "../src/lib/property-value-providers/us-market-data-cache";

const SAMPLES_FILE = ".us-market-samples.json";

function logSection(name: string) {
  console.log(`\n--- ${name} ---`);
}

async function main() {
  process.stdout.write("Syncing US market data...\n");
  process.on("uncaughtException", (err) => {
    console.error("\n[FATAL] Uncaught exception:", err);
    process.exit(1);
  });
  process.on("unhandledRejection", (reason, p) => {
    console.error("\n[FATAL] Unhandled rejection at:", p, "reason:", reason);
    process.exit(1);
  });

  const allSamples: { zillowZip?: unknown[]; zillowCity?: unknown[]; redfinZip?: unknown[]; redfinCity?: unknown[] } = {};
  const counts: Record<string, number> = {};

  try {
    logSection("Zillow ZIP");
    const zZip = await syncZillowZipData();
    counts.zillowZip = zZip.count;
    if (zZip.error) {
      console.error(`  ERROR: ${zZip.error}`);
    } else {
      console.log(`  Cached ${zZip.count} ZIP codes (rows: ${zZip.totalRows ?? "?"})`);
      if (zZip.samples?.length) {
        allSamples.zillowZip = zZip.samples;
        console.log(`  Sample: ${JSON.stringify(zZip.samples[0])}`);
      }
    }

    logSection("Zillow City");
    const zCity = await syncZillowCityData();
    counts.zillowCity = zCity.count;
    if (zCity.error) {
      console.error(`  ERROR: ${zCity.error}`);
    } else {
      console.log(`  Cached ${zCity.count} cities (total rows: ${zCity.totalRows ?? "?"})`);
      if (zCity.samples?.length) {
        allSamples.zillowCity = zCity.samples;
        console.log(`  Sample: ${JSON.stringify(zCity.samples[0])}`);
      }
    }

    logSection("Redfin ZIP");
    const rZip = await syncRedfinZipData();
    counts.redfinZip = rZip.count;
    if (rZip.error) {
      console.error(`  ERROR: ${rZip.error}`);
    } else {
      console.log(`  Cached ${rZip.count} rows (total: ${rZip.totalRows ?? "?"}, unique zips overwritten)`);
      if (rZip.samples?.length) {
        allSamples.redfinZip = rZip.samples;
        console.log(`  Sample: ${JSON.stringify(rZip.samples[0])}`);
      }
    }

    logSection("Redfin City");
    const rCity = await syncRedfinCityData();
    counts.redfinCity = rCity.count;
    if (rCity.error) {
      console.error(`  ERROR: ${rCity.error}`);
    } else {
      console.log(`  Cached ${rCity.count} rows (total: ${rCity.totalRows ?? "?"}, unique cities overwritten)`);
      if (rCity.samples?.length) {
        allSamples.redfinCity = rCity.samples;
        console.log(`  Sample: ${JSON.stringify(rCity.samples[0])}`);
      }
    }

    logSection("Save cache");
    const saved = await saveToFile();
    if (saved) {
      console.log("  Cache saved to .us-market-cache.json");
    } else {
      console.error("  Could not save cache file");
    }

    const samplesPath = path.join(process.cwd(), SAMPLES_FILE);
    await fs.writeFile(samplesPath, JSON.stringify(allSamples, null, 2), "utf-8");
    console.log(`  Samples saved to ${SAMPLES_FILE}`);

    const { zip: zipKey, city: cityKey } = getSampleCacheKeys();
    logSection("Summary");
    console.log(`  Zillow ZIP:  ${counts.zillowZip ?? 0} cached`);
    console.log(`  Zillow City: ${counts.zillowCity ?? 0} cached`);
    console.log(`  Redfin ZIP:  ${counts.redfinZip ?? 0} cached`);
    console.log(`  Redfin City: ${counts.redfinCity ?? 0} cached`);
    console.log(`  Total cache entries: ${getCacheSize()}`);
    console.log(`  Example ZIP key: zip:${zipKey ?? "(none)"}`);
    console.log(`  Example city key: city:${cityKey ?? "(none)"}`);
    console.log("\nDone.\n");
  } catch (err) {
    console.error("\n[FATAL] Sync failed:", err);
    process.exit(1);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
