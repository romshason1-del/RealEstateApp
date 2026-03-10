#!/usr/bin/env tsx
/**
 * Sync US market data from Zillow Research and Redfin Data Center into cache.
 * Run: npx tsx scripts/sync-us-market-data.ts
 * Cache is saved to .us-market-cache.json (add to .gitignore).
 */
import { syncZillowZipData, syncZillowCityData } from "../src/lib/property-value-providers/us-zillow-research-provider";
import { syncRedfinZipData, syncRedfinCityData } from "../src/lib/property-value-providers/us-redfin-provider";
import { saveToFile } from "../src/lib/property-value-providers/us-market-data-cache";

async function main() {
  console.log("Syncing US market data...\n");

  console.log("Zillow ZIP...");
  const zZip = await syncZillowZipData();
  console.log(zZip.error ? `  Error: ${zZip.error}` : `  Cached ${zZip.count} ZIP codes`);

  console.log("Zillow City...");
  const zCity = await syncZillowCityData();
  console.log(zCity.error ? `  Error: ${zCity.error}` : `  Cached ${zCity.count} cities`);

  console.log("Redfin ZIP...");
  const rZip = await syncRedfinZipData();
  console.log(rZip.error ? `  Error: ${rZip.error}` : `  Cached ${rZip.count} ZIP codes`);

  console.log("Redfin City...");
  const rCity = await syncRedfinCityData();
  console.log(rCity.error ? `  Error: ${rCity.error}` : `  Cached ${rCity.count} cities`);

  const saved = await saveToFile();
  console.log(saved ? "\nCache saved to .us-market-cache.json" : "\nCould not save cache file");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
