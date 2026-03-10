#!/usr/bin/env node
/**
 * Verification script for UK address "221B Baker St, London NW1 6XE, UK"
 * Run: node scripts/verify-uk-address.mjs
 * Requires the dev server to be running: npm run dev
 */
const ADDRESS = "221B Baker St, London NW1 6XE, UK";
const BASE = "http://localhost:3000";

async function verify() {
  const url = `${BASE}/api/property-value?address=${encodeURIComponent(ADDRESS)}&countryCode=UK`;
  console.log("Fetching:", url);
  const res = await fetch(url);
  const data = await res.json();
  console.log("\nStatus:", res.status);
  console.log("\nResponse keys:", Object.keys(data));
  console.log("\nuk_land_registry:", data.uk_land_registry ? JSON.stringify(data.uk_land_registry, null, 2) : "MISSING");
  console.log("\naddress:", data.address);
  console.log("\nmessage:", data.message);
  console.log("\ndebug (partial):", data.debug ? { normalized_postcode: data.debug.normalized_postcode, postcode_results_count: data.debug.postcode_results_count, fallback_level_used: data.debug.fallback_level_used } : "none");
  if (data.uk_land_registry) {
    console.log("\n✓ UK section will render with has_building_match:", data.uk_land_registry.has_building_match);
  } else {
    console.log("\n✗ No uk_land_registry - will show 'No property data found'");
  }
}

verify().catch((e) => {
  console.error(e);
  process.exit(1);
});
