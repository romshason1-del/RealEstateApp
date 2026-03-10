#!/usr/bin/env node
/**
 * Production API verification for UK address.
 * Usage: node scripts/verify-production-api.mjs [PRODUCTION_URL]
 * Example: node scripts/verify-production-api.mjs https://your-app.vercel.app
 */
const PRODUCTION_URL = process.argv[2] || process.env.VERCEL_URL || "https://realestateapp.vercel.app";
const ADDRESS = "221B Baker St, London NW1 6XE, UK";
const url = `${PRODUCTION_URL.replace(/\/$/, "")}/api/property-value?address=${encodeURIComponent(ADDRESS)}&countryCode=UK`;

console.log("Testing:", url);
console.log("");

const res = await fetch(url);
const text = await res.text();
let data;
try {
  data = JSON.parse(text);
} catch {
  console.log("Response is NOT JSON (status:", res.status, ")");
  console.log("First 500 chars:", text.slice(0, 500));
  process.exit(1);
}

console.log("HTTP Status:", res.status);
console.log("");
console.log("Response keys:", Object.keys(data).join(", "));
console.log("");
console.log("uk_land_registry:", data.uk_land_registry ? "PRESENT" : "MISSING");
if (data.uk_land_registry) {
  console.log("  has_building_match:", data.uk_land_registry.has_building_match);
  console.log("  fallback_level_used:", data.uk_land_registry.fallback_level_used);
  console.log("  average_area_price:", data.uk_land_registry.average_area_price);
  console.log("  area_transaction_count:", data.uk_land_registry.area_transaction_count);
  console.log("  latest_nearby_transaction:", data.uk_land_registry.latest_nearby_transaction ? "PRESENT" : "null");
}
console.log("");
console.log("address:", data.address ? "PRESENT" : "MISSING");
console.log("message:", data.message);
console.log("");
console.log("Full JSON:");
console.log(JSON.stringify(data, null, 2));
