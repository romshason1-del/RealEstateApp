#!/usr/bin/env node
/**
 * Trace UK HPI flow for debugging.
 * Run: node scripts/trace-uk-hpi.mjs
 * Requires: npm run dev (server on localhost:3000)
 */
const ADDRESSES = [
  "10 Downing St, London SW1A 2AB, UK",
  "221B Baker St, London NW1 6XE, UK",
];

async function trace() {
  for (const addr of ADDRESSES) {
    console.log("\n" + "=".repeat(60));
    console.log("Address:", addr);
    console.log("=".repeat(60));

    const url = `http://localhost:3000/api/property-value?address=${encodeURIComponent(addr)}&countryCode=UK`;
    const res = await fetch(url);
    const data = await res.json();

    console.log("Status:", res.status);
    console.log("message:", data.message);
    console.log("Has uk_land_registry:", !!data.uk_land_registry);

    if (data.uk_land_registry) {
      const uk = data.uk_land_registry;
      console.log("  area_data_source:", uk.area_data_source ?? "(not set)");
      console.log("  average_area_price:", uk.average_area_price);
      console.log("  area_transaction_count:", uk.area_transaction_count);
      console.log("  price_trend:", uk.price_trend ? JSON.stringify(uk.price_trend) : "(none)");
      console.log("  match_confidence:", uk.match_confidence);
    }

    console.log("address (parsed):", data.address ? JSON.stringify(data.address) : "(none)");
  }
}

trace().catch((e) => {
  console.error(e);
  process.exit(1);
});
