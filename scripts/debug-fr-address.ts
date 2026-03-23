/**
 * Debug script: call property-value API for a specific France address and print fr_runtime_debug.
 * Usage: npx tsx scripts/debug-fr-address.ts [address]
 * Default: "18 Chemin des Oliviers, 06600 Antibes"
 * Ensure dev server is running: npm run dev
 * Requires GCP credentials for BigQuery.
 */
const ADDRESS = process.argv[2] ?? "18 Chemin des Oliviers, 06600 Antibes";
const BASE = "http://localhost:3000";

async function main() {
  const params = new URLSearchParams({
    countryCode: "FR",
    address: ADDRESS,
  });
  const url = `${BASE}/api/property-value?${params.toString()}`;
  console.log("Request:", url);
  const res = await fetch(url);
  const json = await res.json();
  const debug = json?.fr?.fr_runtime_debug ?? json?.fr_runtime_debug ?? {};
  console.log("\n=== 1. Exact classification values ===");
  console.log("fr_property_type_final:", debug.fr_property_type_final ?? "(missing)");
  console.log("fr_detect_reason:", debug.fr_detect_reason ?? "(missing)");
  console.log("fr_strict_maison_count:", debug.fr_strict_maison_count ?? "(missing)");
  console.log("fr_strict_appartement_count:", debug.fr_strict_appartement_count ?? "(missing)");
  console.log("fr_strict_lot_distinct_count:", debug.fr_strict_lot_distinct_count ?? "(missing)");
  console.log("\n=== 2. Classification source ===");
  console.log("fr_classification_query_mode:", debug.fr_classification_query_mode ?? "(missing)");
  console.log("fr_detect_classification_reason:", debug.fr_detect_classification_reason ?? "(missing)");
  console.log("\n=== 3. Building intelligence (loose match source) ===");
  console.log("intelligence row is_multi_unit:", (json?.fr?.fr_runtime_debug as any)?.intelligence_is_multi_unit ?? "(check raw row)");
  console.log("\n=== 4. prompt_for_apartment / lot ===");
  console.log("fr_should_prompt_lot:", debug.fr_should_prompt_lot);
  console.log("prompt_for_apartment:", json?.prompt_for_apartment);
  if (!res.ok) {
    console.error("\nHTTP", res.status, json?.message ?? "");
  }
}

main().catch((e) => {
  console.error("FAILED:", e);
  process.exit(1);
});
