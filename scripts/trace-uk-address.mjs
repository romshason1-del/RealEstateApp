#!/usr/bin/env node
/**
 * Trace one UK address end-to-end to find exact match failure.
 * Run: node scripts/trace-uk-address.mjs
 * Requires: npm run dev (server on localhost:3000)
 */
const RAW = "Flat 12, 1 Peninsula Square, London SE10";
const SELECTED = "1 Peninsula Square, London SE10 0ET, UK";
const BASE = "http://localhost:3000";

const params = new URLSearchParams({
  rawInputAddress: RAW,
  selectedFormattedAddress: SELECTED,
  countryCode: "UK",
});

async function trace() {
  const url = `${BASE}/api/property-value?${params.toString()}`;
  console.log("Fetching:", url);
  const res = await fetch(url);
  const data = await res.json();

  console.log("\n=== TRACE OUTPUT ===\n");
  const t = data.debug?.match_trace;
  if (!t) {
    console.log("No match_trace in debug. Response:", JSON.stringify(data, null, 2).slice(0, 2000));
    return;
  }

  console.log("1. rawInputAddress:", t.raw_input_address);
  console.log("2. selectedFormattedAddress:", t.selected_formatted_address);
  console.log("3. parsed houseNumber:", t.parsed_house_number);
  console.log("4. parsed street:", t.parsed_street);
  console.log("5. parsed postcode:", t.parsed_postcode);
  console.log("6. Land Registry rows returned:", t.land_registry_rows_returned);
  console.log("7. rows with matching postcode:", t.rows_with_matching_postcode);
  console.log("8. rows with matching street:", t.rows_with_matching_street);
  console.log("9. rows with matching PAON:", t.rows_with_matching_paon);
  console.log("10. rows with matching SAON:", t.rows_with_matching_saon);
  console.log("\n11. Fallback reason:");
  if (t.fallback_reason) {
    console.log("   - property failed because:", t.fallback_reason.property_failed_because);
    console.log("   - building failed because:", t.fallback_reason.building_failed_because);
    console.log("   - street failed because:", t.fallback_reason.street_failed_because);
    console.log("   - area used because:", t.fallback_reason.area_used_because);
  }
  console.log("\nSample rows (first 8):");
  (t.sample_rows || []).forEach((r, i) => {
    console.log(`  [${i}] PAON=${r.paon} SAON=${r.saon} street=${r.street} | pcMatch=${r.postcodeMatch} stMatch=${r.streetMatch} paonMatch=${r.paonMatch} saonMatch=${r.saonMatch} exact=${r.exactMatch} fuzzy=${r.fuzzyMatch} buildingOnly=${r.buildingOnlyMatch}`);
  });
  console.log("\nvalue_level:", data.property_result?.value_level);
  console.log("has_building_match:", data.uk_land_registry?.has_building_match);
}

trace().catch((e) => {
  console.error(e);
  process.exit(1);
});
