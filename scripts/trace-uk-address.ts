/**
 * Trace one UK address end-to-end.
 * Mode 1: npx tsx scripts/trace-uk-address.ts (provider only)
 * Mode 2: Start dev server, then npx tsx scripts/trace-uk-address.ts --api (calls API)
 */
import { UKLandRegistryProvider } from "../src/lib/property-value-providers/uk-land-registry-provider";
import { extractFlatPrefix } from "../src/lib/address-parse";
import { parseUKAddressFromFullString } from "../src/lib/address-parse";

const RAW = "Flat 3, 37 Bedford Gardens, London W8 7EF, UK";
const SELECTED = "37 Bedford Gardens, London W8 7EF, UK";
const USE_API = process.argv.includes("--api");
const BASE = "http://localhost:3000";

function main() {
  const flatFromRaw = extractFlatPrefix(RAW);
  const parsedSelected = parseUKAddressFromFullString(SELECTED);
  const city = parsedSelected.city || "";
  const postcode = parsedSelected.postcode || "";
  const houseNumber = flatFromRaw || parsedSelected.houseNumber || "";
  const selTrimmed = SELECTED.replace(/,?\s*(UK|United Kingdom)\s*$/i, "").trim();
  const postcodeRe = /\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b/i;
  const pcMatch = selTrimmed.match(postcodeRe);
  const beforePc = pcMatch ? selTrimmed.slice(0, pcMatch.index).trim() : selTrimmed;
  const selParts = beforePc.split(",").map((p) => p.trim()).filter(Boolean);
  const street =
    parsedSelected.houseNumber && parsedSelected.street?.trim()
      ? parsedSelected.street.trim()
      : selParts.length >= 2
        ? selParts.slice(0, -1).join(", ")
        : parsedSelected.street || "";

  if (USE_API) {
    const params = new URLSearchParams({
      address: SELECTED,
      countryCode: "UK",
      rawInputAddress: RAW,
      selectedFormattedAddress: SELECTED,
    });
    fetch(`${BASE}/api/property-value?${params}`)
      .then((r) => r.json())
      .then((data) => {
        const uk = data.uk_land_registry ?? {};
        const pr = data.property_result ?? {};
        console.log("\n=== API RESPONSE (client-facing) ===");
        console.log("1. has_exact_flat_match:", uk.has_exact_flat_match);
        console.log("2. has_building_match:", uk.has_building_match);
        console.log("3. street_avg:", uk.street_average_price ?? null);
        console.log("4. latest_transaction:", uk.latest_building_transaction ?? uk.latest_nearby_transaction ?? null);
        console.log("5. final value_level:", pr.value_level);
        console.log("6. from cache:", data.data_source === "cache");
      })
      .catch((e) => console.error("API fetch failed (is dev server running?):", e.message));
    return;
  }

  const provider = new UKLandRegistryProvider();
  provider
    .getInsights({
      city,
      street,
      houseNumber,
      postcode,
      rawInputAddress: RAW,
      selectedFormattedAddress: SELECTED,
    })
    .then((result) => {
      if ("debug" in result && result.debug) {
        const t = (result.debug as { match_trace?: Record<string, unknown> }).match_trace;
        if (t) {
          const tr = t as Record<string, unknown>;
          console.log("\n=== TRACE OUTPUT ===\n");
          console.log("1. rawInputAddress:", tr.raw_input_address);
          console.log("2. selectedFormattedAddress:", tr.selected_formatted_address);
          console.log("3. parsed houseNumber:", tr.parsed_house_number);
          console.log("4. parsed street:", tr.parsed_street);
          console.log("5. parsed postcode:", tr.parsed_postcode);
          console.log("6. Land Registry rows returned:", tr.land_registry_rows_returned);
          console.log("7. rows with matching postcode:", tr.rows_with_matching_postcode);
          console.log("8. rows with matching street:", tr.rows_with_matching_street);
          console.log("9. rows with matching PAON:", tr.rows_with_matching_paon);
          console.log("10. rows with matching SAON:", tr.rows_with_matching_saon);
          console.log("\n11. Fallback reason:");
          const fr = tr.fallback_reason as Record<string, string> | undefined;
          if (fr) {
            console.log("   - property failed because:", fr.property_failed_because);
            console.log("   - building failed because:", fr.building_failed_because);
            console.log("   - street failed because:", fr.street_failed_because);
            console.log("   - area used because:", fr.area_used_because);
          }
          console.log("\nSample rows:");
          ((tr.sample_rows as unknown[]) || []).forEach((r, i) => {
            const row = r as Record<string, unknown>;
            console.log(
              `  [${i}] PAON=${row.paon} SAON=${row.saon} street=${row.street} | pc=${row.postcodeMatch} st=${row.streetMatch} paon=${row.paonMatch} saon=${row.saonMatch} exact=${row.exactMatch} fuzzy=${row.fuzzyMatch} buildingOnly=${row.buildingOnlyMatch}`
            );
          });
        } else {
          console.log("No match_trace. Full debug:", JSON.stringify(result.debug, null, 2));
        }
        if ("uk_land_registry" in result && result.uk_land_registry) {
          const uk = result.uk_land_registry as {
            has_building_match?: boolean;
            has_exact_flat_match?: boolean;
            latest_building_transaction?: { price: number; date: string } | null;
            latest_nearby_transaction?: { price: number; date: string } | null;
            street_average_price?: number | null;
          };
          const hasExactFlatMatch = uk.has_exact_flat_match === true;
          const hasBuildingMatch = uk.has_building_match === true;
          const latestTx = uk.latest_building_transaction ?? uk.latest_nearby_transaction ?? null;
          const streetAvg = uk.street_average_price ?? null;
          const valueLevel = (hasExactFlatMatch && latestTx != null && latestTx.price > 0
            ? "property-level"
            : hasBuildingMatch
              ? "building-level"
              : streetAvg != null && streetAvg > 0
                ? "street-level"
                : "area-level") as string;
          console.log("\n=== ROUTE-LEVEL (value_level computation) ===");
          console.log("1. has_exact_flat_match:", hasExactFlatMatch);
          console.log("2. has_building_match:", hasBuildingMatch);
          console.log("3. latest_transaction:", latestTx ? { price: latestTx.price, date: latestTx.date } : null);
          console.log("4. final value_level:", valueLevel);
        }
        if ("message" in result) {
          console.log("\nmessage:", (result as { message?: string }).message);
        }
      } else {
        console.log("Result:", JSON.stringify(result, null, 2).slice(0, 2000));
      }
    })
    .catch((e) => {
      console.error(e);
      process.exit(1);
    });
}

main();
