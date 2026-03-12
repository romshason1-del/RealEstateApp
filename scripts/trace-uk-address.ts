/**
 * Trace one UK address end-to-end. Invokes provider directly (no server needed).
 * Run: npx tsx scripts/trace-uk-address.ts
 */
import { UKLandRegistryProvider } from "../src/lib/property-value-providers/uk-land-registry-provider";
import { extractFlatPrefix } from "../src/lib/address-parse";
import { parseUKAddressFromFullString } from "../src/lib/address-parse";

const RAW = "Flat 12, 1 Peninsula Square, London SE10";
const SELECTED = "1 Peninsula Square, London SE10 0ET, UK";

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
          const uk = result.uk_land_registry as { has_building_match?: boolean };
          console.log("\nhas_building_match:", uk.has_building_match);
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
