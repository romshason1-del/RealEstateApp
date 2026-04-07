/**
 * One-off NYC v4 address matching verification (US BigQuery).
 * Usage: npx tsx scripts/us/nyc/verify-nyc-app-output-matching.ts
 */

import { getUSBigQueryClient } from "../../../src/lib/us/bigquery-client";
import { isUSBigQueryConfigured } from "../../../src/lib/us/us-bigquery";
import {
  buildNycAppOutputLookupPipelineInput,
  queryNycAppOutputFinalV4Row,
} from "../../../src/lib/us/us-nyc-app-output-query";
import { buildNycTruthLookupNormalizationDebug } from "../../../src/lib/us/us-nyc-address-normalize";

const ADDRESSES = [
  "36 Riverside Dr, New York, NY 10024",
  "721 5th Ave, New York, NY 10022",
  "40 W 86th St, New York, NY 10024",
  "285 Fulton St, New York, NY 10007",
  "350 5th Ave, New York, NY 10018",
];

async function main() {
  if (!isUSBigQueryConfigured()) {
    console.error("BigQuery not configured (BIGQUERY_PROJECT_ID / credentials).");
    process.exit(1);
  }
  const client = getUSBigQueryClient();

  console.log("| typed input | sent (normalized pipeline input) | row_found | match_column | matched_candidate | lookup in row | property in row |");
  console.log("|---|---|---|---|---|---|---|");

  for (const typed of ADDRESSES) {
    const lineIn = buildNycAppOutputLookupPipelineInput(typed.trim());
    const norm = buildNycTruthLookupNormalizationDebug(lineIn);
    const lineInDisplay = norm?.normalized_full_address ?? lineIn;
    const { row, debug } = await queryNycAppOutputFinalV4Row(client, typed.trim(), null);
    const la = row && typeof row === "object" ? String((row as { lookup_address?: unknown }).lookup_address ?? "") : "";
    const pa = row && typeof row === "object" ? String((row as { property_address?: unknown }).property_address ?? "") : "";
    console.log(
      `| ${typed.replace(/\|/g, "\\|")} | ${lineInDisplay.replace(/\|/g, "\\|")} | ${debug.row_found} | ${debug.match_column ?? "none"} | ${(debug.matched_candidate ?? "").replace(/\|/g, "\\|")} | ${la.replace(/\|/g, "\\|")} | ${pa.replace(/\|/g, "\\|")} |`
    );
  }

  console.log("\n--- candidates (first address sample) ---");
  const d0 = buildNycTruthLookupNormalizationDebug(buildNycAppOutputLookupPipelineInput(ADDRESSES[0]!.trim()));
  console.log("count:", d0?.candidates?.length);
  console.log("first 12:", d0?.candidates?.slice(0, 12));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
