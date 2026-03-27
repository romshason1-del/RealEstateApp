/**
 * NYC live E2E probe: normalization + truth query + (optional) BigQuery "similar" rows when no exact match.
 * US-only. Requires GOOGLE_SERVICE_ACCOUNT_KEY / BIGQUERY_PROJECT_ID (same as app).
 *
 * Usage: npx tsx scripts/nyc-live-e2e-probe.ts
 */

import { parseUSAddressFromFullString } from "../src/lib/address-parse";
import { getUSBigQueryClient } from "../src/lib/us/bigquery-client";
import { US_NYC_CARD_OUTPUT_V5_REFERENCE } from "../src/lib/us/us-nyc-precomputed-card";
import { buildNycTruthLookupNormalizationDebug } from "../src/lib/us/us-nyc-address-normalize";
import { queryUSNYCApiTruthWithCandidatesDebug } from "../src/lib/us/us-nyc-api-truth";
import { adaptUsNycTruthJsonForMainPropertyValueRoute } from "../src/lib/us/us-nyc-main-payload";

const ADDRESSES = [
  "40 W 86th St, New York, NY 10024",
  "245 E 63rd St, New York, NY 10065",
  "234 Central Park W, New York, NY 10024",
];

/** Broad LIKE patterns to find rows in gold table when exact equality misses. */
const PROBE_LIKE_PATTERNS: Record<string, string[]> = {
  "40 W 86th St, New York, NY 10024": ["%40%WEST%86%", "%40%W%86%", "%86TH%"],
  "245 E 63rd St, New York, NY 10065": ["%245%EAST%63%", "%245%E%63%", "%63RD%"],
  "234 Central Park W, New York, NY 10024": ["%234%CENTRAL%PARK%", "%CENTRAL%PARK%WEST%"],
};

async function probeSimilarRows(searched: string): Promise<Record<string, unknown>[]> {
  const patterns = PROBE_LIKE_PATTERNS[searched];
  if (!patterns?.length) return [];
  const client = getUSBigQueryClient();
  const card = `\`${US_NYC_CARD_OUTPUT_V5_REFERENCE}\``;
  const ors = patterns.map((_, i) => `UPPER(full_address) LIKE @p${i}`).join(" OR ");
  const params: Record<string, string> = {};
  patterns.forEach((p, i) => {
    params[`p${i}`] = p;
  });
  const [rows] = await client.query({
    query: `
SELECT full_address, building_type, unit_count
FROM ${card}
WHERE ${ors}
LIMIT 10
`.trim(),
    params,
    location: "EU",
  });
  return ((rows as Record<string, unknown>[]) ?? []).slice(0, 10);
}

async function main(): Promise<void> {
  for (const address of ADDRESSES) {
    const norm = buildNycTruthLookupNormalizationDebug(address);
    if (!norm) {
      console.log(JSON.stringify({ searched_address: address, error: "normalization_failed" }, null, 2));
      continue;
    }

    const { response, debug } = await queryUSNYCApiTruthWithCandidatesDebug(address, norm, {});
    const row = debug.first_row_if_any as Record<string, unknown> | null | undefined;
    const ups = parseUSAddressFromFullString(address);
    const adapted = await adaptUsNycTruthJsonForMainPropertyValueRoute(
      { ...response, us_nyc_debug: debug } as Record<string, unknown>,
      { city: ups.city, street: ups.street, houseNumber: ups.houseNumber }
    );
    const pr = adapted.property_result as Record<string, unknown> | undefined;

    const payload = {
      searched_address: address,
      candidate_generator_version: norm.candidate_generator_version,
      generated_candidates: norm.candidates,
      final_selected_candidate: debug.final_selected_candidate ?? null,
      precomputed_row_matched: debug.precomputed_row_matched ?? false,
      matched_full_address: (row?.full_address as string | undefined) ?? response.nyc_card_full_address ?? null,
      building_type: row?.building_type ?? null,
      unit_count: row?.unit_count ?? null,
      nyc_pending_unit_prompt: response.nyc_pending_unit_prompt ?? null,
      should_prompt_for_unit: adapted.should_prompt_for_unit ?? null,
      unit_prompt_reason: adapted.unit_prompt_reason ?? null,
      estimated_value: response.estimated_value ?? null,
      latest_sale_price: response.latest_sale_price ?? null,
      latest_sale_date: response.latest_sale_date ?? null,
      property_result_value_level: pr?.value_level ?? null,
      property_result_exact_value_message: pr?.exact_value_message ?? null,
    };
    console.log(JSON.stringify(payload, null, 2));

    if (!debug.precomputed_row_matched) {
      try {
        const similar = await probeSimilarRows(address);
        console.log(JSON.stringify({ searched_address: address, similar_rows_sample: similar }, null, 2));
      } catch (e) {
        console.log(JSON.stringify({ searched_address: address, similar_rows_error: String(e) }, null, 2));
      }
    }
    console.log("---");
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
