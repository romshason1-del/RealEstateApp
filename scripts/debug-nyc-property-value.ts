/**
 * NYC debugging: print normalization + raw US truth + adapted main-route fields.
 * Requires GOOGLE_SERVICE_ACCOUNT_KEY / BigQuery (same as app). US-only.
 *
 * Usage (from repo root):
 *   npx tsx scripts/debug-nyc-property-value.ts
 *
 * Optional: NYC_LOG_PROPERTY_VALUE_FIELDS=1 when hitting HTTP instead (see /api/us/property-value).
 */

import { parseUSAddressFromFullString } from "../src/lib/address-parse";
import { buildNycTruthLookupNormalizationDebug } from "../src/lib/us/us-nyc-address-normalize";
import { queryUSNYCApiTruthWithCandidatesDebug } from "../src/lib/us/us-nyc-api-truth";
import { adaptUsNycTruthJsonForMainPropertyValueRoute } from "../src/lib/us/us-nyc-main-payload";

const ADDRESSES = [
  "40 W 86th St, New York, NY 10024",
  "245 E 63rd St, New York, NY 10065",
  "234 Central Park W, New York, NY 10024",
];

async function main(): Promise<void> {
  if (process.env.SKIP_BQ === "1") {
    for (const address of ADDRESSES) {
      const norm = buildNycTruthLookupNormalizationDebug(address);
      console.log(JSON.stringify({ address_searched: address, normalization: norm }, null, 2));
      console.log("---");
    }
    return;
  }

  for (const address of ADDRESSES) {
    const norm = buildNycTruthLookupNormalizationDebug(address);
    if (!norm) {
      console.log(JSON.stringify({ address_searched: address, error: "normalization_failed" }, null, 2));
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
      address_searched: address,
      zip_from_input: norm.zip_from_input ?? null,
      candidate_count: norm.candidates.length,
      final_selected_candidate: debug.final_selected_candidate ?? null,
      precomputed_row_matched: debug.precomputed_row_matched ?? false,
      normalized_candidates: norm.candidates,
      matched_full_address: (row?.full_address as string | undefined) ?? response.nyc_card_full_address ?? null,
      building_type: row?.building_type ?? null,
      unit_count: row?.unit_count ?? null,
      nyc_pending_unit_prompt: response.nyc_pending_unit_prompt ?? null,
      should_prompt_for_unit: adapted.should_prompt_for_unit ?? null,
      unit_prompt_reason: adapted.unit_prompt_reason ?? null,
      unit_lookup_status: response.unit_lookup_status ?? null,
      nyc_final_match_level: response.nyc_final_match_level ?? null,
      nyc_final_transaction_match_level: response.nyc_final_transaction_match_level ?? null,
      estimated_value: response.estimated_value ?? null,
      latest_sale_price: response.latest_sale_price ?? null,
      latest_sale_date: response.latest_sale_date ?? null,
      property_result_value_level: pr?.value_level ?? null,
      property_result_exact_value_message: pr?.exact_value_message ?? null,
    };
    console.log(JSON.stringify(payload, null, 2));
    console.log("---");
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
