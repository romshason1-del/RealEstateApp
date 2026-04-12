#!/usr/bin/env node
/**
 * End-to-end NYC API check against any deployment (local or Vercel).
 *
 * Usage:
 *   set VALIDATE_NYC_BASE=https://your-app.vercel.app
 *   node scripts/us/nyc/verify-nyc-app-output-deployment.mjs
 *
 * Or:
 *   VALIDATE_NYC_BASE=http://localhost:3000 node scripts/us/nyc/verify-nyc-app-output-deployment.mjs
 *
 * Appends nyc_debug=1 so JSON includes us_nyc_app_output_debug + nyc_deployment_trace (commit SHA on Vercel).
 */

const BASE = (process.env.VALIDATE_NYC_BASE ?? "http://localhost:3000").replace(/\/$/, "");

const ADDRESSES = [
  "1 1ST PLACE, Brooklyn, NY, USA",
  "1 1 PLACE, Brooklyn, NY, USA",
  "1 78 STREET, Brooklyn, NY, USA",
  "1 11 STREET, Brooklyn, NY, USA",
];

async function main() {
  console.log("BASE_URL:", BASE);
  console.log("---\n");

  for (const typed of ADDRESSES) {
    const u = new URL("/api/us/nyc-app-output", BASE);
    u.searchParams.set("address", typed);
    u.searchParams.set("nyc_debug", "1");
    const fullUrl = u.toString();

    console.log("## Typed (simulated front: same string sent as address param)");
    console.log(typed);
    console.log("\n## Full URL");
    console.log(fullUrl);
    console.log("\n## Response JSON (full)");
    try {
      const res = await fetch(fullUrl, { headers: { Accept: "application/json" } });
      const text = await res.text();
      let data;
      try {
        data = JSON.parse(text);
      } catch {
        console.log("Non-JSON:", text.slice(0, 500));
        console.log("\n---\n");
        continue;
      }
      console.log(JSON.stringify(data, null, 2));

      const dbg = data.us_nyc_app_output_debug;
      const trace = data.nyc_deployment_trace;
      console.log("\n## Extracted debug summary");
      if (dbg) {
        console.log({
          raw_input: dbg.raw_input,
          normalized_pipeline_input: dbg.normalized_pipeline_input,
          unit_or_lot_param: dbg.unit_or_lot_param,
          norm_keys_tried: dbg.norm_keys_tried?.slice?.(0, 8),
          norm_keys_count: dbg.norm_keys_tried?.length,
          sql_attempts: dbg.sql_attempts,
          no_match_reason: dbg.no_match_reason,
          match_tier: dbg.match_tier,
          row_found: dbg.row_found,
        });
      } else {
        console.log("(no us_nyc_app_output_debug — is nyc_debug=1 in URL? or old build?)");
      }
      console.log("should_prompt_for_unit:", data.should_prompt_for_unit);
      console.log("nyc_sql_match_tier:", data.nyc_sql_match_tier);
      console.log("nyc_bq_row_matched:", data.nyc_bq_row_matched);
      if (trace) {
        console.log("nyc_deployment_trace:", trace);
      }
    } catch (e) {
      console.error("fetch failed:", e?.message ?? e);
    }
    console.log("\n---\n");
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
