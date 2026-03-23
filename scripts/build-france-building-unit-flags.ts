/**
 * Build France Building Unit Flags table in BigQuery.
 *
 * Precomputes which addresses/buildings have more than one distinct unit_number,
 * so we can use it at runtime without heavy queries for has_unit_level_differentiation.
 *
 * Usage:
 *   npx tsx scripts/build-france-building-unit-flags.ts
 *
 * Requires: GOOGLE_APPLICATION_CREDENTIALS or GOOGLE_APPLICATION_CREDENTIALS_JSON
 *           with access to streetiq-bigquery.streetiq_gold
 */

import * as fs from "node:fs";
import * as path from "node:path";
import { BigQuery } from "@google-cloud/bigquery";

function loadDotEnvLocal(): void {
  const p = path.resolve(process.cwd(), ".env.local");
  if (!fs.existsSync(p)) return;
  const content = fs.readFileSync(p, "utf8");
  for (const line of content.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eq = trimmed.indexOf("=");
    if (eq <= 0) continue;
    const key = trimmed.slice(0, eq).trim();
    const value = trimmed.slice(eq + 1).trim();
    if (process.env[key] === undefined) process.env[key] = value;
  }
}

// Load SQL from create-france-building-unit-flags.sql (with normalized columns)
const sqlPath = path.join(process.cwd(), "scripts", "create-france-building-unit-flags.sql");
const CREATE_TABLE_SQL = fs.existsSync(sqlPath)
  ? fs
      .readFileSync(sqlPath, "utf8")
      .split("\n")
      .filter((line) => !line.trim().startsWith("--"))
      .join("\n")
      .trim()
  : (() => {
      throw new Error("create-france-building-unit-flags.sql not found");
    })();

async function main() {
  loadDotEnvLocal();

  const projectId =
    process.env.BIGQUERY_PROJECT_ID ||
    process.env.GOOGLE_CLOUD_PROJECT_ID ||
    (process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON
      ? (JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON) as { project_id?: string }).project_id
      : "streetiq-bigquery") ||
    "streetiq-bigquery";

  const bq = new BigQuery({ projectId });
  const tableRef = "streetiq-bigquery.streetiq_gold.france_building_unit_flags";

  console.log("[FR_UNIT_FLAGS] Building table from france_dvf_rich_source...");
  console.log("[FR_UNIT_FLAGS] Target:", tableRef);

  const [job] = await bq.createQueryJob({
    query: CREATE_TABLE_SQL,
    location: "EU",
  });

  console.log("[FR_UNIT_FLAGS] Job ID:", job.id);
  await job.getQueryResults();
  console.log("[FR_UNIT_FLAGS] A. Table created.\n");

  const countQuery = `SELECT COUNT(*) AS n FROM \`${tableRef}\``;
  const sampleQuery = `
    SELECT postcode, city, street, house_number, postcode_norm, city_norm, street_norm_clean, house_number_norm, distinct_unit_count, rows_count, has_unit_level_differentiation
    FROM \`${tableRef}\`
    ORDER BY distinct_unit_count DESC
    LIMIT 10
  `;

  try {
    const [countRows] = await bq.query({ query: countQuery, location: "EU" });
    const n = (countRows as { n: number }[])?.[0]?.n ?? 0;
    console.log("[FR_UNIT_FLAGS] B. Number of rows:", n.toLocaleString(), "\n");

    const [sampleRows] = await bq.query({ query: sampleQuery, location: "EU" });
    console.log("[FR_UNIT_FLAGS] C. Sample rows (top 10 by distinct_unit_count):");
    console.log(JSON.stringify(sampleRows, null, 2));
  } catch (err) {
    console.warn("[FR_UNIT_FLAGS] Could not fetch count/sample (table was created):", (err as Error)?.message);
    console.log("[FR_UNIT_FLAGS] Run manually: bq query --use_legacy_sql=false --project_id=streetiq-bigquery \"SELECT COUNT(*) FROM streetiq_gold.france_building_unit_flags\"");
  }
  console.log("\n[FR_UNIT_FLAGS] Done.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
