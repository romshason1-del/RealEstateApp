/**
 * Build France multi-unit disclosure helper table in BigQuery.
 *
 * Usage:
 *   npx tsx scripts/build-france-multi-unit-transactions.ts
 *
 * Requires: GOOGLE_APPLICATION_CREDENTIALS or GOOGLE_APPLICATION_CREDENTIALS_JSON
 *           with access to streetiq-bigquery.streetiq_gold
 *
 * Prerequisite: france_dvf_rich_source loaded with document_id + mutation_group_key
 * (see scripts/build-france-rich-source.ts).
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

const sqlPath = path.join(process.cwd(), "scripts", "create-france-multi-unit-transactions.sql");
const CREATE_TABLE_SQL = fs.existsSync(sqlPath)
  ? fs
      .readFileSync(sqlPath, "utf8")
      .split("\n")
      .filter((line) => !line.trim().startsWith("--"))
      .join("\n")
      .trim()
  : (() => {
      throw new Error("create-france-multi-unit-transactions.sql not found");
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
  const tableRef = "streetiq-bigquery.streetiq_gold.france_multi_unit_transactions";

  console.log("[FR_MULTI_UNIT] Building table from france_dvf_rich_source...");
  console.log("[FR_MULTI_UNIT] Target:", tableRef);

  const [job] = await bq.createQueryJob({
    query: CREATE_TABLE_SQL,
    location: "EU",
  });

  console.log("[FR_MULTI_UNIT] Job ID:", job.id);
  await job.getQueryResults();
  console.log("[FR_MULTI_UNIT] Table created.\n");

  const countQuery = `SELECT COUNT(*) AS n, COUNTIF(multi_unit_transaction) AS n_multi FROM \`${tableRef}\``;
  try {
    const [rows] = await bq.query({ query: countQuery, location: "EU" });
    console.log("[FR_MULTI_UNIT] Row counts:", rows?.[0]);
  } catch (e) {
    console.warn("[FR_MULTI_UNIT] Post-check failed:", e);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
