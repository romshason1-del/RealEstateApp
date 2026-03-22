/**
 * Build France Building Profile table in BigQuery.
 *
 * Aggregates from france_dvf_rich_source to create building-level intelligence:
 * - building_key = postcode | normalized_street | house_number
 * - total_transactions, distinct_unit_count, avg_price_m2, median_price_m2
 * - building_class: apartment_building (3+ units) | likely_house (<=2 tx) | unclear
 *
 * Usage:
 *   npx tsx scripts/build-france-building-profile.ts
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

const CREATE_TABLE_SQL = `
CREATE OR REPLACE TABLE \`streetiq-bigquery.streetiq_gold.france_building_profile\` AS
WITH base AS (
  SELECT
    LPAD(TRIM(CAST(postcode AS STRING)), 5, '0') AS postcode,
    TRIM(CAST(street AS STRING)) AS street_raw,
    TRIM(CAST(house_number AS STRING)) AS house_number,
    COALESCE(NULLIF(TRIM(CAST(unit_number AS STRING)), ''), '(single)') AS unit_key,
    price_per_m2
  FROM \`streetiq-bigquery.streetiq_gold.france_dvf_rich_source\`
  WHERE LOWER(TRIM(CAST(country AS STRING))) = 'fr'
    AND postcode IS NOT NULL
    AND TRIM(CAST(postcode AS STRING)) != ''
    AND street IS NOT NULL
    AND TRIM(CAST(street AS STRING)) != ''
    AND house_number IS NOT NULL
    AND TRIM(CAST(house_number AS STRING)) != ''
    AND price_per_m2 IS NOT NULL
    AND price_per_m2 > 0
),
normalized AS (
  SELECT
    postcode,
    house_number,
    unit_key,
    price_per_m2,
    TRIM(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            REGEXP_REPLACE(
                              REGEXP_REPLACE(UPPER(NORMALIZE(TRIM(street_raw), NFD)), r'\\p{M}', ''),
                              r'[^A-Z0-9 ]+', ' '
                            ),
                            r'^(RUE|AVENUE|AV|BD|BOULEVARD|CHEMIN|CHE|ROUTE|IMPASSE|IMP|ALLEE|ALL|PLACE|PL|SQUARE|SQ|SENTE|COURS|PROMENADE|PROM)\\.?\\s+', ''
                          ),
                          r'\\s+DU\\s+', ' '
                        ),
                        r'\\s+DE\\s+', ' '
                      ),
                      r'\\s+DES\\s+', ' '
                    ),
                    r'\\s+LA\\s+', ' '
                  ),
                  r'\\s+LE\\s+', ' '
                ),
                r'^DU\\s+', ''
              ),
              r'^DE\\s+', ''
            ),
            r'^DES\\s+', ''
          ),
          r'^LA\\s+', ''
        ),
        r'^LE\\s+', ''
      ),
      r'\\s+', ' '
    )
  ) AS street_norm
  FROM base
),
with_key AS (
  SELECT
    postcode,
    street_norm,
    house_number,
    CONCAT(postcode, '|', street_norm, '|', house_number) AS building_key,
    unit_key,
    price_per_m2
  FROM normalized
  WHERE street_norm != ''
)
SELECT
  building_key,
  postcode,
  street_norm AS normalized_street,
  house_number,
  COUNT(*) AS total_transactions,
  COUNT(DISTINCT unit_key) AS distinct_unit_count,
  AVG(price_per_m2) AS avg_price_m2,
  APPROX_QUANTILES(price_per_m2, 100)[OFFSET(50)] AS median_price_m2,
  CASE
    WHEN COUNT(DISTINCT unit_key) >= 3 THEN 'apartment_building'
    WHEN COUNT(*) <= 2 THEN 'likely_house'
    ELSE 'unclear'
  END AS building_class
FROM with_key
GROUP BY building_key, postcode, street_norm, house_number
`;

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
  console.log("[FR_BUILDING_PROFILE] Building table from france_dvf_rich_source...");
  console.log("[FR_BUILDING_PROFILE] Target: streetiq-bigquery.streetiq_gold.france_building_profile");

  const [job] = await bq.createQueryJob({
    query: CREATE_TABLE_SQL,
    location: "EU",
  });

  console.log("[FR_BUILDING_PROFILE] Job ID:", job.id);
  await job.getQueryResults();
  console.log("[FR_BUILDING_PROFILE] Table created.");

  const countQuery = `SELECT COUNT(*) AS n FROM \`streetiq-bigquery.streetiq_gold.france_building_profile\``;
  const [countRows] = await bq.query({ query: countQuery, location: "EU" });
  const n = (countRows as { n: number }[])?.[0]?.n ?? 0;
  console.log("[FR_BUILDING_PROFILE] Table row count:", n);
  console.log("[FR_BUILDING_PROFILE] Done.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
