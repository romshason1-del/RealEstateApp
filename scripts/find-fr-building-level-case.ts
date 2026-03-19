import { BigQuery } from "@google-cloud/bigquery";
import fs from "node:fs";
import path from "node:path";
import { getBigQueryConfig } from "@/lib/bigquery-client";

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

async function main() {
  loadDotEnvLocal();
  const { projectId, dataset, table, location } = getBigQueryConfig();
  const bq = new BigQuery({ projectId });
  const fullTable = `\`${projectId}.${dataset}.${table}\``;

  // Find buildings where:
  // - there are >= 2 primary sales (appartement/maison)
  // - at least one row has surface, at least one row has date
  // - BUT no single row has BOTH surface and date
  // This makes same-building "similar apartment" fail (needs surface+date on a picked row),
  // while buildingStrong can still be true.
  const query = `
    WITH b AS (
      SELECT
        TRIM(CAST(\`Code postal\` AS STRING)) AS code_postal,
        TRIM(CAST(\`Commune\` AS STRING)) AS commune,
        TRIM(CAST(\`No voie\` AS STRING)) AS no_voie,
        TRIM(CAST(\`Type de voie\` AS STRING)) AS type_de_voie,
        TRIM(CAST(\`Voie\` AS STRING)) AS voie,
        COUNTIF(
          LOWER(TRIM(CAST(\`Nature mutation\` AS STRING))) = 'vente'
          AND LOWER(TRIM(CAST(\`Type local\` AS STRING))) IN ('appartement','maison')
        ) AS primary_sales,
        COUNTIF(
          LOWER(TRIM(CAST(\`Nature mutation\` AS STRING))) = 'vente'
          AND LOWER(TRIM(CAST(\`Type local\` AS STRING))) IN ('appartement','maison')
          AND SAFE_CAST(REPLACE(REPLACE(TRIM(CAST(\`Surface reelle bati\` AS STRING)), ' ', ''), ',', '.') AS FLOAT64) > 0
        ) AS with_surface,
        COUNTIF(
          LOWER(TRIM(CAST(\`Nature mutation\` AS STRING))) = 'vente'
          AND LOWER(TRIM(CAST(\`Type local\` AS STRING))) IN ('appartement','maison')
          AND SAFE.PARSE_DATE('%d/%m/%Y', CAST(\`Date mutation\` AS STRING)) IS NOT NULL
        ) AS with_date,
        COUNTIF(
          LOWER(TRIM(CAST(\`Nature mutation\` AS STRING))) = 'vente'
          AND LOWER(TRIM(CAST(\`Type local\` AS STRING))) IN ('appartement','maison')
          AND SAFE_CAST(REPLACE(REPLACE(TRIM(CAST(\`Surface reelle bati\` AS STRING)), ' ', ''), ',', '.') AS FLOAT64) > 0
          AND SAFE.PARSE_DATE('%d/%m/%Y', CAST(\`Date mutation\` AS STRING)) IS NOT NULL
        ) AS with_both
      FROM ${fullTable}
      GROUP BY code_postal, commune, no_voie, type_de_voie, voie
    )
    SELECT * FROM b
    WHERE primary_sales >= 2 AND with_surface >= 1 AND with_date >= 1 AND with_both = 0
    ORDER BY primary_sales DESC
    LIMIT 25
  `;

  const [rows] = await bq.query({ query, location });
  console.log(JSON.stringify({ count: (rows as any[]).length, rows }, null, 2));
}

main().catch((e) => {
  console.error("FAILED", e);
  process.exit(1);
});

