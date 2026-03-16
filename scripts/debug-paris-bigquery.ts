/**
 * Paris-specific BigQuery diagnostic script.
 * Run: npx tsx scripts/debug-paris-bigquery.ts
 * Requires: .env.local with GOOGLE_CLOUD_PROJECT_ID and GOOGLE_APPLICATION_CREDENTIALS
 */
import * as path from "path";

if (typeof process !== "undefined") {
  (process.env as Record<string, string>)["FAST_QUERY_PATH"] = "DISABLED";
}

async function loadEnv() {
  try {
    const dotenv = await import("dotenv");
    dotenv.config({ path: path.join(process.cwd(), ".env.local") });
    dotenv.config({ path: path.join(process.cwd(), ".env") });
  } catch {}
}

async function main() {
  await loadEnv();
  const { getBigQueryClient, getBigQueryConfig } = await import("../src/lib/bigquery-client");
  const config = getBigQueryConfig();
  const { projectId, dataset, table, location } = config;
  const client = getBigQueryClient();
  const fullTable = `\`${projectId}.${dataset}.${table}\``;

  const COLS = {
    code_postal: "`Code postal`",
    commune: "`Commune`",
    no_voie: "`No voie`",
    type_de_voie: "`Type de voie`",
    voie: "`Voie`",
    lot_1er: "`1er lot`",
    valeur_fonciere: "`Valeur fonciere`",
    surface_reelle_bati: "`Surface reelle bati`",
    date_mutation: "`Date mutation`",
    nature_mutation: "`Nature mutation`",
  };

  console.log("\n=== A) COUNT by postcode ===\n");

  for (const postcode of ["75004", "75011"]) {
    const q = `SELECT COUNT(*) AS n FROM ${fullTable} WHERE TRIM(CAST(${COLS.code_postal} AS STRING)) = '${postcode}'`;
    const [rows] = await client.query({ query: q, location });
    const n = (rows as { n: number }[])?.[0]?.n ?? 0;
    console.log(`Postcode ${postcode}: n =`, n);
  }

  console.log("\n=== B) 10 sample rows per postcode ===\n");

  for (const postcode of ["75004", "75011"]) {
    const q = `
      SELECT ${COLS.code_postal}, ${COLS.commune}, ${COLS.no_voie}, ${COLS.type_de_voie}, ${COLS.voie}, ${COLS.lot_1er}, ${COLS.valeur_fonciere}, ${COLS.surface_reelle_bati}, ${COLS.date_mutation}
      FROM ${fullTable}
      WHERE TRIM(CAST(${COLS.code_postal} AS STRING)) = '${postcode}'
        AND LOWER(TRIM(CAST(${COLS.nature_mutation} AS STRING))) = 'vente'
      ORDER BY SAFE.PARSE_DATE('%d/%m/%Y', CAST(${COLS.date_mutation} AS STRING)) DESC NULLS LAST
      LIMIT 10
    `;
    const [rows] = await client.query({ query: q, location });
    const arr = (rows as Record<string, unknown>[]) || [];
    console.log(`--- Postcode ${postcode} (${arr.length} rows) ---`);
    arr.forEach((r, i) => {
      console.log(`Sample ${i + 1}:`, {
        "Code postal": r["Code postal"],
        Commune: r.Commune,
        "No voie": r["No voie"],
        "Type de voie": r["Type de voie"],
        Voie: r.Voie,
        "1er lot": r["1er lot"],
        "Valeur fonciere": r["Valeur fonciere"],
        "Surface reelle bati": r["Surface reelle bati"],
        "Date mutation": r["Date mutation"],
      });
    });
    console.log("");
  }

  console.log("\n=== C) Commune values for 75004 / 75011 ===\n");

  const communeCheck = `
    SELECT TRIM(CAST(\`Commune\` AS STRING)) AS commune, COUNT(*) AS n
    FROM ${fullTable}
    WHERE TRIM(CAST(${COLS.code_postal} AS STRING)) IN ('75004', '75011')
    GROUP BY 1
    ORDER BY 2 DESC
  `;
  const [communeRows] = await client.query({ query: communeCheck, location });
  console.log("Commune values in 75004/75011:", (communeRows as { commune: string; n: number }[]) || []);

  console.log("\n=== D) Street name samples for 75004 (Rue de Turenne) ===\n");

  const turenneCheck = `
    SELECT ${COLS.type_de_voie}, ${COLS.voie}, ${COLS.no_voie}, COUNT(*) AS n
    FROM ${fullTable}
    WHERE TRIM(CAST(${COLS.code_postal} AS STRING)) = '75004'
      AND LOWER(TRIM(CAST(${COLS.nature_mutation} AS STRING))) = 'vente'
      AND (LOWER(CAST(${COLS.voie} AS STRING)) LIKE '%turenne%' OR LOWER(CAST(${COLS.voie} AS STRING)) LIKE '%turene%')
    GROUP BY 1, 2, 3
    LIMIT 20
  `;
  const [turenneRows] = await client.query({ query: turenneCheck, location });
  console.log("Turenne-like streets in 75004:", (turenneRows as Record<string, unknown>[]) || []);

  console.log("\n=== E) Street name samples for 75011 (Oberkampf) ===\n");

  const oberkampfCheck = `
    SELECT ${COLS.type_de_voie}, ${COLS.voie}, ${COLS.no_voie}, COUNT(*) AS n
    FROM ${fullTable}
    WHERE TRIM(CAST(${COLS.code_postal} AS STRING)) = '75011'
      AND LOWER(TRIM(CAST(${COLS.nature_mutation} AS STRING))) = 'vente'
      AND LOWER(CAST(${COLS.voie} AS STRING)) LIKE '%oberkampf%'
    GROUP BY 1, 2, 3
    LIMIT 20
  `;
  const [oberkampfRows] = await client.query({ query: oberkampfCheck, location });
  console.log("Oberkampf-like streets in 75011:", (oberkampfRows as Record<string, unknown>[]) || []);

  console.log("\nDone.");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
